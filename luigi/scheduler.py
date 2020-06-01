# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
The system for scheduling tasks and executing them in order.
Deals with dependencies, priorities, resources, etc.
The :py:class:`~luigi.worker.Worker` pulls tasks from the scheduler (usually over the REST interface) and executes them.
See :doc:`/central_scheduler` for more info.
"""

import collections
from collections.abc import MutableSet
import json

from luigi.batch_notifier import BatchNotifier

import pickle
import functools
import hashlib
import inspect
import itertools
import logging
import os
import re
import time
import uuid

from luigi import configuration
from luigi import notifications
from luigi import parameter
from luigi import task_history as history
from luigi.task_status import DISABLED, DONE, FAILED, PENDING, RUNNING, SUSPENDED, UNKNOWN, \
    BATCH_RUNNING
from luigi.task import Config
from luigi.parameter import ParameterVisibility

from luigi.metrics import MetricsCollectors

logger = logging.getLogger(__name__)

UPSTREAM_RUNNING = 'UPSTREAM_RUNNING'
UPSTREAM_MISSING_INPUT = 'UPSTREAM_MISSING_INPUT'
UPSTREAM_FAILED = 'UPSTREAM_FAILED'
UPSTREAM_DISABLED = 'UPSTREAM_DISABLED'

UPSTREAM_SEVERITY_ORDER = (
    '',
    UPSTREAM_RUNNING,
    UPSTREAM_MISSING_INPUT,
    UPSTREAM_FAILED,
    UPSTREAM_DISABLED,
)
UPSTREAM_SEVERITY_KEY = UPSTREAM_SEVERITY_ORDER.index
STATUS_TO_UPSTREAM_MAP = {
    FAILED: UPSTREAM_FAILED,
    RUNNING: UPSTREAM_RUNNING,
    BATCH_RUNNING: UPSTREAM_RUNNING,
    PENDING: UPSTREAM_MISSING_INPUT,
    DISABLED: UPSTREAM_DISABLED,
}

WORKER_STATE_DISABLED = 'disabled'
WORKER_STATE_ACTIVE = 'active'

TASK_FAMILY_RE = re.compile(r'([^(_]+)[(_]')

RPC_METHODS = {}

_retry_policy_fields = [
    "retry_count",
    "disable_hard_timeout",
    "disable_window",
]
RetryPolicy = collections.namedtuple("RetryPolicy", _retry_policy_fields)


def _get_empty_retry_policy():
    return RetryPolicy(*[None] * len(_retry_policy_fields))


def rpc_method(**request_args):
    def _rpc_method(fn):
        # If request args are passed, return this function again for use as
        # the decorator function with the request args attached.
        args, varargs, varkw, defaults, kwonlyargs, kwonlydefaults, ann = inspect.getfullargspec(fn)
        assert not varargs
        first_arg, *all_args = args
        assert first_arg == 'self'
        defaults = dict(zip(reversed(all_args), reversed(defaults or ())))
        required_args = frozenset(arg for arg in all_args if arg not in defaults)
        fn_name = fn.__name__

        @functools.wraps(fn)
        def rpc_func(self, *args, **kwargs):
            actual_args = defaults.copy()
            actual_args.update(dict(zip(all_args, args)))
            actual_args.update(kwargs)
            if not all(arg in actual_args for arg in required_args):
                raise TypeError('{} takes {} arguments ({} given)'.format(
                    fn_name, len(all_args), len(actual_args)))
            return self._request('/api/{}'.format(fn_name), actual_args, **request_args)

        RPC_METHODS[fn_name] = rpc_func
        return fn

    return _rpc_method


class scheduler(Config):
    retry_delay = parameter.FloatParameter(default=900.0)
    remove_delay = parameter.FloatParameter(default=600.0)
    worker_disconnect_delay = parameter.FloatParameter(default=60.0)
    state_path = parameter.Parameter(default='/var/lib/luigi-server/state.pickle')

    batch_emails = parameter.BoolParameter(default=False, description="Send e-mails in batches rather than immediately")

    # Jobs are disabled if we see more than retry_count failures in disable_window seconds.
    # These disables last for disable_persist seconds.
    disable_window = parameter.IntParameter(default=3600)
    retry_count = parameter.IntParameter(default=999999999)
    disable_hard_timeout = parameter.IntParameter(default=999999999)
    disable_persist = parameter.IntParameter(default=86400)
    max_shown_tasks = parameter.IntParameter(default=100000)
    max_graph_nodes = parameter.IntParameter(default=100000)

    record_task_history = parameter.BoolParameter(default=False)

    prune_on_get_work = parameter.BoolParameter(default=False)

    pause_enabled = parameter.BoolParameter(default=True)

    send_messages = parameter.BoolParameter(default=True)

    metrics_collector = parameter.EnumParameter(enum=MetricsCollectors, default=MetricsCollectors.default)

    stable_done_cooldown_secs = parameter.IntParameter(default=10,
                                                       description="Sets cooldown period to avoid running the same task twice")
    """
    Sets a cooldown period in seconds after a task was completed, during this period the same task will not accepted by the scheduler.
    """

    def _get_retry_policy(self):
        return RetryPolicy(self.retry_count, self.disable_hard_timeout, self.disable_window)


class Failures:
    """
    This class tracks the number of failures in a given time window.

    Failures added are marked with the current timestamp, and this class counts
    the number of failures in a sliding time window ending at the present.
    """

    def __init__(self, window):
        """
        Initialize with the given window.

        :param window: how long to track failures for, as a float (number of seconds).
        """
        self.window = window
        self.failures = collections.deque()
        self.first_failure_time = None

    def add_failure(self):
        """
        Add a failure event with the current timestamp.
        """
        failure_time = time.time()

        if not self.first_failure_time:
            self.first_failure_time = failure_time

        self.failures.append(failure_time)

    def num_failures(self):
        """
        Return the number of failures in the window.
        """
        min_time = time.time() - self.window

        while self.failures and self.failures[0] < min_time:
            self.failures.popleft()

        return len(self.failures)

    def clear(self):
        """
        Clear the failure queue.
        """
        self.failures.clear()


def _get_default(x, default):
    if x is not None:
        return x
    else:
        return default


class OrderedSet(MutableSet):
    """
    Standard Python OrderedSet recipe found at http://code.activestate.com/recipes/576694/

    Modified to include a peek function to get the last element
    """

    def __init__(self, iterable=None):
        self.end = end = []
        end += [None, end, end]         # sentinel node for doubly linked list
        self.map = {}                   # key --> [key, prev, next]
        if iterable is not None:
            self |= iterable

    def __len__(self):
        return len(self.map)

    def __contains__(self, key):
        return key in self.map

    def add(self, key):
        if key not in self.map:
            end = self.end
            curr = end[1]
            curr[2] = end[1] = self.map[key] = [key, curr, end]

    def discard(self, key):
        if key in self.map:
            key, prev, next = self.map.pop(key)
            prev[2] = next
            next[1] = prev

    def __iter__(self):
        end = self.end
        curr = end[2]
        while curr is not end:
            yield curr[0]
            curr = curr[2]

    def __reversed__(self):
        end = self.end
        curr = end[1]
        while curr is not end:
            yield curr[0]
            curr = curr[1]

    def peek(self, last=True):
        if not self:
            raise KeyError('set is empty')
        key = self.end[1][0] if last else self.end[2][0]
        return key

    def pop(self, last=True):
        key = self.peek(last)
        self.discard(key)
        return key

    def __repr__(self):
        if not self:
            return '%s()' % (self.__class__.__name__,)
        return '%s(%r)' % (self.__class__.__name__, list(self))

    def __eq__(self, other):
        if isinstance(other, OrderedSet):
            return len(self) == len(other) and list(self) == list(other)
        return set(self) == set(other)


class Task:
    def __init__(self, task_id, status, deps, resources=None, priority=0, family='', module=None,
                 params=None, param_visibilities=None, accepts_messages=False, tracking_url=None, status_message=None,
                 progress_percentage=None, retry_policy='notoptional'):
        self.id = task_id
        self.stakeholders = set()  # workers ids that are somehow related to this task (i.e. don't prune while any of these workers are still active)
        self.workers = OrderedSet()  # workers ids that can perform task - task is 'BROKEN' if none of these workers are active
        if deps is None:
            self.deps = set()
        else:
            self.deps = set(deps)
        self.status = status  # PENDING, RUNNING, FAILED or DONE
        self.time = time.time()  # Timestamp when task was first added
        self.updated = self.time
        self.retry = None
        self.remove = None
        self.worker_running = None  # the worker id that is currently running the task or None
        self.time_running = None  # Timestamp when picked up by worker
        self.expl = None
        self.priority = priority
        self.resources = _get_default(resources, {})
        self.family = family
        self.module = module
        self.param_visibilities = _get_default(param_visibilities, {})
        self.params = {}
        self.public_params = {}
        self.hidden_params = {}
        self.set_params(params)
        self.accepts_messages = accepts_messages
        self.retry_policy = retry_policy
        self.failures = Failures(self.retry_policy.disable_window)
        self.tracking_url = tracking_url
        self.status_message = status_message
        self.progress_percentage = progress_percentage
        self.scheduler_message_responses = {}
        self.scheduler_disable_time = None
        self.runnable = False
        self.batchable = False
        self.batch_id = None

    def __repr__(self):
        return "Task(%r)" % vars(self)

    def set_params(self, params):
        self.params = _get_default(params, {})
        self.public_params = {key: value for key, value in self.params.items() if
                              self.param_visibilities.get(key, ParameterVisibility.PUBLIC) == ParameterVisibility.PUBLIC}
        self.hidden_params = {key: value for key, value in self.params.items() if
                              self.param_visibilities.get(key, ParameterVisibility.PUBLIC) == ParameterVisibility.HIDDEN}

    # TODO(2017-08-10) replace this function with direct calls to batchable
    # this only exists for backward compatibility
    def is_batchable(self):
        try:
            return self.batchable
        except AttributeError:
            return False

    def add_failure(self):
        self.failures.add_failure()

    def has_excessive_failures(self):
        if self.failures.first_failure_time is not None:
            if (time.time() >= self.failures.first_failure_time + self.retry_policy.disable_hard_timeout):
                return True

        logger.debug('%s task num failures is %s and limit is %s', self.id, self.failures.num_failures(), self.retry_policy.retry_count)
        if self.failures.num_failures() >= self.retry_policy.retry_count:
            logger.debug('%s task num failures limit(%s) is exceeded', self.id, self.retry_policy.retry_count)
            return True

        return False

    @property
    def pretty_id(self):
        param_str = ', '.join(u'{}={}'.format(key, value) for key, value in sorted(self.public_params.items()))
        return u'{}({})'.format(self.family, param_str)


class Worker:
    """
    Structure for tracking worker activity and keeping their references.
    """

    def __init__(self, worker_id, last_active=None):
        self.id = worker_id
        self.reference = None  # reference to the worker in the real world. (Currently a dict containing just the host)
        self.last_active = last_active or time.time()  # seconds since epoch
        self.last_get_work = None
        self.started = time.time()  # seconds since epoch
        self.tasks = set()  # task objects
        self.info = {}
        self.disabled = False
        self.rpc_messages = []

    def add_info(self, info):
        self.info.update(info)

    def update(self, worker_reference, get_work=False):
        if worker_reference:
            self.reference = worker_reference
        self.last_active = time.time()
        if get_work:
            self.last_get_work = time.time()

    def prune(self, config):
        # Delete workers that haven't said anything for a while (probably killed)
        if self.last_active + config.worker_disconnect_delay < time.time():
            return True

    def get_tasks(self, state, *statuses):
        num_self_tasks = len(self.tasks)
        num_state_tasks = sum(len(state._status_tasks[status]) for status in statuses)
        if num_self_tasks < num_state_tasks:
            return filter(lambda task: task.status in statuses, self.tasks)
        else:
            return filter(lambda task: self.id in task.workers, state.get_active_tasks_by_status(*statuses))

    def is_trivial_worker(self, state):
        """
        If it's not an assistant having only tasks that are without
        requirements.

        We have to pass the state parameter for optimization reasons.
        """
        if self.assistant:
            return False
        return all(not task.resources for task in self.get_tasks(state, PENDING))

    @property
    def assistant(self):
        return self.info.get('assistant', False)

    @property
    def enabled(self):
        return not self.disabled

    @property
    def state(self):
        if self.enabled:
            return WORKER_STATE_ACTIVE
        else:
            return WORKER_STATE_DISABLED

    def add_rpc_message(self, name, **kwargs):
        # the message has the format {'name': <function_name>, 'kwargs': <function_kwargs>}
        self.rpc_messages.append({'name': name, 'kwargs': kwargs})

    def fetch_rpc_messages(self):
        messages = self.rpc_messages[:]
        del self.rpc_messages[:]
        return messages

    def __str__(self):
        return self.id


class SimpleTaskState:
    """
    Keep track of the current state and handle persistence.

    The point of this class is to enable other ways to keep state, eg. by using a database
    These will be implemented by creating an abstract base class that this and other classes
    inherit from.
    """

    def __init__(self, state_path):
        self._state_path = state_path
        self._tasks = {}  # map from id to a Task object
        self._status_tasks = collections.defaultdict(dict)
        self._active_workers = {}  # map from id to a Worker object
        self._task_batchers = {}
        self._metrics_collector = None

    def get_state(self):
        return self._tasks, self._active_workers, self._task_batchers

    def set_state(self, state):
        self._tasks, self._active_workers = state[:2]
        if len(state) >= 3:
            self._task_batchers = state[2]

    def dump(self):
        try:
            with open(self._state_path, 'wb') as fobj:
                pickle.dump(self.get_state(), fobj)
        except IOError:
            logger.warning("Failed saving scheduler state", exc_info=1)
        else:
            logger.info("Saved state in %s", self._state_path)

    # prone to lead to crashes when old state is unpickled with updated code. TODO some kind of version control?
    def load(self):
        if os.path.exists(self._state_path):
            logger.info("Attempting to load state from %s", self._state_path)
            try:
                with open(self._state_path, 'rb') as fobj:
                    state = pickle.load(fobj)
            except BaseException:
                logger.exception("Error when loading state. Starting from empty state.")
                return

            self.set_state(state)
            self._status_tasks = collections.defaultdict(dict)
            for task in self._tasks.values():
                self._status_tasks[task.status][task.id] = task
        else:
            logger.info("No prior state file exists at %s. Starting with empty state", self._state_path)

    def get_active_tasks(self):
        return self._tasks.values()

    def get_active_tasks_by_status(self, *statuses):
        return itertools.chain.from_iterable(self._status_tasks[status].values() for status in statuses)

    def get_active_task_count_for_status(self, status):
        if status:
            return len(self._status_tasks[status])
        else:
            return len(self._tasks)

    def get_batch_running_tasks(self, batch_id):
        assert batch_id is not None
        return [
            task for task in self.get_active_tasks_by_status(BATCH_RUNNING)
            if task.batch_id == batch_id
        ]

    def set_batcher(self, worker_id, family, batcher_args, max_batch_size):
        self._task_batchers.setdefault(worker_id, {})
        self._task_batchers[worker_id][family] = (batcher_args, max_batch_size)

    def get_batcher(self, worker_id, family):
        return self._task_batchers.get(worker_id, {}).get(family, (None, 1))

    def num_pending_tasks(self):
        """
        Return how many tasks are PENDING + RUNNING. O(1).
        """
        return len(self._status_tasks[PENDING]) + len(self._status_tasks[RUNNING])

    def get_task(self, task_id, default=None, setdefault=None):
        if setdefault:
            task = self._tasks.setdefault(task_id, setdefault)
            self._status_tasks[task.status][task.id] = task
            return task
        else:
            return self._tasks.get(task_id, default)

    def has_task(self, task_id):
        return task_id in self._tasks

    def re_enable(self, task, config=None):
        task.scheduler_disable_time = None
        task.failures.clear()
        if config:
            self.set_status(task, FAILED, config)
            task.failures.clear()

    def set_batch_running(self, task, batch_id, worker_id):
        self.set_status(task, BATCH_RUNNING)
        task.batch_id = batch_id
        task.worker_running = worker_id
        task.resources_running = task.resources
        task.time_running = time.time()

    def set_status(self, task, new_status, config=None):
        if new_status == FAILED:
            assert config is not None

        if new_status == DISABLED and task.status in (RUNNING, BATCH_RUNNING):
            return

        remove_on_failure = task.batch_id is not None and not task.batchable

        if task.status == DISABLED:
            if new_status == DONE:
                self.re_enable(task)

            # don't allow workers to override a scheduler disable
            elif task.scheduler_disable_time is not None and new_status != DISABLED:
                return

        if task.status == RUNNING and task.batch_id is not None and new_status != RUNNING:
            for batch_task in self.get_batch_running_tasks(task.batch_id):
                self.set_status(batch_task, new_status, config)
                batch_task.batch_id = None
            task.batch_id = None

        if new_status == FAILED and task.status != DISABLED:
            task.add_failure()
            if task.has_excessive_failures():
                task.scheduler_disable_time = time.time()
                new_status = DISABLED
                if not config.batch_emails:
                    notifications.send_error_email(
                        'Luigi Scheduler: DISABLED {task} due to excessive failures'.format(task=task.id),
                        '{task} failed {failures} times in the last {window} seconds, so it is being '
                        'disabled for {persist} seconds'.format(
                            failures=task.retry_policy.retry_count,
                            task=task.id,
                            window=config.disable_window,
                            persist=config.disable_persist,
                        ))
        elif new_status == DISABLED:
            task.scheduler_disable_time = None

        if new_status != task.status:
            self._status_tasks[task.status].pop(task.id)
            self._status_tasks[new_status][task.id] = task
            task.status = new_status
            task.updated = time.time()
            self.update_metrics(task, config)

        if new_status == FAILED:
            task.retry = time.time() + config.retry_delay
            if remove_on_failure:
                task.remove = time.time()

    def fail_dead_worker_task(self, task, config, assistants):
        # If a running worker disconnects, tag all its jobs as FAILED and subject it to the same retry logic
        if task.status in (BATCH_RUNNING, RUNNING) and task.worker_running and task.worker_running not in task.stakeholders | assistants:
            logger.info("Task %r is marked as running by disconnected worker %r -> marking as "
                        "FAILED with retry delay of %rs", task.id, task.worker_running,
                        config.retry_delay)
            task.worker_running = None
            self.set_status(task, FAILED, config)
            task.retry = time.time() + config.retry_delay

    def update_status(self, task, config):
        # Mark tasks with no remaining active stakeholders for deletion
        if (not task.stakeholders) and (task.remove is None) and (task.status != RUNNING):
            # We don't check for the RUNNING case, because that is already handled
            # by the fail_dead_worker_task function.
            logger.debug("Task %r has no stakeholders anymore -> might remove "
                         "task in %s seconds", task.id, config.remove_delay)
            task.remove = time.time() + config.remove_delay

        # Re-enable task after the disable time expires
        if task.status == DISABLED and task.scheduler_disable_time is not None:
            if time.time() - task.scheduler_disable_time > config.disable_persist:
                self.re_enable(task, config)

        # Reset FAILED tasks to PENDING if max timeout is reached, and retry delay is >= 0
        if task.status == FAILED and config.retry_delay >= 0 and task.retry < time.time():
            self.set_status(task, PENDING, config)

    def may_prune(self, task):
        return task.remove and time.time() >= task.remove

    def inactivate_tasks(self, delete_tasks):
        # The terminology is a bit confusing: we used to "delete" tasks when they became inactive,
        # but with a pluggable state storage, you might very well want to keep some history of
        # older tasks as well. That's why we call it "inactivate" (as in the verb)
        for task in delete_tasks:
            task_obj = self._tasks.pop(task)
            self._status_tasks[task_obj.status].pop(task)

    def get_active_workers(self, last_active_lt=None, last_get_work_gt=None):
        for worker in self._active_workers.values():
            if last_active_lt is not None and worker.last_active >= last_active_lt:
                continue
            last_get_work = worker.last_get_work
            if last_get_work_gt is not None and (
                            last_get_work is None or last_get_work <= last_get_work_gt):
                continue
            yield worker

    def get_assistants(self, last_active_lt=None):
        return filter(lambda w: w.assistant, self.get_active_workers(last_active_lt))

    def get_worker_ids(self):
        return self._active_workers.keys()  # only used for unit tests

    def get_worker(self, worker_id):
        return self._active_workers.setdefault(worker_id, Worker(worker_id))

    def inactivate_workers(self, delete_workers):
        # Mark workers as inactive
        for worker in delete_workers:
            self._active_workers.pop(worker)
        self._remove_workers_from_tasks(delete_workers)

    def _remove_workers_from_tasks(self, workers, remove_stakeholders=True):
        for task in self.get_active_tasks():
            if remove_stakeholders:
                task.stakeholders.difference_update(workers)
            task.workers -= workers

    def disable_workers(self, worker_ids):
        self._remove_workers_from_tasks(worker_ids, remove_stakeholders=False)
        for worker_id in worker_ids:
            worker = self.get_worker(worker_id)
            worker.disabled = True
            worker.tasks.clear()

    def update_metrics(self, task, config):
        if task.status == DISABLED:
            self._metrics_collector.handle_task_disabled(task, config)
        elif task.status == DONE:
            self._metrics_collector.handle_task_done(task)
        elif task.status == FAILED:
            self._metrics_collector.handle_task_failed(task)


class Scheduler:
    """
    Async scheduler that can handle multiple workers, etc.

    Can be run locally or on a server (using RemoteScheduler + server.Server).
    """

    def __init__(self, config=None, resources=None, task_history_impl=None, **kwargs):
        """
        Keyword Arguments:
        :param config: an object of class "scheduler" or None (in which the global instance will be used)
        :param resources: a dict of str->int constraints
        :param task_history_impl: ignore config and use this object as the task history
        """
        self._config = config or scheduler(**kwargs)
        self._state = SimpleTaskState(self._config.state_path)

        if task_history_impl:
            self._task_history = task_history_impl
        elif self._config.record_task_history:
            from luigi import db_task_history  # Needs sqlalchemy, thus imported here
            self._task_history = db_task_history.DbTaskHistory()
        else:
            self._task_history = history.NopHistory()
        self._resources = resources or configuration.get_config().getintdict('resources')  # TODO: Can we make this a Parameter?
        self._make_task = functools.partial(Task, retry_policy=self._config._get_retry_policy())
        self._worker_requests = {}
        self._paused = False

        if self._config.batch_emails:
            self._email_batcher = BatchNotifier()

        self._state._metrics_collector = MetricsCollectors.get(self._config.metrics_collector)

    def load(self):
        self._state.load()

    def dump(self):
        self._state.dump()
        if self._config.batch_emails:
            self._email_batcher.send_email()

    @rpc_method()
    def prune(self):
        logger.debug("Starting pruning of task graph")
        self._prune_workers()
        self._prune_tasks()
        self._prune_emails()
        logger.debug("Done pruning task graph")

    def _prune_workers(self):
        remove_workers = []
        for worker in self._state.get_active_workers():
            if worker.prune(self._config):
                logger.debug("Worker %s timed out (no contact for >=%ss)", worker, self._config.worker_disconnect_delay)
                remove_workers.append(worker.id)

        self._state.inactivate_workers(remove_workers)

    def _prune_tasks(self):
        assistant_ids = {w.id for w in self._state.get_assistants()}
        remove_tasks = []

        for task in self._state.get_active_tasks():
            self._state.fail_dead_worker_task(task, self._config, assistant_ids)
            self._state.update_status(task, self._config)
            if self._state.may_prune(task):
                logger.info("Removing task %r", task.id)
                remove_tasks.append(task.id)

        self._state.inactivate_tasks(remove_tasks)

    def _prune_emails(self):
        if self._config.batch_emails:
            self._email_batcher.update()

    def _update_worker(self, worker_id, worker_reference=None, get_work=False):
        # Keep track of whenever the worker was last active.
        # For convenience also return the worker object.
        worker = self._state.get_worker(worker_id)
        worker.update(worker_reference, get_work=get_work)
        return worker

    def _update_priority(self, task, prio, worker):
        """
        Update priority of the given task.

        Priority can only be increased.
        If the task doesn't exist, a placeholder task is created to preserve priority when the task is later scheduled.
        """
        task.priority = prio = max(prio, task.priority)
        for dep in task.deps or []:
            t = self._state.get_task(dep)
            if t is not None and prio > t.priority:
                self._update_priority(t, prio, worker)

    @rpc_method()
    def add_task_batcher(self, worker, task_family, batched_args, max_batch_size=float('inf')):
        self._state.set_batcher(worker, task_family, batched_args, max_batch_size)

    @rpc_method()
    def forgive_failures(self, task_id=None):
        status = PENDING
        task = self._state.get_task(task_id)
        if task is None:
            return {"task_id": task_id, "status": None}

        # we forgive only failures
        if task.status == FAILED:
            # forgive but do not forget
            self._update_task_history(task, status)
            self._state.set_status(task, status, self._config)
        return {"task_id": task_id, "status": task.status}

    @rpc_method()
    def mark_as_done(self, task_id=None):
        status = DONE
        task = self._state.get_task(task_id)
        if task is None:
            return {"task_id": task_id, "status": None}

        # we can force mark DONE for running or failed tasks
        if task.status in {RUNNING, FAILED, DISABLED}:
            self._update_task_history(task, status)
            self._state.set_status(task, status, self._config)
        return {"task_id": task_id, "status": task.status}

    @rpc_method()
    def add_task(self, task_id=None, status=PENDING, runnable=True,
                 deps=None, new_deps=None, expl=None, resources=None,
                 priority=0, family='', module=None, params=None, param_visibilities=None, accepts_messages=False,
                 assistant=False, tracking_url=None, worker=None, batchable=None,
                 batch_id=None, retry_policy_dict=None, owners=None, **kwargs):
        """
        * add task identified by task_id if it doesn't exist
        * if deps is not None, update dependency list
        * update status of task
        * add additional workers/stakeholders
        * update priority when needed
        """
        assert worker is not None
        worker_id = worker
        worker = self._update_worker(worker_id)

        resources = {} if resources is None else resources.copy()

        if retry_policy_dict is None:
            retry_policy_dict = {}

        retry_policy = self._generate_retry_policy(retry_policy_dict)

        if worker.enabled:
            _default_task = self._make_task(
                task_id=task_id, status=PENDING, deps=deps, resources=resources,
                priority=priority, family=family, module=module, params=params, param_visibilities=param_visibilities,
            )
        else:
            _default_task = None

        task = self._state.get_task(task_id, setdefault=_default_task)

        if task is None or (task.status != RUNNING and not worker.enabled):
            return

        # Ignore claims that the task is PENDING if it very recently was marked as DONE.
        if status == PENDING and task.status == DONE and (time.time() - task.updated) < self._config.stable_done_cooldown_secs:
            return

        # for setting priority, we'll sometimes create tasks with unset family and params
        if not task.family:
            task.family = family
        if not getattr(task, 'module', None):
            task.module = module
        if not getattr(task, 'param_visibilities', None):
            task.param_visibilities = _get_default(param_visibilities, {})
        if not task.params:
            task.set_params(params)

        if batch_id is not None:
            task.batch_id = batch_id
        if status == RUNNING and not task.worker_running:
            task.worker_running = worker_id
            if batch_id:
                # copy resources_running of the first batch task
                batch_tasks = self._state.get_batch_running_tasks(batch_id)
                task.resources_running = batch_tasks[0].resources_running.copy()
            task.time_running = time.time()

        if accepts_messages is not None:
            task.accepts_messages = accepts_messages

        if tracking_url is not None or task.status != RUNNING:
            task.tracking_url = tracking_url
            if task.batch_id is not None:
                for batch_task in self._state.get_batch_running_tasks(task.batch_id):
                    batch_task.tracking_url = tracking_url

        if batchable is not None:
            task.batchable = batchable

        if task.remove is not None:
            task.remove = None  # unmark task for removal so it isn't removed after being added

        if expl is not None:
            task.expl = expl
            if task.batch_id is not None:
                for batch_task in self._state.get_batch_running_tasks(task.batch_id):
                    batch_task.expl = expl

        task_is_not_running = task.status not in (RUNNING, BATCH_RUNNING)
        task_started_a_run = status in (DONE, FAILED, RUNNING)
        running_on_this_worker = task.worker_running == worker_id
        if task_is_not_running or (task_started_a_run and running_on_this_worker) or new_deps:
            # don't allow re-scheduling of task while it is running, it must either fail or succeed on the worker actually running it
            if status != task.status or status == PENDING:
                # Update the DB only if there was a acctual change, to prevent noise.
                # We also check for status == PENDING b/c that's the default value
                # (so checking for status != task.status woule lie)
                self._update_task_history(task, status)
            self._state.set_status(task, PENDING if status == SUSPENDED else status, self._config)

        if status == FAILED and self._config.batch_emails:
            batched_params, _ = self._state.get_batcher(worker_id, family)
            if batched_params:
                unbatched_params = {
                    param: value
                    for param, value in task.params.items()
                    if param not in batched_params
                }
            else:
                unbatched_params = task.params
            try:
                expl_raw = json.loads(expl)
            except ValueError:
                expl_raw = expl

            self._email_batcher.add_failure(
                task.pretty_id, task.family, unbatched_params, expl_raw, owners)
            if task.status == DISABLED:
                self._email_batcher.add_disable(
                    task.pretty_id, task.family, unbatched_params, owners)

        if deps is not None:
            task.deps = set(deps)

        if new_deps is not None:
            task.deps.update(new_deps)

        if resources is not None:
            task.resources = resources

        if worker.enabled and not assistant:
            task.stakeholders.add(worker_id)

            # Task dependencies might not exist yet. Let's create dummy tasks for them for now.
            # Otherwise the task dependencies might end up being pruned if scheduling takes a long time
            for dep in task.deps or []:
                t = self._state.get_task(dep, setdefault=self._make_task(task_id=dep, status=UNKNOWN, deps=None, priority=priority))
                t.stakeholders.add(worker_id)

        self._update_priority(task, priority, worker_id)

        # Because some tasks (non-dynamic dependencies) are `_make_task`ed
        # before we know their retry_policy, we always set it here
        task.retry_policy = retry_policy

        if runnable and status != FAILED and worker.enabled:
            task.workers.add(worker_id)
            self._state.get_worker(worker_id).tasks.add(task)
            task.runnable = runnable

    @rpc_method()
    def announce_scheduling_failure(self, task_name, family, params, expl, owners, **kwargs):
        if not self._config.batch_emails:
            return
        worker_id = kwargs['worker']
        batched_params, _ = self._state.get_batcher(worker_id, family)
        if batched_params:
            unbatched_params = {
                param: value
                for param, value in params.items()
                if param not in batched_params
            }
        else:
            unbatched_params = params
        self._email_batcher.add_scheduling_fail(task_name, family, unbatched_params, expl, owners)

    @rpc_method()
    def add_worker(self, worker, info, **kwargs):
        self._state.get_worker(worker).add_info(info)

    @rpc_method()
    def disable_worker(self, worker):
        self._state.disable_workers({worker})

    @rpc_method()
    def set_worker_processes(self, worker, n):
        self._state.get_worker(worker).add_rpc_message('set_worker_processes', n=n)

    @rpc_method()
    def send_scheduler_message(self, worker, task, content):
        if not self._config.send_messages:
            return {"message_id": None}

        message_id = str(uuid.uuid4())
        self._state.get_worker(worker).add_rpc_message('dispatch_scheduler_message', task_id=task,
                                                       message_id=message_id, content=content)

        return {"message_id": message_id}

    @rpc_method()
    def add_scheduler_message_response(self, task_id, message_id, response):
        if self._state.has_task(task_id):
            task = self._state.get_task(task_id)
            task.scheduler_message_responses[message_id] = response

    @rpc_method()
    def get_scheduler_message_response(self, task_id, message_id):
        response = None
        if self._state.has_task(task_id):
            task = self._state.get_task(task_id)
            response = task.scheduler_message_responses.pop(message_id, None)
        return {"response": response}

    @rpc_method()
    def has_task_history(self):
        return self._config.record_task_history

    @rpc_method()
    def is_pause_enabled(self):
        return {'enabled': self._config.pause_enabled}

    @rpc_method()
    def is_paused(self):
        return {'paused': self._paused}

    @rpc_method()
    def pause(self):
        if self._config.pause_enabled:
            self._paused = True

    @rpc_method()
    def unpause(self):
        if self._config.pause_enabled:
            self._paused = False

    @rpc_method()
    def update_resources(self, **resources):
        if self._resources is None:
            self._resources = {}
        self._resources.update(resources)

    @rpc_method()
    def update_resource(self, resource, amount):
        if not isinstance(amount, int) or amount < 0:
            return False
        self._resources[resource] = amount
        return True

    def _generate_retry_policy(self, task_retry_policy_dict):
        retry_policy_dict = self._config._get_retry_policy()._asdict()
        retry_policy_dict.update({k: v for k, v in task_retry_policy_dict.items() if v is not None})
        return RetryPolicy(**retry_policy_dict)

    def _has_resources(self, needed_resources, used_resources):
        if needed_resources is None:
            return True

        available_resources = self._resources or {}
        for resource, amount in needed_resources.items():
            if amount + used_resources[resource] > available_resources.get(resource, 1):
                return False
        return True

    def _used_resources(self):
        used_resources = collections.defaultdict(int)
        if self._resources is not None:
            for task in self._state.get_active_tasks_by_status(RUNNING):
                resources_running = getattr(task, "resources_running", task.resources)
                if resources_running:
                    for resource, amount in resources_running.items():
                        used_resources[resource] += amount
        return used_resources

    def _rank(self, task):
        """
        Return worker's rank function for task scheduling.

        :return:
        """

        return task.priority, -task.time

    def _schedulable(self, task):
        if task.status != PENDING:
            return False
        for dep in task.deps:
            dep_task = self._state.get_task(dep, default=None)
            if dep_task is None or dep_task.status != DONE:
                return False
        return True

    def _reset_orphaned_batch_running_tasks(self, worker_id):
        running_batch_ids = {
            task.batch_id
            for task in self._state.get_active_tasks_by_status(RUNNING)
            if task.worker_running == worker_id
        }
        orphaned_tasks = [
            task for task in self._state.get_active_tasks_by_status(BATCH_RUNNING)
            if task.worker_running == worker_id and task.batch_id not in running_batch_ids
        ]
        for task in orphaned_tasks:
            self._state.set_status(task, PENDING)

    @rpc_method()
    def count_pending(self, worker):
        worker_id, worker = worker, self._state.get_worker(worker)

        num_pending, num_unique_pending, num_pending_last_scheduled = 0, 0, 0
        running_tasks = []

        upstream_status_table = {}
        for task in worker.get_tasks(self._state, RUNNING):
            if self._upstream_status(task.id, upstream_status_table) == UPSTREAM_DISABLED:
                continue
            # Return a list of currently running tasks to the client,
            # makes it easier to troubleshoot
            other_worker = self._state.get_worker(task.worker_running)
            if other_worker is not None:
                more_info = {'task_id': task.id, 'worker': str(other_worker)}
                more_info.update(other_worker.info)
                running_tasks.append(more_info)

        for task in worker.get_tasks(self._state, PENDING, FAILED):
            if self._upstream_status(task.id, upstream_status_table) == UPSTREAM_DISABLED:
                continue
            num_pending += 1
            num_unique_pending += int(len(task.workers) == 1)
            num_pending_last_scheduled += int(task.workers.peek(last=True) == worker_id)

        return {
            'n_pending_tasks': num_pending,
            'n_unique_pending': num_unique_pending,
            'n_pending_last_scheduled': num_pending_last_scheduled,
            'worker_state': worker.state,
            'running_tasks': running_tasks,
        }

    @rpc_method(allow_null=False)
    def get_work(self, host=None, assistant=False, current_tasks=None, worker=None, **kwargs):
        # TODO: remove any expired nodes

        # Algo: iterate over all nodes, find the highest priority node no dependencies and available
        # resources.

        # Resource checking looks both at currently available resources and at which resources would
        # be available if all running tasks died and we rescheduled all workers greedily. We do both
        # checks in order to prevent a worker with many low-priority tasks from starving other
        # workers with higher priority tasks that share the same resources.

        # TODO: remove tasks that can't be done, figure out if the worker has absolutely
        # nothing it can wait for

        if self._config.prune_on_get_work:
            self.prune()

        assert worker is not None
        worker_id = worker
        worker = self._update_worker(
            worker_id,
            worker_reference={'host': host},
            get_work=True)
        if not worker.enabled:
            reply = {'n_pending_tasks': 0,
                     'running_tasks': [],
                     'task_id': None,
                     'n_unique_pending': 0,
                     'worker_state': worker.state,
                     }
            return reply

        if assistant:
            self.add_worker(worker_id, [('assistant', assistant)])

        batched_params, unbatched_params, batched_tasks, max_batch_size = None, None, [], 1
        best_task = None
        if current_tasks is not None:
            ct_set = set(current_tasks)
            for task in sorted(self._state.get_active_tasks_by_status(RUNNING), key=self._rank):
                if task.worker_running == worker_id and task.id not in ct_set:
                    best_task = task

        if current_tasks is not None:
            # batch running tasks that weren't claimed since the last get_work go back in the pool
            self._reset_orphaned_batch_running_tasks(worker_id)

        greedy_resources = collections.defaultdict(int)

        worker = self._state.get_worker(worker_id)
        if self._paused:
            relevant_tasks = []
        elif worker.is_trivial_worker(self._state):
            relevant_tasks = worker.get_tasks(self._state, PENDING, RUNNING)
            used_resources = collections.defaultdict(int)
            greedy_workers = dict()  # If there's no resources, then they can grab any task
        else:
            relevant_tasks = self._state.get_active_tasks_by_status(PENDING, RUNNING)
            used_resources = self._used_resources()
            activity_limit = time.time() - self._config.worker_disconnect_delay
            active_workers = self._state.get_active_workers(last_get_work_gt=activity_limit)
            greedy_workers = dict((worker.id, worker.info.get('workers', 1))
                                  for worker in active_workers)
        tasks = list(relevant_tasks)
        tasks.sort(key=self._rank, reverse=True)

        for task in tasks:
            if (best_task and batched_params and task.family == best_task.family and
                    len(batched_tasks) < max_batch_size and task.is_batchable() and all(
                    task.params.get(name) == value for name, value in unbatched_params.items()) and
                    task.resources == best_task.resources and self._schedulable(task)):
                for name, params in batched_params.items():
                    params.append(task.params.get(name))
                batched_tasks.append(task)
            if best_task:
                continue

            if task.status == RUNNING and (task.worker_running in greedy_workers):
                greedy_workers[task.worker_running] -= 1
                for resource, amount in (getattr(task, 'resources_running', task.resources) or {}).items():
                    greedy_resources[resource] += amount

            if self._schedulable(task) and self._has_resources(task.resources, greedy_resources):
                in_workers = (assistant and task.runnable) or worker_id in task.workers
                if in_workers and self._has_resources(task.resources, used_resources):
                    best_task = task
                    batch_param_names, max_batch_size = self._state.get_batcher(
                        worker_id, task.family)
                    if batch_param_names and task.is_batchable():
                        try:
                            batched_params = {
                                name: [task.params[name]] for name in batch_param_names
                            }
                            unbatched_params = {
                                name: value for name, value in task.params.items()
                                if name not in batched_params
                            }
                            batched_tasks.append(task)
                        except KeyError:
                            batched_params, unbatched_params = None, None
                else:
                    workers = itertools.chain(task.workers, [worker_id]) if assistant else task.workers
                    for task_worker in workers:
                        if greedy_workers.get(task_worker, 0) > 0:
                            # use up a worker
                            greedy_workers[task_worker] -= 1

                            # keep track of the resources used in greedy scheduling
                            for resource, amount in (task.resources or {}).items():
                                greedy_resources[resource] += amount

                            break

        reply = self.count_pending(worker_id)

        if len(batched_tasks) > 1:
            batch_string = '|'.join(task.id for task in batched_tasks)
            batch_id = hashlib.md5(batch_string.encode('utf-8')).hexdigest()
            for task in batched_tasks:
                self._state.set_batch_running(task, batch_id, worker_id)

            combined_params = best_task.params.copy()
            combined_params.update(batched_params)

            reply['task_id'] = None
            reply['task_family'] = best_task.family
            reply['task_module'] = getattr(best_task, 'module', None)
            reply['task_params'] = combined_params
            reply['batch_id'] = batch_id
            reply['batch_task_ids'] = [task.id for task in batched_tasks]

        elif best_task:
            self.update_metrics_task_started(best_task)
            self._state.set_status(best_task, RUNNING, self._config)
            best_task.worker_running = worker_id
            best_task.resources_running = best_task.resources.copy()
            best_task.time_running = time.time()
            self._update_task_history(best_task, RUNNING, host=host)

            reply['task_id'] = best_task.id
            reply['task_family'] = best_task.family
            reply['task_module'] = getattr(best_task, 'module', None)
            reply['task_params'] = best_task.params

        else:
            reply['task_id'] = None

        return reply

    @rpc_method(attempts=1)
    def ping(self, **kwargs):
        worker_id = kwargs['worker']
        worker = self._update_worker(worker_id)
        return {"rpc_messages": worker.fetch_rpc_messages()}

    def _upstream_status(self, task_id, upstream_status_table):
        if task_id in upstream_status_table:
            return upstream_status_table[task_id]
        elif self._state.has_task(task_id):
            task_stack = [task_id]

            while task_stack:
                dep_id = task_stack.pop()
                dep = self._state.get_task(dep_id)
                if dep:
                    if dep.status == DONE:
                        continue
                    if dep_id not in upstream_status_table:
                        if dep.status == PENDING and dep.deps:
                            task_stack += [dep_id] + list(dep.deps)
                            upstream_status_table[dep_id] = ''  # will be updated postorder
                        else:
                            dep_status = STATUS_TO_UPSTREAM_MAP.get(dep.status, '')
                            upstream_status_table[dep_id] = dep_status
                    elif upstream_status_table[dep_id] == '' and dep.deps:
                        # This is the postorder update step when we set the
                        # status based on the previously calculated child elements
                        status = max((upstream_status_table.get(a_task_id, '')
                                      for a_task_id in dep.deps),
                                     key=UPSTREAM_SEVERITY_KEY)
                        upstream_status_table[dep_id] = status
            return upstream_status_table[dep_id]

    def _serialize_task(self, task_id, include_deps=True, deps=None):
        task = self._state.get_task(task_id)

        ret = {
            'display_name': task.pretty_id,
            'status': task.status,
            'workers': list(task.workers),
            'worker_running': task.worker_running,
            'time_running': getattr(task, "time_running", None),
            'start_time': task.time,
            'last_updated': getattr(task, "updated", task.time),
            'params': task.public_params,
            'name': task.family,
            'priority': task.priority,
            'resources': task.resources,
            'resources_running': getattr(task, "resources_running", None),
            'tracking_url': getattr(task, "tracking_url", None),
            'status_message': getattr(task, "status_message", None),
            'progress_percentage': getattr(task, "progress_percentage", None),
        }
        if task.status == DISABLED:
            ret['re_enable_able'] = task.scheduler_disable_time is not None
        if include_deps:
            ret['deps'] = list(task.deps if deps is None else deps)
        if self._config.send_messages and task.status == RUNNING:
            ret['accepts_messages'] = task.accepts_messages
        return ret

    @rpc_method()
    def graph(self, **kwargs):
        self.prune()
        serialized = {}
        seen = set()
        for task in self._state.get_active_tasks():
            serialized.update(self._traverse_graph(task.id, seen))
        return serialized

    def _filter_done(self, task_ids):
        for task_id in task_ids:
            task = self._state.get_task(task_id)
            if task is None or task.status != DONE:
                yield task_id

    def _traverse_graph(self, root_task_id, seen=None, dep_func=None, include_done=True):
        """ Returns the dependency graph rooted at task_id

        This does a breadth-first traversal to find the nodes closest to the
        root before hitting the scheduler.max_graph_nodes limit.

        :param root_task_id: the id of the graph's root
        :return: A map of task id to serialized node
        """

        if seen is None:
            seen = set()
        elif root_task_id in seen:
            return {}

        if dep_func is None:
            def dep_func(t):
                return t.deps

        seen.add(root_task_id)
        serialized = {}
        queue = collections.deque([root_task_id])
        while queue:
            task_id = queue.popleft()

            task = self._state.get_task(task_id)
            if task is None or not task.family:
                logger.debug('Missing task for id [%s]', task_id)

                # NOTE : If a dependency is missing from self._state there is no way to deduce the
                #        task family and parameters.
                family_match = TASK_FAMILY_RE.match(task_id)
                family = family_match.group(1) if family_match else UNKNOWN
                params = {'task_id': task_id}
                serialized[task_id] = {
                    'deps': [],
                    'status': UNKNOWN,
                    'workers': [],
                    'start_time': UNKNOWN,
                    'params': params,
                    'name': family,
                    'display_name': task_id,
                    'priority': 0,
                }
            else:
                deps = dep_func(task)
                if not include_done:
                    deps = list(self._filter_done(deps))
                serialized[task_id] = self._serialize_task(task_id, deps=deps)
                for dep in sorted(deps):
                    if dep not in seen:
                        seen.add(dep)
                        queue.append(dep)

            if task_id != root_task_id:
                del serialized[task_id]['display_name']
            if len(serialized) >= self._config.max_graph_nodes:
                break

        return serialized

    @rpc_method()
    def dep_graph(self, task_id, include_done=True, **kwargs):
        self.prune()
        if not self._state.has_task(task_id):
            return {}
        return self._traverse_graph(task_id, include_done=include_done)

    @rpc_method()
    def inverse_dep_graph(self, task_id, include_done=True, **kwargs):
        self.prune()
        if not self._state.has_task(task_id):
            return {}
        inverse_graph = collections.defaultdict(set)
        for task in self._state.get_active_tasks():
            for dep in task.deps:
                inverse_graph[dep].add(task.id)
        return self._traverse_graph(
            task_id, dep_func=lambda t: inverse_graph[t.id], include_done=include_done)

    @rpc_method()
    def task_list(self, status='', upstream_status='', limit=True, search=None, max_shown_tasks=None,
                  **kwargs):
        """
        Query for a subset of tasks by status.
        """
        if not search:
            count_limit = max_shown_tasks or self._config.max_shown_tasks
            pre_count = self._state.get_active_task_count_for_status(status)
            if limit and pre_count > count_limit:
                return {'num_tasks': -1 if upstream_status else pre_count}
        self.prune()

        result = {}
        upstream_status_table = {}  # used to memoize upstream status
        if search is None:
            def filter_func(_):
                return True
        else:
            terms = search.split()

            def filter_func(t):
                return all(term in t.pretty_id for term in terms)

        tasks = self._state.get_active_tasks_by_status(status) if status else self._state.get_active_tasks()
        for task in filter(filter_func, tasks):
            if task.status != PENDING or not upstream_status or upstream_status == self._upstream_status(task.id, upstream_status_table):
                serialized = self._serialize_task(task.id, include_deps=False)
                result[task.id] = serialized
        if limit and len(result) > (max_shown_tasks or self._config.max_shown_tasks):
            return {'num_tasks': len(result)}
        return result

    def _first_task_display_name(self, worker):
        task_id = worker.info.get('first_task', '')
        if self._state.has_task(task_id):
            return self._state.get_task(task_id).pretty_id
        else:
            return task_id

    @rpc_method()
    def worker_list(self, include_running=True, **kwargs):
        self.prune()
        workers = [
            dict(
                name=worker.id,
                last_active=worker.last_active,
                started=worker.started,
                state=worker.state,
                first_task_display_name=self._first_task_display_name(worker),
                num_unread_rpc_messages=len(worker.rpc_messages),
                **worker.info
            ) for worker in self._state.get_active_workers()]
        workers.sort(key=lambda worker: worker['started'], reverse=True)
        if include_running:
            running = collections.defaultdict(dict)
            for task in self._state.get_active_tasks_by_status(RUNNING):
                if task.worker_running:
                    running[task.worker_running][task.id] = self._serialize_task(task.id, include_deps=False)

            num_pending = collections.defaultdict(int)
            num_uniques = collections.defaultdict(int)
            for task in self._state.get_active_tasks_by_status(PENDING):
                for worker in task.workers:
                    num_pending[worker] += 1
                if len(task.workers) == 1:
                    num_uniques[list(task.workers)[0]] += 1

            for worker in workers:
                tasks = running[worker['name']]
                worker['num_running'] = len(tasks)
                worker['num_pending'] = num_pending[worker['name']]
                worker['num_uniques'] = num_uniques[worker['name']]
                worker['running'] = tasks
        return workers

    @rpc_method()
    def resource_list(self):
        """
        Resources usage info and their consumers (tasks).
        """
        self.prune()
        resources = [
            dict(
                name=resource,
                num_total=r_dict['total'],
                num_used=r_dict['used']
            ) for resource, r_dict in self.resources().items()]
        if self._resources is not None:
            consumers = collections.defaultdict(dict)
            for task in self._state.get_active_tasks_by_status(RUNNING):
                if task.status == RUNNING and task.resources:
                    for resource, amount in task.resources.items():
                        consumers[resource][task.id] = self._serialize_task(task.id, include_deps=False)
            for resource in resources:
                tasks = consumers[resource['name']]
                resource['num_consumer'] = len(tasks)
                resource['running'] = tasks
        return resources

    def resources(self):
        ''' get total resources and available ones '''
        used_resources = self._used_resources()
        ret = collections.defaultdict(dict)
        for resource, total in self._resources.items():
            ret[resource]['total'] = total
            if resource in used_resources:
                ret[resource]['used'] = used_resources[resource]
            else:
                ret[resource]['used'] = 0
        return ret

    @rpc_method()
    def task_search(self, task_str, **kwargs):
        """
        Query for a subset of tasks by task_id.

        :param task_str:
        :return:
        """
        self.prune()
        result = collections.defaultdict(dict)
        for task in self._state.get_active_tasks():
            if task.id.find(task_str) != -1:
                serialized = self._serialize_task(task.id, include_deps=False)
                result[task.status][task.id] = serialized
        return result

    @rpc_method()
    def re_enable_task(self, task_id):
        serialized = {}
        task = self._state.get_task(task_id)
        if task and task.status == DISABLED and task.scheduler_disable_time:
            self._state.re_enable(task, self._config)
            serialized = self._serialize_task(task_id)
        return serialized

    @rpc_method()
    def fetch_error(self, task_id, **kwargs):
        if self._state.has_task(task_id):
            task = self._state.get_task(task_id)
            return {"taskId": task_id, "error": task.expl, 'displayName': task.pretty_id}
        else:
            return {"taskId": task_id, "error": ""}

    @rpc_method()
    def set_task_status_message(self, task_id, status_message):
        if self._state.has_task(task_id):
            task = self._state.get_task(task_id)
            task.status_message = status_message
            if task.status == RUNNING and task.batch_id is not None:
                for batch_task in self._state.get_batch_running_tasks(task.batch_id):
                    batch_task.status_message = status_message

    @rpc_method()
    def get_task_status_message(self, task_id):
        if self._state.has_task(task_id):
            task = self._state.get_task(task_id)
            return {"taskId": task_id, "statusMessage": task.status_message}
        else:
            return {"taskId": task_id, "statusMessage": ""}

    @rpc_method()
    def set_task_progress_percentage(self, task_id, progress_percentage):
        if self._state.has_task(task_id):
            task = self._state.get_task(task_id)
            task.progress_percentage = progress_percentage
            if task.status == RUNNING and task.batch_id is not None:
                for batch_task in self._state.get_batch_running_tasks(task.batch_id):
                    batch_task.progress_percentage = progress_percentage

    @rpc_method()
    def get_task_progress_percentage(self, task_id):
        if self._state.has_task(task_id):
            task = self._state.get_task(task_id)
            return {"taskId": task_id, "progressPercentage": task.progress_percentage}
        else:
            return {"taskId": task_id, "progressPercentage": None}

    @rpc_method()
    def decrease_running_task_resources(self, task_id, decrease_resources):
        if self._state.has_task(task_id):
            task = self._state.get_task(task_id)
            if task.status != RUNNING:
                return

            def decrease(resources, decrease_resources):
                for resource, decrease_amount in decrease_resources.items():
                    if decrease_amount > 0 and resource in resources:
                        resources[resource] = max(0, resources[resource] - decrease_amount)

            decrease(task.resources_running, decrease_resources)
            if task.batch_id is not None:
                for batch_task in self._state.get_batch_running_tasks(task.batch_id):
                    decrease(batch_task.resources_running, decrease_resources)

    @rpc_method()
    def get_running_task_resources(self, task_id):
        if self._state.has_task(task_id):
            task = self._state.get_task(task_id)
            return {"taskId": task_id, "resources": getattr(task, "resources_running", None)}
        else:
            return {"taskId": task_id, "resources": None}

    def _update_task_history(self, task, status, host=None):
        try:
            if status == DONE or status == FAILED:
                successful = (status == DONE)
                self._task_history.task_finished(task, successful)
            elif status == PENDING:
                self._task_history.task_scheduled(task)
            elif status == RUNNING:
                self._task_history.task_started(task, host)
        except BaseException:
            logger.warning("Error saving Task history", exc_info=True)

    @property
    def task_history(self):
        # Used by server.py to expose the calls
        return self._task_history

    @rpc_method()
    def update_metrics_task_started(self, task):
        self._state._metrics_collector.handle_task_started(task)
