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
try:
    import cPickle as pickle
except ImportError:
    import pickle
import datetime
import functools
import itertools
import logging
import os
import time

from luigi import six

from luigi import configuration
from luigi import notifications
from luigi import parameter
from luigi import task_history as history
from luigi.task_status import DISABLED, DONE, FAILED, PENDING, RUNNING, SUSPENDED, UNKNOWN
from luigi.task import Config

logger = logging.getLogger("luigi.server")


class Scheduler(object):
    """
    Abstract base class.

    Note that the methods all take string arguments, not Task objects...
    """""
    add_task = NotImplemented
    get_work = NotImplemented
    ping = NotImplemented

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
    PENDING: UPSTREAM_MISSING_INPUT,
    DISABLED: UPSTREAM_DISABLED,
}


class scheduler(Config):
    # TODO(erikbern): the config_path is needed for backwards compatilibity. We should drop the compatibility
    # at some point (in particular this would force users to replace all dashes with underscores in the config)
    retry_delay = parameter.FloatParameter(default=900.0)
    remove_delay = parameter.FloatParameter(default=600.0)
    worker_disconnect_delay = parameter.FloatParameter(default=60.0)
    state_path = parameter.Parameter(default='/var/lib/luigi-server/state.pickle')

    # Jobs are disabled if we see more than disable_failures failures in disable_window seconds.
    # These disables last for disable_persist seconds.
    disable_window = parameter.IntParameter(default=3600,
                                            config_path=dict(section='scheduler', name='disable-window-seconds'))
    disable_failures = parameter.IntParameter(default=None,
                                              config_path=dict(section='scheduler', name='disable-num-failures'))
    disable_persist = parameter.IntParameter(default=86400,
                                             config_path=dict(section='scheduler', name='disable-persist-seconds'))
    max_shown_tasks = parameter.IntParameter(default=100000)
    prune_done_tasks = parameter.BoolParameter(default=False)

    record_task_history = parameter.BoolParameter(default=False)


def fix_time(x):
    # Backwards compatibility for a fix in Dec 2014. Prior to the fix, pickled state might store datetime objects
    # Let's remove this function soon
    if isinstance(x, datetime.datetime):
        return time.mktime(x.timetuple())
    else:
        return x


class Failures(object):
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

    def add_failure(self):
        """
        Add a failure event with the current timestamp.
        """
        self.failures.append(time.time())

    def num_failures(self):
        """
        Return the number of failures in the window.
        """
        min_time = time.time() - self.window

        while self.failures and fix_time(self.failures[0]) < min_time:
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


class Task(object):

    def __init__(self, task_id, status, deps, resources=None, priority=0, family='', module=None,
                 params=None, disable_failures=None, disable_window=None):
        self.id = task_id
        self.stakeholders = set()  # workers ids that are somehow related to this task (i.e. don't prune while any of these workers are still active)
        self.workers = set()  # workers ids that can perform task - task is 'BROKEN' if none of these workers are active
        if deps is None:
            self.deps = set()
        else:
            self.deps = set(deps)
        self.status = status  # PENDING, RUNNING, FAILED or DONE
        self.time = time.time()  # Timestamp when task was first added
        self.retry = None
        self.remove = None
        self.worker_running = None  # the worker id that is currently running the task or None
        self.time_running = None  # Timestamp when picked up by worker
        self.expl = None
        self.priority = priority
        self.resources = _get_default(resources, {})
        self.family = family
        self.module = module
        self.params = _get_default(params, {})
        self.disable_failures = disable_failures
        self.failures = Failures(disable_window)
        self.scheduler_disable_time = None

    def __repr__(self):
        return "Task(%r)" % vars(self)

    def add_failure(self):
        self.failures.add_failure()

    def has_excessive_failures(self):
        return self.failures.num_failures() >= self.disable_failures

    def can_disable(self):
        return self.disable_failures is not None


class Worker(object):
    """
    Structure for tracking worker activity and keeping their references.
    """

    def __init__(self, worker_id, last_active=None):
        self.id = worker_id
        self.reference = None  # reference to the worker in the real world. (Currently a dict containing just the host)
        self.last_active = last_active  # seconds since epoch
        self.started = time.time()  # seconds since epoch
        self.info = {}

    def add_info(self, info):
        self.info.update(info)

    def update(self, worker_reference):
        if worker_reference:
            self.reference = worker_reference
        self.last_active = time.time()

    def prune(self, config):
        # Delete workers that haven't said anything for a while (probably killed)
        if self.last_active + config.worker_disconnect_delay < time.time():
            return True

    @property
    def assistant(self):
        return self.info.get('assistant', False)

    def __str__(self):
        return self.id


class SimpleTaskState(object):
    """
    Keep track of the current state and handle persistance.

    The point of this class is to enable other ways to keep state, eg. by using a database
    These will be implemented by creating an abstract base class that this and other classes
    inherit from.
    """

    def __init__(self, state_path):
        self._state_path = state_path
        self._tasks = {}  # map from id to a Task object
        self._status_tasks = collections.defaultdict(dict)
        self._active_workers = {}  # map from id to a Worker object

    def dump(self):
        state = (self._tasks, self._active_workers)
        try:
            with open(self._state_path, 'wb') as fobj:
                pickle.dump(state, fobj)
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
                logger.exception("Error when loading state. Starting from clean slate.")
                return

            self._tasks, self._active_workers = state
            self._status_tasks = collections.defaultdict(dict)
            for task in six.itervalues(self._tasks):
                self._status_tasks[task.status][task.id] = task

            # Convert from old format
            # TODO: this is really ugly, we need something more future-proof
            # Every time we add an attribute to the Worker class, this code needs to be updated
            for k, v in six.iteritems(self._active_workers):
                if isinstance(v, float):
                    self._active_workers[k] = Worker(worker_id=k, last_active=v)
        else:
            logger.info("No prior state file exists at %s. Starting with clean slate", self._state_path)

    def get_active_tasks(self, status=None):
        if status:
            for task in six.itervalues(self._status_tasks[status]):
                yield task
        else:
            for task in six.itervalues(self._tasks):
                yield task

    def get_running_tasks(self):
        return six.itervalues(self._status_tasks[RUNNING])

    def get_pending_tasks(self):
        return itertools.chain.from_iterable(six.itervalues(self._status_tasks[status])
                                             for status in [PENDING, RUNNING])

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

    def set_status(self, task, new_status, config=None):
        if new_status == FAILED:
            assert config is not None

        # not sure why we have SUSPENDED, as it can never be set
        if new_status == SUSPENDED:
            new_status = PENDING

        if new_status == DISABLED and task.status == RUNNING:
            return

        if task.status == DISABLED:
            if new_status == DONE:
                self.re_enable(task)

            # don't allow workers to override a scheduler disable
            elif task.scheduler_disable_time is not None:
                return

        if new_status == FAILED and task.can_disable():
            task.add_failure()
            if task.has_excessive_failures():
                task.scheduler_disable_time = time.time()
                new_status = DISABLED
                notifications.send_error_email(
                    'Luigi Scheduler: DISABLED {task} due to excessive failures'.format(task=task.id),
                    '{task} failed {failures} times in the last {window} seconds, so it is being '
                    'disabled for {persist} seconds'.format(
                        failures=config.disable_failures,
                        task=task.id,
                        window=config.disable_window,
                        persist=config.disable_persist,
                    ))
        elif new_status == DISABLED:
            task.scheduler_disable_time = None

        self._status_tasks[task.status].pop(task.id)
        self._status_tasks[new_status][task.id] = task
        task.status = new_status

    def prune(self, task, config, assistants):
        remove = False

        # Mark tasks with no remaining active stakeholders for deletion
        if not task.stakeholders:
            if task.remove is None:
                logger.info("Task %r has stakeholders %r but none remain connected -> will remove "
                            "task in %s seconds", task.id, task.stakeholders, config.remove_delay)
                task.remove = time.time() + config.remove_delay

        # If a running worker disconnects, tag all its jobs as FAILED and subject it to the same retry logic
        if task.status == RUNNING and task.worker_running and task.worker_running not in task.stakeholders | assistants:
            logger.info("Task %r is marked as running by disconnected worker %r -> marking as "
                        "FAILED with retry delay of %rs", task.id, task.worker_running,
                        config.retry_delay)
            task.worker_running = None
            self.set_status(task, FAILED, config)
            task.retry = time.time() + config.retry_delay

        # Re-enable task after the disable time expires
        if task.status == DISABLED and task.scheduler_disable_time:
            if time.time() - fix_time(task.scheduler_disable_time) > config.disable_persist:
                self.re_enable(task, config)

        # Remove tasks that have no stakeholders
        if task.remove and time.time() > task.remove:
            logger.info("Removing task %r (no connected stakeholders)", task.id)
            remove = True

        # Reset FAILED tasks to PENDING if max timeout is reached, and retry delay is >= 0
        if task.status == FAILED and config.retry_delay >= 0 and task.retry < time.time():
            self.set_status(task, PENDING, config)

        return remove

    def inactivate_tasks(self, delete_tasks):
        # The terminology is a bit confusing: we used to "delete" tasks when they became inactive,
        # but with a pluggable state storage, you might very well want to keep some history of
        # older tasks as well. That's why we call it "inactivate" (as in the verb)
        for task in delete_tasks:
            task_obj = self._tasks.pop(task)
            self._status_tasks[task_obj.status].pop(task)

    def get_active_workers(self, last_active_lt=None):
        for worker in six.itervalues(self._active_workers):
            if last_active_lt is not None and worker.last_active >= last_active_lt:
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

        # remove workers from tasks
        for task in self.get_active_tasks():
            task.stakeholders.difference_update(delete_workers)
            task.workers.difference_update(delete_workers)

    def get_necessary_tasks(self):
        necessary_tasks = set()
        for task in self.get_active_tasks():
            if task.status not in (DONE, DISABLED) or \
                    getattr(task, 'scheduler_disable_time', None) is not None:
                necessary_tasks.update(task.deps)
                necessary_tasks.add(task.id)
        return necessary_tasks


class CentralPlannerScheduler(Scheduler):
    """
    Async scheduler that can handle multiple workers, etc.

    Can be run locally or on a server (using RemoteScheduler + server.Server).
    """

    def __init__(self, config=None, resources=None, task_history_impl=None, **kwargs):
        """
        Keyword Arguments:
        :param config: an object of class "scheduler" or None (in which the global instance will be used)
        :param resources: a dict of str->int constraints
        :param task_history_override: ignore config and use this object as the task history
        """
        self._config = config or scheduler(**kwargs)
        self._state = SimpleTaskState(self._config.state_path)

        if task_history_impl:
            self._task_history = task_history_impl
        elif self._config.record_task_history:
            import db_task_history  # Needs sqlalchemy, thus imported here
            self._task_history = db_task_history.DbTaskHistory()
        else:
            self._task_history = history.NopHistory()
        self._resources = resources or configuration.get_config().getintdict('resources')  # TODO: Can we make this a Parameter?
        self._make_task = functools.partial(
            Task, disable_failures=self._config.disable_failures,
            disable_window=self._config.disable_window)

    def load(self):
        self._state.load()

    def dump(self):
        self._state.dump()

    def prune(self):
        logger.info("Starting pruning of task graph")
        remove_workers = []
        for worker in self._state.get_active_workers():
            if worker.prune(self._config):
                logger.info("Worker %s timed out (no contact for >=%ss)", worker, self._config.worker_disconnect_delay)
                remove_workers.append(worker.id)

        self._state.inactivate_workers(remove_workers)

        assistant_ids = set(w.id for w in self._state.get_assistants())
        remove_tasks = []

        if assistant_ids:
            necessary_tasks = self._state.get_necessary_tasks()
        else:
            necessary_tasks = ()

        for task in self._state.get_active_tasks():
            if task.id not in necessary_tasks and self._state.prune(task, self._config, assistant_ids):
                remove_tasks.append(task.id)

        self._state.inactivate_tasks(remove_tasks)

        logger.info("Done pruning task graph")

    def update(self, worker_id, worker_reference=None):
        """
        Keep track of whenever the worker was last active.
        """
        worker = self._state.get_worker(worker_id)
        worker.update(worker_reference)

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

    def add_task(self, worker, task_id, status=PENDING, runnable=True,
                 deps=None, new_deps=None, expl=None, resources=None,
                 priority=0, family='', module=None, params=None,
                 assistant=False, **kwargs):
        """
        * add task identified by task_id if it doesn't exist
        * if deps is not None, update dependency list
        * update status of task
        * add additional workers/stakeholders
        * update priority when needed
        """
        self.update(worker)

        task = self._state.get_task(task_id, setdefault=self._make_task(
            task_id=task_id, status=PENDING, deps=deps, resources=resources,
            priority=priority, family=family, module=module, params=params))

        # for setting priority, we'll sometimes create tasks with unset family and params
        if not task.family:
            task.family = family
        if not getattr(task, 'module', None):
            task.module = module
        if not task.params:
            task.params = _get_default(params, {})

        if task.remove is not None:
            task.remove = None  # unmark task for removal so it isn't removed after being added

        if not (task.status == RUNNING and status == PENDING):
            # don't allow re-scheduling of task while it is running, it must either fail or succeed first
            if status == PENDING or status != task.status:
                # Update the DB only if there was a acctual change, to prevent noise.
                # We also check for status == PENDING b/c that's the default value
                # (so checking for status != task.status woule lie)
                self._update_task_history(task_id, status)
            self._state.set_status(task, PENDING if status == SUSPENDED else status, self._config)
            if status == FAILED:
                task.retry = time.time() + self._config.retry_delay

        if deps is not None:
            task.deps = set(deps)

        if new_deps is not None:
            task.deps.update(new_deps)

        if resources is not None:
            task.resources = resources

        if not assistant:
            task.stakeholders.add(worker)

            # Task dependencies might not exist yet. Let's create dummy tasks for them for now.
            # Otherwise the task dependencies might end up being pruned if scheduling takes a long time
            for dep in task.deps or []:
                t = self._state.get_task(dep, setdefault=self._make_task(task_id=dep, status=UNKNOWN, deps=None, priority=priority))
                t.stakeholders.add(worker)

        self._update_priority(task, priority, worker)

        if runnable:
            task.workers.add(worker)

        if expl is not None:
            task.expl = expl

    def add_worker(self, worker, info, **kwargs):
        self._state.get_worker(worker).add_info(info)

    def update_resources(self, **resources):
        if self._resources is None:
            self._resources = {}
        self._resources.update(resources)

    def _has_resources(self, needed_resources, used_resources):
        if needed_resources is None:
            return True

        available_resources = self._resources or {}
        for resource, amount in six.iteritems(needed_resources):
            if amount + used_resources[resource] > available_resources.get(resource, 1):
                return False
        return True

    def _used_resources(self):
        used_resources = collections.defaultdict(int)
        if self._resources is not None:
            for task in self._state.get_active_tasks():
                if task.status == RUNNING and task.resources:
                    for resource, amount in six.iteritems(task.resources):
                        used_resources[resource] += amount
        return used_resources

    def _rank(self):
        """
        Return worker's rank function for task scheduling.

        :return:
        """
        dependents = collections.defaultdict(int)

        def not_done(t):
            task = self._state.get_task(t, default=None)
            return task is None or task.status != DONE
        for task in self._state.get_pending_tasks():
            if task.status != DONE:
                deps = list(filter(not_done, task.deps))
                inverse_num_deps = 1.0 / max(len(deps), 1)
                for dep in deps:
                    dependents[dep] += inverse_num_deps

        return lambda task: (task.priority, dependents[task.id], -task.time)

    def _schedulable(self, task):
        if task.status != PENDING:
            return False
        for dep in task.deps:
            dep_task = self._state.get_task(dep, default=None)
            if dep_task is None or dep_task.status != DONE:
                return False
        return True

    def get_work(self, worker, host=None, assistant=False, **kwargs):
        # TODO: remove any expired nodes

        # Algo: iterate over all nodes, find the highest priority node no dependencies and available
        # resources.

        # Resource checking looks both at currently available resources and at which resources would
        # be available if all running tasks died and we rescheduled all workers greedily. We do both
        # checks in order to prevent a worker with many low-priority tasks from starving other
        # workers with higher priority tasks that share the same resources.

        # TODO: remove tasks that can't be done, figure out if the worker has absolutely
        # nothing it can wait for

        # Return remaining tasks that have no FAILED descendents
        self.update(worker, {'host': host})
        if assistant:
            self.add_worker(worker, [('assistant', assistant)])
        best_task = None
        locally_pending_tasks = 0
        running_tasks = []
        upstream_table = {}

        used_resources = self._used_resources()
        greedy_resources = collections.defaultdict(int)
        n_unique_pending = 0
        greedy_workers = dict((worker.id, worker.info.get('workers', 1))
                              for worker in self._state.get_active_workers())

        tasks = list(self._state.get_pending_tasks())
        tasks.sort(key=self._rank(), reverse=True)

        for task in tasks:
            upstream_status = self._upstream_status(task.id, upstream_table)
            in_workers = (assistant and task.workers) or worker in task.workers
            if task.status == 'RUNNING' and in_workers:
                # Return a list of currently running tasks to the client,
                # makes it easier to troubleshoot
                other_worker = self._state.get_worker(task.worker_running)
                more_info = {'task_id': task.id, 'worker': str(other_worker)}
                if other_worker is not None:
                    more_info.update(other_worker.info)
                    running_tasks.append(more_info)

            if task.status == PENDING and in_workers and upstream_status != UPSTREAM_DISABLED:
                locally_pending_tasks += 1
                if len(task.workers) == 1 and not assistant:
                    n_unique_pending += 1

            if task.status == RUNNING and (task.worker_running in greedy_workers):
                greedy_workers[task.worker_running] -= 1
                for resource, amount in six.iteritems((task.resources or {})):
                    greedy_resources[resource] += amount

            if not best_task and self._schedulable(task) and self._has_resources(task.resources, greedy_resources):
                if in_workers and self._has_resources(task.resources, used_resources):
                    best_task = task
                else:
                    workers = itertools.chain(task.workers, [worker]) if assistant else task.workers
                    for task_worker in workers:
                        if greedy_workers.get(task_worker, 0) > 0:
                            # use up a worker
                            greedy_workers[task_worker] -= 1

                            # keep track of the resources used in greedy scheduling
                            for resource, amount in six.iteritems((task.resources or {})):
                                greedy_resources[resource] += amount

                            break

        reply = {'n_pending_tasks': locally_pending_tasks,
                 'running_tasks': running_tasks,
                 'task_id': None,
                 'n_unique_pending': n_unique_pending}

        if best_task:
            self._state.set_status(best_task, RUNNING, self._config)
            best_task.worker_running = worker
            best_task.time_running = time.time()
            self._update_task_history(best_task.id, RUNNING, host=host)

            reply['task_id'] = best_task.id
            reply['task_family'] = best_task.family
            reply['task_module'] = getattr(best_task, 'module', None)
            reply['task_params'] = best_task.params

        return reply

    def ping(self, worker, **kwargs):
        self.update(worker)

    def _upstream_status(self, task_id, upstream_status_table):
        if task_id in upstream_status_table:
            return upstream_status_table[task_id]
        elif self._state.has_task(task_id):
            task_stack = [task_id]

            while task_stack:
                dep_id = task_stack.pop()
                if self._state.has_task(dep_id):
                    dep = self._state.get_task(dep_id)
                    if dep_id not in upstream_status_table:
                        if dep.status == PENDING and dep.deps:
                            task_stack = task_stack + [dep_id] + list(dep.deps)
                            upstream_status_table[dep_id] = ''  # will be updated postorder
                        else:
                            dep_status = STATUS_TO_UPSTREAM_MAP.get(dep.status, '')
                            upstream_status_table[dep_id] = dep_status
                    elif upstream_status_table[dep_id] == '' and dep.deps:
                        # This is the postorder update step when we set the
                        # status based on the previously calculated child elements
                        upstream_status = [upstream_status_table.get(task_id, '') for task_id in dep.deps]
                        upstream_status.append('')  # to handle empty list
                        status = max(upstream_status, key=UPSTREAM_SEVERITY_KEY)
                        upstream_status_table[dep_id] = status
            return upstream_status_table[dep_id]

    def _serialize_task(self, task_id, include_deps=True):
        task = self._state.get_task(task_id)
        ret = {
            'deps': list(task.deps),
            'status': task.status,
            'workers': list(task.workers),
            'worker_running': task.worker_running,
            'time_running': getattr(task, "time_running", None),
            'start_time': task.time,
            'params': task.params,
            'name': task.family,
            'priority': task.priority,
            'resources': task.resources,
        }
        if task.status == DISABLED:
            ret['re_enable_able'] = task.scheduler_disable_time is not None
        if include_deps:
            ret['deps'] = list(task.deps)
        return ret

    def graph(self, **kwargs):
        self.prune()
        serialized = {}
        for task in self._state.get_active_tasks():
            serialized[task.id] = self._serialize_task(task.id)
        return serialized

    def _recurse_deps(self, task_id, serialized):
        if task_id not in serialized:
            task = self._state.get_task(task_id)
            if task is None or not task.family:
                logger.warn('Missing task for id [%s]', task_id)

                # try to infer family and params from task_id
                try:
                    family, _, param_str = task_id.rstrip(')').partition('(')
                    params = dict(param.split('=') for param in param_str.split(', '))
                except BaseException:
                    family, params = '', {}
                serialized[task_id] = {
                    'deps': [],
                    'status': UNKNOWN,
                    'workers': [],
                    'start_time': UNKNOWN,
                    'params': params,
                    'name': family,
                    'priority': 0,
                }
            else:
                serialized[task_id] = self._serialize_task(task_id)
                for dep in task.deps:
                    self._recurse_deps(dep, serialized)

    def dep_graph(self, task_id, **kwargs):
        self.prune()
        serialized = {}
        if self._state.has_task(task_id):
            self._recurse_deps(task_id, serialized)
        return serialized

    def task_list(self, status, upstream_status, limit=True, **kwargs):
        """
        Query for a subset of tasks by status.
        """
        self.prune()
        result = {}
        upstream_status_table = {}  # used to memoize upstream status
        for task in self._state.get_active_tasks(status):
            if (task.status != PENDING or not upstream_status or
                    upstream_status == self._upstream_status(task.id, upstream_status_table)):
                serialized = self._serialize_task(task.id, False)
                result[task.id] = serialized
        if limit and len(result) > self._config.max_shown_tasks:
            return {'num_tasks': len(result)}
        return result

    def worker_list(self, include_running=True, **kwargs):
        self.prune()
        workers = [
            dict(
                name=worker.id,
                last_active=worker.last_active,
                started=getattr(worker, 'started', None),
                **worker.info
            ) for worker in self._state.get_active_workers()]
        workers.sort(key=lambda worker: worker['started'], reverse=True)
        if include_running:
            running = collections.defaultdict(dict)
            num_pending = collections.defaultdict(int)
            num_uniques = collections.defaultdict(int)
            for task in self._state.get_pending_tasks():
                if task.status == RUNNING and task.worker_running:
                    running[task.worker_running][task.id] = self._serialize_task(task.id, False)
                elif task.status == PENDING:
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

    def inverse_dep_graph(self, task_id, **kwargs):
        self.prune()
        serialized = {}
        if self._state.has_task(task_id):
            self._traverse_inverse_deps(task_id, serialized)
        return serialized

    def _traverse_inverse_deps(self, task_id, serialized):
        stack = [task_id]
        serialized[task_id] = self._serialize_task(task_id)
        while len(stack) > 0:
            curr_id = stack.pop()
            for task in self._state.get_active_tasks():
                if curr_id in task.deps:
                    serialized[curr_id]["deps"].append(task.id)
                    if task.id not in serialized:
                        serialized[task.id] = self._serialize_task(task.id)
                        serialized[task.id]["deps"] = []
                        stack.append(task.id)

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
                serialized = self._serialize_task(task.id, False)
                result[task.status][task.id] = serialized
        return result

    def re_enable_task(self, task_id):
        serialized = {}
        task = self._state.get_task(task_id)
        if task and task.status == DISABLED and task.scheduler_disable_time:
            self._state.re_enable(task, self._config)
            serialized = self._serialize_task(task_id)
        return serialized

    def fetch_error(self, task_id, **kwargs):
        if self._state.has_task(task_id):
            return {"taskId": task_id, "error": self._state.get_task(task_id).expl}
        else:
            return {"taskId": task_id, "error": ""}

    def _update_task_history(self, task_id, status, host=None):
        try:
            if status == DONE or status == FAILED:
                successful = (status == DONE)
                self._task_history.task_finished(task_id, successful)
            elif status == PENDING:
                self._task_history.task_scheduled(task_id)
            elif status == RUNNING:
                self._task_history.task_started(task_id, host)
        except BaseException:
            logger.warning("Error saving Task history", exc_info=True)

    @property
    def task_history(self):
        # Used by server.py to expose the calls
        return self._task_history
