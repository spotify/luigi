# Copyright (c) 2012 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import collections
import heapq
import datetime
import functools
import notifications
import os
import logging
import time
import cPickle as pickle
import task_history as history
logger = logging.getLogger("luigi.server")

from task_status import PENDING, FAILED, DONE, RUNNING, SUSPENDED, UNKNOWN, DISABLED


class Scheduler(object):
    ''' Abstract base class

    Note that the methods all take string arguments, not Task objects...
    '''
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
UPSTREAM_SEVERITY_KEY = lambda st: UPSTREAM_SEVERITY_ORDER.index(st)
STATUS_TO_UPSTREAM_MAP = {
    FAILED: UPSTREAM_FAILED,
    RUNNING: UPSTREAM_RUNNING,
    PENDING: UPSTREAM_MISSING_INPUT,
    DISABLED: UPSTREAM_DISABLED,
}


class Failures(object):
    """ This class tracks the number of failures in a given time window

    Failures added are marked with the current timestamp, and this class counts
    the number of failures in a sliding time window ending at the present.

    """

    def __init__(self, window):
        """ Initialize with the given window

        :param window: how long to track failures for, as a datetime.timedelta
        """
        self.window = window
        self.failures = collections.deque()

    def add_failure(self):
        """ Add a failure event with the current timestamp """
        self.failures.append(datetime.datetime.now())

    def num_failures(self):
        """ Return the number of failures in the window """
        min_time = datetime.datetime.now() - self.window
        while self.failures and self.failures[0] < min_time:
            self.failures.popleft()
        return len(self.failures)

    def clear(self):
        """ Clear the failure queue """
        self.failures.clear()


class Task(object):
    def __init__(self, id, status, deps, resources={}, priority=0, family='', params={},
                 disable_failures=None, disable_window=None):
        self.id = id
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
        self.resources = resources
        self.family = family
        self.params = params
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

    def re_enable(self):
        self.scheduler_disable_time = None
        self.status = FAILED
        self.failures.clear()


class Worker(object):
    """ Structure for tracking worker activity and keeping their references """
    def __init__(self, id, last_active=None):
        self.id = id
        self.reference = None  # reference to the worker in the real world. (Currently a dict containing just the host)
        self.last_active = last_active  # seconds since epoch
        self.started = time.time()  # seconds since epoch
        self.info = {}

    def add_info(self, info):
        self.info.update(info)

    def __str__(self):
        return self.id


class SimpleTaskState(object):
    ''' Keep track of the current state and handle persistance

    The point of this class is to enable other ways to keep state, eg. by using a database
    These will be implemented by creating an abstract base class that this and other classes
    inherit from.
    '''

    def __init__(self, state_path):
        self._state_path = state_path
        self._tasks = {}  # map from id to a Task object
        self._active_workers = {}  # map from id to a Worker object

    def dump(self):
        state = (self._tasks, self._active_workers)
        try:
            with open(self._state_path, 'w') as fobj:
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
                with open(self._state_path) as fobj:
                    state = pickle.load(fobj)
            except:
                logger.exception("Error when loading state. Starting from clean slate.")
                return

            self._tasks, self._active_workers = state

            # Convert from old format
            # TODO: this is really ugly, we need something more future-proof
            # Every time we add an attribute to the Worker class, this code needs to be updated
            for k, v in self._active_workers.iteritems():
                if isinstance(v, float):
                    self._active_workers[k] = Worker(id=k, last_active=v)
        else:
            logger.info("No prior state file exists at %s. Starting with clean slate", self._state_path)

    def get_active_tasks(self):
        for task in self._tasks.itervalues():
            yield task

    def get_pending_tasks(self):
        for task in self._tasks.itervalues():
            if task.status in [PENDING, RUNNING]:
                yield task

    def get_task(self, task_id, default=None, setdefault=None):
        if setdefault:
            return self._tasks.setdefault(task_id, setdefault)
        else:
            return self._tasks.get(task_id, default)

    def has_task(self, task_id):
        return task_id in self._tasks

    def inactivate_tasks(self, delete_tasks):
        # The terminology is a bit confusing: we used to "delete" tasks when they became inactive,
        # but with a pluggable state storage, you might very well want to keep some history of
        # older tasks as well. That's why we call it "inactivate" (as in the verb)
        for task in delete_tasks:
            self._tasks.pop(task)

    def get_active_workers(self):
        for worker in self._active_workers.itervalues():
            yield worker

    def get_worker_ids(self):
        return self._active_workers.keys() # only used for unit tests

    def get_worker(self, worker_id, create=False):
        if create:
            return self._active_workers.setdefault(worker_id, Worker(worker_id))
        else:
            return self._active_workers.get(worker_id, None)

    def inactivate_workers(self, delete_workers):
        # Mark workers as inactive
        for worker in delete_workers:
            if worker in self._active_workers:
                self._active_workers.pop(worker)


class CentralPlannerScheduler(Scheduler):
    ''' Async scheduler that can handle multiple workers etc

    Can be run locally or on a server (using RemoteScheduler + server.Server).
    '''

    def __init__(self, retry_delay=900.0, remove_delay=600.0, worker_disconnect_delay=60.0,
                 state_path='/var/lib/luigi-server/state.pickle', task_history=None,
                 resources=None, disable_persist=0, disable_window=0, disable_failures=None):
        '''
        (all arguments are in seconds)
        Keyword Arguments:
        retry_delay -- How long after a Task fails to try it again, or -1 to never retry
        remove_delay -- How long after a Task finishes to remove it from the scheduler
        state_path -- Path to state file (tasks and active workers)
        worker_disconnect_delay -- If a worker hasn't communicated for this long, remove it from active workers
        '''
        self._retry_delay = retry_delay
        self._remove_delay = remove_delay
        self._worker_disconnect_delay = worker_disconnect_delay
        self._task_history = task_history or history.NopHistory()
        self._state = SimpleTaskState(state_path)

        self._task_history = task_history or history.NopHistory()
        self._resources = resources
        self._disable_failures = disable_failures
        self._disable_window = disable_window
        self._make_task = functools.partial(
            Task, disable_failures=disable_failures,
            disable_window=datetime.timedelta(seconds=disable_window))
        self._disable_persist = disable_persist
        self._disable_time = datetime.timedelta(seconds=disable_persist)

        # Keep track of a queue of checks we've scheduled
        self._event_queue = []

    def load(self):
        self._state.load()

    def dump(self):
        self._state.dump()

    def _check_old_worker(self, timestamp, worker_id):
        worker = self._state.get_worker(worker_id)
        if not worker:
            return
        if timestamp >= worker.last_active + self._worker_disconnect_delay:
            self._state.inactivate_workers([worker])

    def _check_orphan_task(self, timestamp, task_id):
        # Mark tasks with no remaining active stakeholders for deletion
        task = self._state.get_task(task_id)

        if task.remove is not None:
            return

        for worker in task.stakeholders:
            if self._state.get_worker(worker) is None:
                task.stakeholders.pop(worker)

        if not task.stakeholders:
            logger.info("Task %r has stakeholders %r but none remain connected -> will remove task in %s seconds", task.id, task.stakeholders, self._remove_delay)
            task.remove = timestamp + self._remove_delay
            self._schedule_event(task.remove, self._check_remove_task, task_id)
        else:
            # Check again later
            self._schedule_event(timestamp + self._remove_delay, self._check_orphan_task, task_id)

    def _check_running_task_with_disconnected_worker(self, timestamp, task_id):
        # If a running worker disconnects, tag all its jobs as FAILED and subject it to the same retry logic
        task = self._state.get_task(task_id)
        print task.status
        if task.status != RUNNING:
            return

        # Check if worker disconnected
        self._check_old_worker(timestamp, task.worker_running)

        if self._state.get_worker(task.worker_running):
            logger.info("Task %r is marked as running by disconnected worker %r -> marking as FAILED with retry delay of %rs", task.id, task.worker_running, self._retry_delay)
            task.worker_running = None
            self.set_status(task, FAILED)
        else:
            # Check again in a a bit later
            self._schedule_event(timestamp + self._worker_disconnect_delay, self._check_running_task_with_disconnected_worker, task_id)

    def _check_reenable_task(self, timestamp, task_id):
        # re-enable task after the disable time expires
        task = self._state.get_task(task_id)
        if task.status == DISABLED and task.scheduler_disable_time:
            if timestamp >= task.scheduler_disable_time + self._disable_time:
                task.re_enable()

    def _check_remove_task(self, timestamp, task_id):
        # Remove tasks that have no stakeholders
        task = self._state.get_task(task_id)
        if task.remove and timestamp > task.remove:
            logger.info("Removing task %r (no connected stakeholders)", task.id)
            self._state.inactivate_tasks([task.id])

    def _check_retry_task(self, timestamp, task_id):
        # Reset FAILED tasks to PENDING if max timeout is reached, and retry delay is >= 0
        task = self._state.get_task(task_id)
        print task.status, self._retry_delay, task.retry, timestamp
        if task.status == FAILED and self._retry_delay >= 0 and timestamp >= task.retry:
            self.set_status(task, PENDING)

    def prune(self):
        while len(self._event_queue):
            timestamp, function, args = self._event_queue[0]
            if timestamp > time.time():
                break
            heapq.heappop(self._event_queue)

            function(timestamp, *args)

    def _schedule_event(self, timestamp, event, *args):
        heapq.heappush(self._event_queue, (timestamp, event, args))

    def set_status(self, task, new_status, host=None):
        ''' This is the state machine of a task. The intention is no other task state updates take place elsewhere

        The state machine also schedules various checks that will perform later that may change the state of a task'''

        if task.status == RUNNING and new_status == PENDING:
            # don't allow re-scheduling of task while it is running, it must either fail or succeed first
            return

        if new_status == 'UNKNOWN':
            # UNKNOWN is reserved for new tasks, don't set it to UNKNOWN
            return

        if new_status == PENDING or task.status != PENDING:
            # Update the DB only if there was a acctual change, to prevent noise.
            # We also check for status == PENDING b/c that's the default value
            # (so checking for status != task.status woule lie)
            self._update_task_history(task.id, new_status)

        if new_status == FAILED:
            task.retry = time.time() + self._retry_delay
            self._schedule_event(task.retry, self._check_retry_task, task.id)

        # not sure why we have SUSPENDED, as it can never be set
        if new_status == SUSPENDED:
            new_status = PENDING

        if new_status == DISABLED and task.status == RUNNING:
            return

        if task.status == DISABLED:
            if new_status == DISABLED:
                task.scheduler_disable_time = None
            elif new_status == DONE:
                task.re_enable()
                task.status = DONE
            elif task.scheduler_disable_time is None:
                # when it is disabled by client, we allow the status change
                task.status = new_status
            return

        if new_status == FAILED and task.can_disable():
            task.add_failure()
            if task.has_excessive_failures():
                task.scheduler_disable_time = datetime.datetime.now()
                new_status = DISABLED
                notifications.send_error_email(
                    'Luigi Scheduler: DISABLED {task} due to excessive failures'.format(task=task.id),
                    '{task} failed {failures} times in the last {window} seconds, so it is being '
                    'disabled for {persist} seconds'.format(
                        failures=self._disable_failures,
                        task=task.id,
                        window=self._disable_window,
                        persist=self._disable_persist,
                        ))
        elif new_status == DISABLED:
            task.scheduler_disable_time = None

        if task.status == UNKNOWN:
            # New task, create a loop checking whether the task should be removed
            self._schedule_event(time.time() + self._worker_disconnect_delay,self._check_orphan_task, task.id)
        
        if new_status == RUNNING and task.status != RUNNING:
            self._schedule_event(time.time() + self._worker_disconnect_delay, self._check_running_task_with_disconnected_worker, task.id)
            self._update_task_history(task.id, RUNNING, host=host)

        task.status = new_status


    def _update_worker_activity(self, worker_id, worker_reference=None):
        """ Keep track of whenever the worker was last active """
        worker = self._state.get_worker(worker_id, create=True)
        if worker_reference:
            worker.reference = worker_reference
        worker.last_active = time.time()

        # Delete the worker later if there's no news
        self._schedule_event(time.time() + self._worker_disconnect_delay, self._check_old_worker, worker_id)

    def _update_priority(self, task, prio, worker):
        """ Update priority of the given task

        Priority can only be increased. If the task doesn't exist, a placeholder
        task is created to preserve priority when the task is later scheduled.
        """
        task.priority = prio = max(prio, task.priority)
        for dep in task.deps or []:
            t = self._state.get_task(dep)
            if t is not None and prio > t.priority:
                self._update_priority(t, prio, worker)

    def add_task(self, worker, task_id, status=PENDING, runnable=True,
                 deps=None, new_deps=None, expl=None, resources=None,
                 priority=0, family='', params={}):
        """
        * Add task identified by task_id if it doesn't exist
        * If deps is not None, update dependency list
        * Update status of task
        * Add additional workers/stakeholders
        * Update priority when needed
        """
        self._update_worker_activity(worker)

        task = self._state.get_task(task_id, setdefault=self._make_task(
                id=task_id, status=UNKNOWN, deps=deps, resources=resources,
                priority=priority, family=family, params=params))

        # for setting priority, we'll sometimes create tasks with unset family and params
        if not task.family:
            task.family = family
        if not task.params:
            task.params = params

        if task.remove is not None:
            task.remove = None  # unmark task for removal so it isn't removed after being added

        self.set_status(task, PENDING if status == SUSPENDED else status)

        if deps is not None:
            task.deps = set(deps)

        if new_deps is not None:
            task.deps.update(new_deps)

        task.stakeholders.add(worker)
        task.resources = resources

        # Task dependencies might not exist yet. Let's create dummy tasks for them for now.
        # Otherwise the task dependencies might end up being pruned if scheduling takes a long time
        for dep in task.deps or []:
            t = self._state.get_task(dep, setdefault=self._make_task(id=dep, status=UNKNOWN, deps=None, priority=priority))
            t.stakeholders.add(worker)

        self._update_priority(task, priority, worker)

        if runnable:
            task.workers.add(worker)

        if expl is not None:
            task.expl = expl

    def add_worker(self, worker, info):
        self._state.get_worker(worker).add_info(info)

    def update_resources(self, **resources):
        if self._resources is None:
            self._resources = {}
        self._resources.update(resources)

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
            for task in self._state.get_active_tasks():
                if task.status == RUNNING and task.resources:
                    for resource, amount in task.resources.items():
                        used_resources[resource] += amount
        return used_resources

    def _rank(self):
        ''' Return worker's rank function for task scheduling '''
        dependents = collections.defaultdict(int)
        def not_done(t):
            task = self._state.get_task(t, default=None)
            return task is None or task.status != DONE
        for task in self._state.get_active_tasks():
            if task.status != DONE:
                deps = filter(not_done, task.deps)
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

    def get_work(self, worker, host=None):
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
        self.prune()
        self._update_worker_activity(worker, {'host': host})
        best_task = None
        best_task_id = None
        locally_pending_tasks = 0
        running_tasks = []

        used_resources = self._used_resources()
        greedy_resources = collections.defaultdict(int)
        n_unique_pending = 0
        greedy_workers = dict((worker.id, worker.info.get('workers', 1))
                              for worker in self._state.get_active_workers())

        tasks = list(self._state.get_pending_tasks())
        tasks.sort(key=self._rank(), reverse=True)

        for task in tasks:
            if task.status == 'RUNNING' and worker in task.workers:
                # Return a list of currently running tasks to the client,
                # makes it easier to troubleshoot
                other_worker = self._state.get_worker(task.worker_running)
                more_info = {'task_id': task.id, 'worker': str(other_worker)}
                if other_worker is not None:
                    more_info.update(other_worker.info)
                    running_tasks.append(more_info)

            if task.status == PENDING and worker in task.workers:
                locally_pending_tasks += 1
                if len(task.workers) == 1:
                    n_unique_pending += 1

            if task.status == RUNNING and task.worker_running in greedy_workers:
                greedy_workers[task.worker_running] -= 1
                for resource, amount in (task.resources or {}).items():
                    greedy_resources[resource] += amount

            if not best_task and self._schedulable(task) and self._has_resources(task.resources, greedy_resources):
                if worker in task.workers and self._has_resources(task.resources, used_resources):
                    best_task = task
                    best_task_id = task.id
                else:
                    for task_worker in task.workers:
                        if greedy_workers.get(task_worker, 0) > 0:
                            # use up a worker
                            greedy_workers[task_worker] -= 1

                            # keep track of the resources used in greedy scheduling
                            for resource, amount in (task.resources or {}).items():
                                greedy_resources[resource] += amount

                            break

        if best_task:
            self.set_status(best_task, RUNNING, host=host)

            best_task.worker_running = worker
            best_task.time_running = time.time()

        return {'n_pending_tasks': locally_pending_tasks,
                'n_unique_pending': n_unique_pending,
                'task_id': best_task_id,
                'running_tasks': running_tasks}

    def ping(self, worker):
        self._update_worker_activity(worker)

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
                        upstream_status = [upstream_status_table.get(id, '') for id in dep.deps]
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
        if include_deps:
            ret['deps'] = list(task.deps)
        return ret

    def graph(self):
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
                except:
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

    def dep_graph(self, task_id):
        self.prune()
        serialized = {}
        if self._state.has_task(task_id):
            self._recurse_deps(task_id, serialized)
        return serialized

    def task_list(self, status, upstream_status):
        ''' query for a subset of tasks by status '''
        self.prune()
        result = {}
        upstream_status_table = {}  # used to memoize upstream status
        for task in self._state.get_active_tasks():
            if not status or task.status == status:
                if (task.status != PENDING or not upstream_status or
                    upstream_status == self._upstream_status(task.id, upstream_status_table)):
                    serialized = self._serialize_task(task.id, False)
                    result[task.id] = serialized
        return result

    def worker_list(self, include_running=True):
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

    def inverse_dependencies(self, task_id):
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

    def task_search(self, task_str):
        ''' query for a subset of tasks by task_id '''
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
            task.re_enable()
            serialized = self._serialize_task(task_id)
        return serialized

    def fetch_error(self, task_id):
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
        except:
            logger.warning("Error saving Task history", exc_info=1)

    @property
    def task_history(self):
        # Used by server.py to expose the calls
        return self._task_history
