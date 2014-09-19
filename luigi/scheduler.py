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
import datetime
import os
import logging
import time
import cPickle as pickle
import task_history as history
logger = logging.getLogger("luigi.server")

from task_status import PENDING, FAILED, DONE, RUNNING, UNKNOWN


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

UPSTREAM_SEVERITY_ORDER = ('', UPSTREAM_RUNNING, UPSTREAM_MISSING_INPUT, UPSTREAM_FAILED)
UPSTREAM_SEVERITY_KEY = lambda st: UPSTREAM_SEVERITY_ORDER.index(st)
STATUS_TO_UPSTREAM_MAP = {FAILED: UPSTREAM_FAILED, RUNNING: UPSTREAM_RUNNING, PENDING: UPSTREAM_MISSING_INPUT}


class Task(object):
    def __init__(self, status, deps, resources={}, priority=0, family='', params={}):
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

    def __repr__(self):
        return "Task(%r)" % vars(self)


class Worker(object):
    """ Structure for tracking worker activity and keeping their references """
    def __init__(self, id, last_active=None):
        self.id = id
        self.reference = None  # reference to the worker in the real world. (Currently a dict containing just the host)
        self.last_active = last_active  # seconds since epoch
        self.info = {}

    def add_info(self, info):
        self.info.update(info)

    def __str__(self):
        return self.id


class CentralPlannerScheduler(Scheduler):
    ''' Async scheduler that can handle multiple workers etc

    Can be run locally or on a server (using RemoteScheduler + server.Server).
    '''

    def __init__(self, retry_delay=900.0, remove_delay=600.0, worker_disconnect_delay=60.0,
                 state_path='/var/lib/luigi-server/state.pickle', task_history=None,
                 resources=None):
        '''
        (all arguments are in seconds)
        Keyword Arguments:
        retry_delay -- How long after a Task fails to try it again, or -1 to never retry
        remove_delay -- How long after a Task finishes to remove it from the scheduler
        state_path -- Path to state file (tasks and active workers)
        worker_disconnect_delay -- If a worker hasn't communicated for this long, remove it from active workers
        '''
        self._state_path = state_path
        self._tasks = {}  # map from id to a Task object
        self._retry_delay = retry_delay
        self._remove_delay = remove_delay
        self._worker_disconnect_delay = worker_disconnect_delay
        self._active_workers = {}  # map from id to a Worker object
        self._task_history = task_history or history.NopHistory()
        self._resources = resources

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

    def prune(self):
        logger.info("Starting pruning of task graph")
        # Delete workers that haven't said anything for a while (probably killed)
        delete_workers = []
        for worker in self._active_workers.values():
            if worker.last_active < time.time() - self._worker_disconnect_delay:
                logger.info("Worker %s timed out (no contact for >=%ss)", worker, self._worker_disconnect_delay)
                delete_workers.append(worker.id)

        for worker in delete_workers:
            self._active_workers.pop(worker)

        remaining_workers = set(self._active_workers.keys())

        # Mark tasks with no remaining active stakeholders for deletion
        for task_id, task in self._tasks.iteritems():
            if not task.stakeholders.intersection(remaining_workers):
                if task.remove is None:
                    logger.info("Task %r has stakeholders %r but none remain connected -> will remove task in %s seconds", task_id, task.stakeholders, self._remove_delay)
                    task.remove = time.time() + self._remove_delay

            if task.status == RUNNING and task.worker_running and task.worker_running not in remaining_workers:
                # If a running worker disconnects, tag all its jobs as FAILED and subject it to the same retry logic
                logger.info("Task %r is marked as running by disconnected worker %r -> marking as FAILED with retry delay of %rs", task_id, task.worker_running, self._retry_delay)
                task.worker_running = None
                task.status = FAILED
                task.retry = time.time() + self._retry_delay

        # Remove tasks that have no stakeholders
        remove_tasks = []
        for task_id, task in self._tasks.iteritems():
            if task.remove and time.time() > task.remove:
                logger.info("Removing task %r (no connected stakeholders)", task_id)
                remove_tasks.append(task_id)

        for task_id in remove_tasks:
            self._tasks.pop(task_id)

        # Reset FAILED tasks to PENDING if max timeout is reached, and retry delay is >= 0
        for task in self._tasks.values():
            if task.status == FAILED and self._retry_delay >= 0 and task.retry < time.time():
                task.status = PENDING
        logger.info("Done pruning task graph")

    def update(self, worker_id, worker_reference=None):
        """ Keep track of whenever the worker was last active """
        worker = self._active_workers.setdefault(worker_id, Worker(worker_id))
        if worker_reference:
            worker.reference = worker_reference
        worker.last_active = time.time()

    def _update_priority(self, task, prio, worker):
        """ Update priority of the given task

        Priority can only be increased. If the task doesn't exist, a placeholder
        task is created to preserve priority when the task is later scheduled.
        """
        task.priority = prio = max(prio, task.priority)
        for dep in task.deps or []:
            t = self._tasks[dep] # This should always exist, see add_task
            if prio > t.priority:
                self._update_priority(t, prio, worker)

    def add_task(self, worker, task_id, status=PENDING, runnable=True, deps=None, expl=None,
                 resources=None, priority=0, family='', params={}):
        """
        * Add task identified by task_id if it doesn't exist
        * If deps is not None, update dependency list
        * Update status of task
        * Add additional workers/stakeholders
        * Update priority when needed
        """
        self.update(worker)

        task = self._tasks.setdefault(task_id, Task(
            status=PENDING, deps=deps, resources=resources, priority=priority, family=family,
            params=params))

        # for setting priority, we'll sometimes create tasks with unset family and params
        if not task.family:
            task.family = family
        if not task.params:
            task.params = params

        if task.remove is not None:
            task.remove = None  # unmark task for removal so it isn't removed after being added

        if not (task.status == RUNNING and status == PENDING):
            # don't allow re-scheduling of task while it is running, it must either fail or succeed first
            if status == PENDING or status != task.status:
                # Update the DB only if there was a acctual change, to prevent noise.
                # We also check for status == PENDING b/c that's the default value
                # (so checking for status != task.status woule lie)
                self._update_task_history(task_id, status)
            task.status = status
            if status == FAILED:
                task.retry = time.time() + self._retry_delay

        if deps is not None:
            task.deps = set(deps)

        task.stakeholders.add(worker)
        task.resources = resources

        # Task dependencies might not exist yet. Let's create dummy tasks for them for now.
        # Otherwise the task dependencies might end up being pruned if scheduling takes a long time
        for dep in task.deps or []:
            t = self._tasks.setdefault(dep, Task(status=UNKNOWN, deps=None, priority=priority))
            t.stakeholders.add(worker)

        self._update_priority(task, priority, worker)

        if runnable:
            task.workers.add(worker)

        if expl is not None:
            task.expl = expl

    def add_worker(self, worker, info):
        self._active_workers[worker].add_info(info)

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
            for task in self._tasks.itervalues():
                if task.status == RUNNING and task.resources:
                    for resource, amount in task.resources.items():
                        used_resources[resource] += amount
        return used_resources

    def _rank(self, worker):
        ''' Return worker's rank function for task scheduling '''
        return lambda (task_id, task): (task.priority, worker in task.workers, -task.time)

    def _not_schedulable(self, task, used_resources):
        return any((
            task.status != PENDING,
            any(dep not in self._tasks or self._tasks[dep].status != DONE for dep in task.deps),
            not self._has_resources(task.resources, used_resources)
        ))

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
        self.update(worker, {'host': host})
        best_task = None
        locally_pending_tasks = 0
        running_tasks = []
        used_resources = self._used_resources()
        potential_resources = collections.defaultdict(int)
        potential_workers = set([worker])

        for task_id, task in sorted(self._tasks.iteritems(), key=self._rank(worker), reverse=True):
            if task.status == RUNNING and worker in task.workers:
                # Return a list of currently running tasks to the client,
                # makes it easier to troubleshoot
                other_worker = self._active_workers[task.worker_running]
                more_info = {'task_id': task_id, 'worker': str(other_worker)}
                if other_worker is not None:
                    more_info.update(other_worker.info)
                running_tasks.append(more_info)

            if task.status == PENDING and worker in task.workers:
                locally_pending_tasks += 1

            if self._not_schedulable(task, potential_resources) or best_task:
                continue

            if worker in task.workers and self._has_resources(task.resources, used_resources):
                best_task = task_id
            else:
                # keep track of the resources used in greedy scheduling
                for w in filter(lambda w: w not in potential_workers, task.workers):
                    for resource, amount in (task.resources or {}).items():
                        potential_resources[resource] += amount
                    potential_workers.add(w)

        if best_task:
            t = self._tasks[best_task]
            t.status = RUNNING
            t.worker_running = worker
            t.time_running = time.time()
            self._update_task_history(best_task, RUNNING, host=host)

        return {'n_pending_tasks': locally_pending_tasks,
                'task_id': best_task,
                'running_tasks': running_tasks}

    def ping(self, worker):
        self.update(worker)

    def _upstream_status(self, task_id, upstream_status_table):
        if task_id in upstream_status_table:
            return upstream_status_table[task_id]
        elif task_id in self._tasks:
            task_stack = [task_id]

            while task_stack:
                dep_id = task_stack.pop()
                if dep_id in self._tasks:
                    dep = self._tasks[dep_id]
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

    def _serialize_task(self, task_id):
        task = self._tasks[task_id]
        return {
            'deps': list(task.deps),
            'status': task.status,
            'workers': list(task.workers),
            'worker_running': task.worker_running,
            'time_running': getattr(task, "time_running", None),
            'start_time': task.time,
            'params': task.params,
            'name': task.family
        }

    def graph(self):
        self.prune()
        serialized = {}
        for task_id, task in self._tasks.iteritems():
            serialized[task_id] = self._serialize_task(task_id)
        return serialized

    def _recurse_deps(self, task_id, serialized):
        if task_id not in serialized:
            task = self._tasks.get(task_id)
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
                    'name': family
                }
            else:
                serialized[task_id] = self._serialize_task(task_id)
                for dep in task.deps:
                    self._recurse_deps(dep, serialized)

    def dep_graph(self, task_id):
        self.prune()
        serialized = {}
        if task_id in self._tasks:
            self._recurse_deps(task_id, serialized)
        return serialized

    def task_list(self, status, upstream_status):
        ''' query for a subset of tasks by status '''
        self.prune()
        result = {}
        upstream_status_table = {}  # used to memoize upstream status
        for task_id, task in self._tasks.iteritems():
            if not status or task.status == status:
                if (task.status != PENDING or not upstream_status or
                    upstream_status == self._upstream_status(task_id, upstream_status_table)):
                    serialized = self._serialize_task(task_id)
                    result[task_id] = serialized
        return result

    def inverse_dependencies(self, task_id):
        self.prune()
        serialized = {}
        if task_id in self._tasks:
            self._traverse_inverse_deps(task_id, serialized)
        return serialized

    def _traverse_inverse_deps(self, task_id, serialized):
        stack = [task_id]
        serialized[task_id] = self._serialize_task(task_id)
        while len(stack) > 0:
            curr_id = stack.pop()
            for id, task in self._tasks.iteritems():
                if curr_id in task.deps:
                    serialized[curr_id]["deps"].append(id)
                    if id not in serialized:
                        serialized[id] = self._serialize_task(id)
                        serialized[id]["deps"] = []
                        stack.append(id)

    def task_search(self, task_str):
        ''' query for a subset of tasks by task_id '''
        self.prune()
        result = collections.defaultdict(dict)
        for task_id, task in self._tasks.iteritems():
            if task_id.find(task_str) != -1:
                serialized = self._serialize_task(task_id)
                result[task.status][task_id] = serialized
        return result

    def fetch_error(self, task_id):
        if self._tasks[task_id].expl is not None:
            return {"taskId": task_id, "error": self._tasks[task_id].expl}
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
