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


class Task(object):
    def __init__(self, status, deps):
        self.stakeholders = set()  # workers that are somehow related to this task (i.e. don't prune while any of these workers are still active)
        self.workers = set()  # workers that can perform task - task is 'BROKEN' if none of these workers are active
        if deps is None:
            self.deps = set()
        else:
            self.deps = set(deps)
        self.status = status  # PENDING, RUNNING, FAILED or DONE
        self.time = time.time()  # Timestamp when task was first added
        self.retry = None
        self.remove = None
        self.worker_running = None  # the worker that is currently running the task or None
        self.expl = None

    def __repr__(self):
        return "Task(%r)" % vars(self)


class CentralPlannerScheduler(Scheduler):
    ''' Async scheduler that can handle multiple workers etc

    Can be run locally or on a server (using RemoteScheduler + server.Server).
    '''

    def __init__(self, retry_delay=900.0, remove_delay=600.0, worker_disconnect_delay=60.0, task_history=None):
        '''
        (all arguments are in seconds)
        Keyword Arguments:
        retry_delay -- How long after a Task fails to try it again, or -1 to never retry
        remove_delay -- How long after a Task finishes to remove it from the scheduler
        worker_disconnect_delay -- If a worker hasn't communicated for this long, remove it from active workers
        '''
        self._state_path = '/var/lib/luigi-server/state.pickle'
        self._tasks = {}
        self._retry_delay = retry_delay
        self._remove_delay = remove_delay
        self._worker_disconnect_delay = worker_disconnect_delay
        self._active_workers = {}  # map from id to timestamp (last updated)
        self._task_history = task_history or history.NopHistory()
        # TODO: have a Worker object instead, add more data to it

    def dump(self):
        state = (self._tasks, self._active_workers)
        try:
            with open(self._state_path, 'w') as fobj:
                pickle.dump(state, fobj)
        except IOError:
            logger.warning("Failed saving scheduler state", exc_info=1)
        else:
            logger.info("Saved state in %s", self._state_path)

    def load(self):
        if os.path.exists(self._state_path):
            logger.info("Attempting to load state from %s", self._state_path)
            with open(self._state_path) as fobj:
                state = pickle.load(fobj)
            self._tasks, self._active_workers = state
        else:
            logger.info("No prior state file exists at %s. Starting with clean slate", self._state_path)

    def prune(self):
        logger.info("Starting pruning of task graph")
        # Delete workers that haven't said anything for a while (probably killed)
        delete_workers = []
        for worker in self._active_workers:
            if self._active_workers[worker] < time.time() - self._worker_disconnect_delay:
                logger.info("worker %r updated at %s timed out (no contact for >=%ss)", worker, self._active_workers[worker], self._worker_disconnect_delay)
                delete_workers.append(worker)

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

    def update(self, worker):
        # update timestamp so that we keep track
        # of whenever the worker was last active
        self._active_workers[worker] = time.time()

    def add_task(self, worker, task_id, status=PENDING, runnable=True, deps=None, expl=None):
        """
        * Add task identified by task_id if it doesn't exist
        * If deps is not None, update dependency list
        * Update status of task
        * Add additional workers/stakeholders
        """
        self.update(worker)

        task = self._tasks.setdefault(task_id, Task(status=PENDING, deps=deps))

        if task.remove is not None:
            task.remove = None  # unmark task for removal so it isn't removed after being added

        if not (task.status == RUNNING and status == PENDING):
            # don't allow re-scheduling of task while it is running, it must either fail or succeed first
            task.status = status
            if status == FAILED:
                task.retry = time.time() + self._retry_delay

        if deps is not None:
            task.deps = set(deps)

        task.stakeholders.add(worker)

        if runnable:
            task.workers.add(worker)

        if expl is not None:
            task.expl = expl
        self._update_task_history(task_id, status)

    def get_work(self, worker, host=None):
        # TODO: remove any expired nodes

        # Algo: iterate over all nodes, find first node with no dependencies

        # TODO: remove tasks that can't be done, figure out if the worker has absolutely
        # nothing it can wait for

        # Return remaining tasks that have no FAILED descendents
        self.update(worker)
        best_t = float('inf')
        best_task = None
        locally_pending_tasks = 0

        for task_id, task in self._tasks.iteritems():
            if worker not in task.workers:
                continue

            if task.status != PENDING:
                continue

            locally_pending_tasks += 1
            ok = True
            for dep in task.deps:
                if dep not in self._tasks:
                    ok = False
                elif self._tasks[dep].status != DONE:
                    ok = False

            if ok:
                if task.time < best_t:
                    best_t = task.time
                    best_task = task_id

        if best_task:
            t = self._tasks[best_task]
            t.status = RUNNING
            t.worker_running = worker
            self._update_task_history(best_task, RUNNING, host=host)

        return locally_pending_tasks, best_task

    def ping(self, worker):
        self.update(worker)

    def _upstream_status(self, task, upstream_status_table):
        def get_upstream_status(task, upstream_status_table):
            if task.status != PENDING:
                return ''
            if not task.deps:
                return UPSTREAM_MISSING_INPUT
            status = ''
            status_key = lambda st: UPSTREAM_SEVERITY_ORDER.index(st)
            for dep_id in task.deps:
                if dep_id in self._tasks:
                    dep = self._tasks[dep_id]
                    if dep.status == FAILED:
                        return UPSTREAM_FAILED
                    if dep.status == RUNNING:
                        status = max(status, UPSTREAM_RUNNING, key=status_key)
                    elif dep.status == PENDING:
                        status = max(status, self._upstream_status(dep, upstream_status_table), key=status_key)
                    if status == UPSTREAM_FAILED:
                        return UPSTREAM_FAILED
            return status

        if task in upstream_status_table:
            return upstream_status_table[task]
        else:
            task_status = get_upstream_status(task, upstream_status_table)
            upstream_status_table[task] = task_status
            return task_status

    def _serialize_task(self, task_id):
        task = self._tasks[task_id]
        return {
            'deps': list(task.deps),
            'status': task.status,
            'workers': list(task.workers),
            'start_time': task.time,
            'params': self._get_task_params(task_id),
            'name': self._get_task_name(task_id)
        }

    def _get_task_params(self, task_id):
        params = {}
        params_part = task_id.split('(')[1].strip(')')
        params_strings = params_part.split(", ")

        for param in params_strings:
            if not param:
                continue
            split_param = param.split('=')
            if len(split_param) != 2:
                return {'<complex parameters>': params_part}
            params[split_param[0]] = split_param[1]
        return params

    def _get_task_name(self, task_id):
        return task_id.split('(')[0]

    def graph(self):
        self.prune()
        serialized = {}
        for task_id, task in self._tasks.iteritems():
            serialized[task_id] = self._serialize_task(task_id)
        return serialized

    def _recurse_deps(self, task_id, serialized):
        if task_id not in serialized:
            task = self._tasks.get(task_id)
            if task is None:
                logger.warn('Missing task for id [%s]' % task_id)
                serialized[task_id] = {
                    'deps': [],
                    'status': UNKNOWN,
                    'workers': [],
                    'start_time': UNKNOWN,
                    'params': self._get_task_params(task_id),
                    'name': self._get_task_name(task_id)
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
                    upstream_status == self._upstream_status(task, upstream_status_table)):
                    serialized = self._serialize_task(task_id)
                    result[task_id] = serialized
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
