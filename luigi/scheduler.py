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
logger = logging.getLogger("luigi-interface")


class Scheduler(object):
    ''' Abstract base class

    Note that the methods all take string arguments, not Task objects...
    '''
    add_task = NotImplemented
    get_work = NotImplemented
    ping = NotImplemented

PENDING = 'PENDING'
FAILED = 'FAILED'
DONE = 'DONE'
RUNNING = 'RUNNING'


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

    def __init__(self, retry_delay=900.0, remove_delay=600.0, worker_disconnect_delay=60.0):  # seconds
        self._state_path = '/var/lib/luigi-server/state.pickle'
        self._tasks = {}
        self._retry_delay = retry_delay
        self._remove_delay = remove_delay
        self._worker_disconnect_delay = worker_disconnect_delay
        self._active_workers = {}  # map from id to timestamp (last updated)
        # TODO: have a Worker object instead, add more data to it

    def dump(self):
        print "saving state..."
        state = (self._tasks, self._active_workers)
        with open(self._state_path, 'w') as fobj:
            pickle.dump(state, fobj)

    def load(self):
        if os.path.exists(self._state_path):
            print "loading state..."
            with open(self._state_path) as fobj:
                state = pickle.load(fobj)
            self._tasks, self._active_workers = state
        else:
            print "not loading state, %s doesn't exist" % self._state_path

    def prune(self):
        # Delete workers that haven't said anything for a while (probably killed)
        delete_workers = []
        for worker in self._active_workers:
            if self._active_workers[worker] < time.time() - self._worker_disconnect_delay:
                print 'worker', worker, 'updated at', self._active_workers[worker], 'timed out at', time.time(), '-', self._worker_disconnect_delay
                delete_workers.append(worker)

        for worker in delete_workers:
            self._active_workers.pop(worker)

        remaining_workers = set(self._active_workers.keys())

        # Mark tasks with no remaining active stakeholders for deletion
        for task_id, task in self._tasks.iteritems():
            if not task.stakeholders.intersection(remaining_workers):
                if task.remove == None:
                    print 'task', task_id, 'has stakeholders', task.stakeholders, 'but only', remaining_workers, 'remain -> will remove task in', self._remove_delay, 'seconds'
                    task.remove = time.time() + self._remove_delay

            if task.status == RUNNING and task.worker_running and task.worker_running not in remaining_workers:
                # If a running worker disconnects, tag all its jobs as FAILED and subject it to the same retry logic
                print 'task', task_id, 'is marked as running by inactive worker', task.worker_running, '(only', remaining_workers, 'remain) -> marking as FAILED'
                task.worker_running = None
                task.status = FAILED
                task.retry = time.time() + self._retry_delay

        # Remove tasks that have no stakeholders
        remove_tasks = []
        for task_id, task in self._tasks.iteritems():
            if task.remove and time.time() > task.remove:
                print 'Removing task', task_id
                remove_tasks.append(task_id)

        for task_id in remove_tasks:
            self._tasks.pop(task_id)

        # Reset FAILED tasks to PENDING if max timeout is reached
        for task in self._tasks.values():
            if task.status == FAILED and task.retry < time.time():
                task.status = PENDING

    def update(self, worker):
        # update timestamp so that we keep track
        # of whenever the worker was last active
        self._active_workers[worker] = time.time()
        self.prune()

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

    def get_work(self, worker):
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

        return locally_pending_tasks, best_task

    def ping(self, worker):
        self.update(worker)

    def graph(self):
        self.prune()
        serialized = {}
        for taskname, task in self._tasks.iteritems():
            serialized[taskname] = {
                'deps': list(task.deps),
                'status': task.status,
                'workers': list(task.workers)
            }
        return serialized
