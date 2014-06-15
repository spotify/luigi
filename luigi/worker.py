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

import random
from scheduler import CentralPlannerScheduler, PENDING, FAILED, DONE
import threading
import time
import os
import socket
import configuration
import traceback
import logging
import warnings
import notifications
from target import Target
from task import Task

try:
    import simplejson as json
except ImportError:
    import json

logger = logging.getLogger('luigi-interface')


class TaskException(Exception):
    pass


class Event:
    # TODO nice descriptive subclasses of Event instead of strings? pass their instances to the callback instead of an undocumented arg list?
    DEPENDENCY_DISCOVERED = "event.core.dependency.discovered"  # triggered for every (task, upstream task) pair discovered in a jobflow
    DEPENDENCY_MISSING = "event.core.dependency.missing"
    DEPENDENCY_PRESENT = "event.core.dependency.present"
    FAILURE = "event.core.failure"
    SUCCESS = "event.core.success"


class Worker(object):
    """ Worker object communicates with a scheduler.

    Simple class that talks to a scheduler and:
    - Tells the scheduler what it has to do + its dependencies
    - Asks for stuff to do (pulls it in a loop and runs it)
    """

    def __init__(self, scheduler=CentralPlannerScheduler(), worker_id=None,
                 worker_processes=1, ping_interval=None, keep_alive=None,
                 wait_interval=None):
        if not worker_id:
            worker_id = 'worker-%09d' % random.randrange(0, 999999999)

        config = configuration.get_config()

        if ping_interval is None:
            ping_interval = config.getfloat('core', 'worker-ping-interval', 1.0)

        if keep_alive is None:
            keep_alive = config.getboolean('core', 'worker-keep-alive', False)
        self.__keep_alive = keep_alive

        if keep_alive:
            if wait_interval is None:
                wait_interval = config.getint('core', 'worker-wait-interval', 1)
            self.__wait_interval = wait_interval

        self._id = worker_id
        self._scheduler = scheduler
        if (isinstance(scheduler, CentralPlannerScheduler)
                and worker_processes != 1):
            warnings.warn("Will only use one process when running with local in-process scheduler")
            worker_processes = 1

        self.worker_processes = worker_processes
        self.host = socket.gethostname()
        self._scheduled_tasks = {}

        # store the previous tasks executed by the same worker
        # for debugging reasons
        self._previous_tasks = []

        class KeepAliveThread(threading.Thread):
            """ Periodically tell the scheduler that the worker still lives """
            def __init__(self):
                super(KeepAliveThread, self).__init__()
                self._should_stop = threading.Event()

            def stop(self):
                self._should_stop.set()

            def run(self):
                while True:
                    self._should_stop.wait(ping_interval)
                    if self._should_stop.is_set():
                        logger.info("Worker %s was stopped. Shutting down Keep-Alive thread" % worker_id)
                        break
                    try:
                        scheduler.ping(worker=worker_id)
                    except:  # httplib.BadStatusLine:
                        logger.warning('Failed pinging scheduler')

        self._keep_alive_thread = KeepAliveThread()
        self._keep_alive_thread.daemon = True
        self._keep_alive_thread.start()

    def stop(self):
        """ Stop the KeepAliveThread associated with this Worker
            This should be called whenever you are done with a worker instance to clean up

        Warning: this should _only_ be performed if you are sure this worker
        is not performing any work or will perform any work after this has been called

        TODO: also kill all currently running tasks
        TODO (maybe): Worker should be/have a context manager to enforce calling this
            whenever you stop using a Worker instance
        """
        self._keep_alive_thread.stop()
        self._keep_alive_thread.join()

    def _validate_task(self, task):
        if not isinstance(task, Task):
            raise TaskException('Can not schedule non-task %s' % task)

        if not task.initialized():
            # we can't get the repr of it since it's not initialized...
            raise TaskException('Task of class %s not initialized. Did you override __init__ and forget to call super(...).__init__?' % task.__class__.__name__)

    def _log_complete_error(self, task):
        log_msg = "Will not schedule {task} or any dependencies due to error in complete() method:".format(task=task)
        logger.warning(log_msg, exc_info=1)  # Needs to be called from except-clause to work

    def _log_unexpected_error(self, task):
        logger.exception("Luigi unexpected framework error while scheduling %s", task) # needs to be called from within except clause

    def _email_complete_error(self, task, formatted_traceback):
          # like logger.exception but with WARNING level
        subject = "Luigi: {task} failed scheduling".format(task=task)
        message = "Will not schedule {task} or any dependencies due to error in complete() method:\n{traceback}".format(task=task, traceback=formatted_traceback)
        notifications.send_error_email(subject, message)

    def _email_unexpected_error(self, task, formatted_traceback):
        subject = "Luigi: Framework error while scheduling {task}".format(task=task)
        message = "Luigi framework error:\n{traceback}".format(traceback=formatted_traceback)
        notifications.send_error_email(subject, message)

    def add(self, task):
        """ Add a Task for the worker to check and possibly schedule and run """
        stack = [task]
        try:
            while stack:
                current = stack.pop()
                for next in self._add(current):
                    stack.append(next)
        except (KeyboardInterrupt, TaskException):
            raise
        except:
            formatted_traceback = traceback.format_exc()
            self._log_unexpected_error(task)
            self._email_unexpected_error(task, formatted_traceback)

    def _check_complete(self, task):
        return task.complete()

    def _add(self, task):
        self._validate_task(task)
        if task.task_id in self._scheduled_tasks:
            return []  # already scheduled
        logger.debug("Checking if %s is complete", task)
        is_complete = False
        try:
            is_complete = self._check_complete(task)
            self._check_complete_value(is_complete)
        except KeyboardInterrupt:
            raise
        except:
            formatted_traceback = traceback.format_exc()
            self._log_complete_error(task)
            task.trigger_event(Event.DEPENDENCY_MISSING, task)
            self._email_complete_error(task, formatted_traceback)
            # abort, i.e. don't schedule any subtasks of a task with
            # failing complete()-method since we don't know if the task
            # is complete and subtasks might not be desirable to run if
            # they have already ran before
            return []

        if is_complete:
            # Not submitting dependencies of finished tasks
            self._scheduler.add_task(self._id, task.task_id, status=DONE,
                                      runnable=False)
            task.trigger_event(Event.DEPENDENCY_PRESENT, task)
        elif task.run == NotImplemented:
            self._add_external(task)
        else:
            return self._add_task_and_deps(task)
        return []

    def _add_external(self, external_task):
        self._scheduled_tasks[external_task.task_id] = external_task
        self._scheduler.add_task(self._id, external_task.task_id, status=PENDING,
                                  runnable=False, priority=external_task.priority)
        external_task.trigger_event(Event.DEPENDENCY_MISSING, external_task)
        logger.warning('Task %s is not complete and run() is not implemented. Probably a missing external dependency.', external_task.task_id)

    def _validate_dependency(self, dependency):
        if isinstance(dependency, Target):
            raise Exception('requires() can not return Target objects. Wrap it in an ExternalTask class')
        elif not isinstance(dependency, Task):
            raise Exception('requires() must return Task objects')

    def _add_task_and_deps(self, task):
        self._scheduled_tasks[task.task_id] = task
        deps = task.deps()
        for d in deps:
            self._validate_dependency(d)
            task.trigger_event(Event.DEPENDENCY_DISCOVERED, task, d)

        deps = [d.task_id for d in deps]
        self._scheduler.add_task(self._id, task.task_id, status=PENDING,
                                  deps=deps, runnable=True, priority=task.priority)
        logger.info('Scheduled %s', task.task_id)

        for d in task.deps():
            yield d  # return additional tasks to add

    def _check_complete_value(self, is_complete):
        if is_complete not in (True, False):
            raise Exception("Return value of Task.complete() must be boolean (was %r)" % is_complete)

    def _run_task(self, task_id):
        task = self._scheduled_tasks[task_id]

        logger.info('[pid %s] Worker %s running   %s', os.getpid(), self._id, task_id)
        try:
            # Verify that all the tasks are fulfilled!
            ok = True
            for task_2 in task.deps():
                if not task_2.complete():
                    ok = False
                    missing_dep = task_2

            if not ok:
                # TODO: possibly try to re-add task again ad pending
                raise RuntimeError('Unfulfilled dependency %r at run time!\nPrevious tasks: %r' % (missing_dep.task_id, self._previous_tasks))
            task.run()
            error_message = json.dumps(task.on_success())
            logger.info('[pid %s] Worker %s done      %s', os.getpid(), self._id, task_id)
            task.trigger_event(Event.SUCCESS, task)
            status = DONE

        except KeyboardInterrupt:
            raise
        except Exception as ex:
            status = FAILED
            logger.exception("[pid %s] Worker %s failed    %s", os.getpid(), self._id, task)
            error_message = task.on_failure(ex)
            task.trigger_event(Event.FAILURE, task, ex)
            subject = "Luigi: %s FAILED" % task
            notifications.send_error_email(subject, error_message)

        self._scheduler.add_task(self._id, task_id, status=status,
                                  expl=error_message, runnable=None)

        return status

    def _log_remote_tasks(self, running_tasks, n_pending_tasks):
        logger.info("Done")
        logger.info("There are no more tasks to run at this time")
        if running_tasks:
            for r in running_tasks:
                logger.info('%s is currently run by worker %s', r['task_id'], r['worker'])
        elif n_pending_tasks:
            logger.info("There are %s pending tasks possibly being run by other workers", n_pending_tasks)

    def _reap_children(self, children):
        died_pid, status = os.wait()
        if died_pid in children:
            children.remove(died_pid)
        else:
            logger.warning("Some random process %s died", died_pid)

    def _get_work(self):
        logger.debug("Asking scheduler for work...")
        r = self._scheduler.get_work(worker=self._id, host=self.host)
        # Support old version of scheduler
        if isinstance(r, tuple) or isinstance(r, list):
            n_pending_tasks, task_id = r
            running_tasks = []
        else:
            n_pending_tasks = r['n_pending_tasks']
            task_id = r['task_id']
            running_tasks = r['running_tasks']
        return task_id, running_tasks, n_pending_tasks

    def _fork_task(self, children, task_id):
        child_pid = os.fork()
        if child_pid:
            children.add(child_pid)
        else:
            # need to have different random seeds...
            random.seed((os.getpid(), time.time()))
            self._run_task(task_id)
            os._exit(0)

    def _sleeper(self):
        # TODO is exponential backoff necessary?
        while True:
            wait_interval = self.__wait_interval + random.randint(1, 5)
            logger.debug('Sleeping for %d seconds', wait_interval)
            time.sleep(wait_interval)
            yield

    def run(self):
        children = set()
        sleeper  = self._sleeper()

        while True:
            while len(children) >= self.worker_processes:
                self._reap_children(children)

            task_id, running_tasks, n_pending_tasks = self._get_work()

            if task_id is None:
                self._log_remote_tasks(running_tasks, n_pending_tasks)
                if not children:
                    if self.__keep_alive and running_tasks and n_pending_tasks:
                        sleeper.next()
                        continue
                    else:
                        break
                else:
                    self._reap_children(children)
                    continue

            # task_id is not None:
            logger.debug("Pending tasks: %s", n_pending_tasks)
            if self.worker_processes > 1:
                self._fork_task(children, task_id)
            else:
                self._run_task(task_id)

            self._previous_tasks.append(task_id)

        while children:
            self._reap_children(children)
