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

class Worker(object):
    """ Worker object communicates with a scheduler.

    Simple class that talks to a scheduler and:
    - Tells the scheduler what it has to do + its dependencies
    - Asks for stuff to do (pulls it in a loop and runs it)
    """

    def __init__(self, scheduler=CentralPlannerScheduler(), worker_id=None, worker_processes=1):
        if not worker_id:
            worker_id = 'worker-%09d' % random.randrange(0, 999999999)

        self.__id = worker_id
        self.__scheduler = scheduler
        if isinstance(scheduler, CentralPlannerScheduler) and worker_processes != 1:
            warnings.warn("Will only use one process when running with local in-process scheduler")
            worker_processes = 1

        self.worker_processes = worker_processes
        self.__scheduled_tasks = {}

        self._previous_tasks = []  # store the previous tasks executed by the same worker for debugging reasons

        class KeepAliveThread(threading.Thread):
            """ Periodically tell the scheduler that the worker still lives """
            def run(self):
                while True:
                    time.sleep(1.0)
                    try:
                        scheduler.ping(worker=worker_id)
                    except:  # httplib.BadStatusLine:
                        print 'WARNING: could not ping!'
                        raise

        k = KeepAliveThread()
        k.daemon = True
        k.start()

    def add(self, task):
        if not isinstance(task, Task):
            raise TaskException('Can not schedule non-task %s' % task)

        if not task.initialized():
            # we can't get the repr of it since it's not initialized...
            raise TaskException('Task of class %s not initialized. Did you override __init__ and forget to call super(...).__init__?' % task.__class__.__name__)

        try:
            task_id = task.task_id

            if task_id in self.__scheduled_tasks:
                return  # already scheduled
            logger.debug("Checking if %s is complete" % task_id)
            is_complete = False
            try:
                is_complete = task.complete()
                if is_complete not in (True, False):
                    raise Exception("Return value of Task.complete() must be boolean (was %r)" % is_complete)
            except KeyboardInterrupt:
                raise
            except:
                msg = "Will not schedule %s or any dependencies due to error in complete() method:" % (task,)
                logger.warning(msg, exc_info=1)  # like logger.exception but with WARNING level
                receiver = configuration.get_config().get('core', 'error-email', None)
                sender = configuration.get_config().get('core', 'email-sender', notifications.DEFAULT_CLIENT_EMAIL)
                logger.info("Sending warning email to %r" % receiver)
                notifications.send_email(
                    subject="Luigi: %s failed scheduling" % (task,),
                    message="%s:\n%s" % (msg, traceback.format_exc()),
                    sender=sender,
                    recipients=(receiver,))
                return
                # abort, i.e. don't schedule any subtasks of a task with
                # failing complete()-method since we don't know if the task
                # is complete and subtasks might not be desirable to run if
                # they have already ran before

            if is_complete:
                # Not submitting dependencies of finished tasks
                self.__scheduler.add_task(self.__id, task_id, status=DONE, runnable=False)

            elif task.run == NotImplemented:
                self.__scheduled_tasks[task_id] = task
                self.__scheduler.add_task(self.__id, task_id, status=PENDING, runnable=False)
                logger.warning('Task %s is not complete and run() is not implemented. Probably a missing external dependency.', task_id)
            else:
                self.__scheduled_tasks[task_id] = task
                deps = task.deps()
                for d in deps:
                    if isinstance(d, Target):
                        raise Exception('requires() can not return Target objects. Wrap it in an ExternalTask class')
                    elif not isinstance(d, Task):
                        raise Exception('requires() must return Task objects')
                deps = [d.task_id for d in task.deps()]
                self.__scheduler.add_task(self.__id, task_id, status=PENDING, deps=deps, runnable=True)
                logger.info('Scheduled %s' % task_id)

                for task_2 in task.deps():
                    self.add(task_2)  # Schedule stuff recursively
        except KeyboardInterrupt:
            raise
        except:
            logger.exception("Luigi unexpected framework error while scheduling %s" % task)
            receiver = configuration.get_config().get('core', 'error-email', None)
            sender = configuration.get_config().get('core', 'email-sender', notifications.DEFAULT_CLIENT_EMAIL)
            notifications.send_email(
                subject="Luigi: Framework error while scheduling %s" % (task,),
                message="Luigi framework error:\n%s" % traceback.format_exc(),
                recipients=(receiver,),
                sender=sender)

    def _run_task(self, task_id):
        task = self.__scheduled_tasks[task_id]

        logger.info('[pid %s] Running   %s', os.getpid(), task_id)
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
            expl = json.dumps(task.on_success())
            logger.info('[pid %s] Done      %s', os.getpid(), task_id)
            status = DONE

        except KeyboardInterrupt:
            raise
        except Exception as ex:
            status = FAILED
            logger.exception("[pid %s] Error while running %s" % (os.getpid(), task))
            expl = task.on_failure(ex)
            receiver = configuration.get_config().get('core', 'error-email', None)
            sender = configuration.get_config().get('core', 'email-sender', notifications.DEFAULT_CLIENT_EMAIL)
            logger.info("[pid %s] Sending error email to %r", os.getpid(), receiver)
            notifications.send_email("Luigi: %s FAILED" % task, expl, sender, (receiver,))

        self.__scheduler.add_task(self.__id, task_id, status=status, expl=expl, runnable=None)

    def run(self):
        children = set()

        while True:
            while len(children) >= self.worker_processes:
                died_pid, status = os.wait()
                if died_pid in children:
                    children.remove(died_pid)
                else:
                    logger.warning("Some random process %s died" % died_pid)

            logger.debug("Asking scheduler for work...")
            pending_tasks, task_id = self.__scheduler.get_work(worker=self.__id)

            if task_id is None:
                logger.info("Done")
                logger.info("There are no more tasks to run at this time")
                if pending_tasks:
                    logger.info("There are %s pending tasks possibly being run by other workers", pending_tasks)
            else:
                logger.debug("Pending tasks: %s", pending_tasks)

            if task_id == None:
                # TODO: sleep for a bit and query server again if there are pending tasks in the future we might be able to run
                if not children:
                    break
                else:
                    died_pid, status = os.wait()
                    if died_pid in children:
                        children.remove(died_pid)
                    else:
                        logger.warning("Some random process %s died" % died_pid)
                    continue
            if self.worker_processes > 1:
                child_pid = os.fork()
                if child_pid:
                    children.add(child_pid)
                else:
                    random.seed((os.getpid(), time.time()))  # need to have different random seeds...
                    self._run_task(task_id)
                    os._exit(0)
            else:
                self._run_task(task_id)

            self._previous_tasks.append(task_id)

        while children:
            died_pid, status = os.wait()
            if died_pid in children:
                children.remove(died_pid)
            else:
                logger.warning("Some random process %s died" % died_pid)
