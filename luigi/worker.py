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
from scheduler import (CentralPlannerScheduler, PENDING, RUNNING, FAILED,
                       SUSPENDED, DONE, DISABLED)
import collections
import threading
import time
import os
import socket
import configuration
import traceback
import logging
import notifications
import getpass
import multiprocessing # Note: this seems to have some stability issues: https://github.com/spotify/luigi/pull/438
import Queue
import types
import interface
from target import Target
from task import Task, flatten, getpaths
from event import Event

try:
    import simplejson as json
except ImportError:
    import json

logger = logging.getLogger('luigi-interface')

# Prevent fork() from being called during a C-level getaddrinfo() which uses a process-global mutex,
# that may not be unlocked in child process, resulting in the process being locked indefinitely.
fork_lock = threading.Lock()


class TaskException(Exception):
    pass


class TaskProcess(multiprocessing.Process):
    ''' Wrap all task execution in this class.

    Mainly for convenience since this is run in a separate process. '''
    def __init__(self, task, worker_id, result_queue, random_seed=False, worker_timeout=0):
        super(TaskProcess, self).__init__()
        self.task = task
        self.worker_id = worker_id
        self.result_queue = result_queue
        self.random_seed = random_seed
        if task.worker_timeout is not None:
            worker_timeout = task.worker_timeout
        self.timeout_time = time.time() + worker_timeout if worker_timeout else None

    def run(self):
        logger.info('[pid %s] Worker %s running   %s', os.getpid(), self.worker_id, self.task.task_id)

        if self.random_seed:
            # Need to have different random seeds if running in separate processes
            random.seed((os.getpid(), time.time()))

        status = FAILED
        error_message = ''
        missing = []
        new_deps = []
        try:
            # Verify that all the tasks are fulfilled!
            missing = [dep.task_id for dep in self.task.deps() if not dep.complete()]
            if missing:
                deps = 'dependency' if len(missing) == 1 else 'dependencies'
                raise RuntimeError('Unfulfilled %s at run time: %s' % (deps, ', '.join(missing)))
            self.task.trigger_event(Event.START, self.task)
            t0 = time.time()
            status = None
            try:
                task_gen = self.task.run()
                if isinstance(task_gen, types.GeneratorType):  # new deps
                    next_send = None
                    while True:
                        try:
                            if next_send is None:
                                requires = task_gen.next()
                            else:
                                requires = task_gen.send(next_send)
                        except StopIteration:
                            break

                        new_req = flatten(requires)
                        status = (RUNNING if all(t.complete() for t in new_req)
                                  else SUSPENDED)
                        new_deps = [(t.task_module, t.task_family, t.to_str_params())
                                    for t in new_req]
                        if status == RUNNING:
                            self.result_queue.put(
                                (self.task.task_id, status, '', missing,
                                 new_deps))
                            next_send = getpaths(requires)
                        else:
                            logger.info(
                                '[pid %s] Worker %s new requirements      %s',
                                os.getpid(), self.worker_id, self.task.task_id)
                            return
            finally:
                if status != SUSPENDED:
                    self.task.trigger_event(
                        Event.PROCESSING_TIME, self.task, time.time() - t0)
            error_message = json.dumps(self.task.on_success())
            logger.info('[pid %s] Worker %s done      %s', os.getpid(),
                        self.worker_id, self.task.task_id)
            self.task.trigger_event(Event.SUCCESS, self.task)
            status = DONE

        except KeyboardInterrupt:
            raise
        except BaseException as ex:
            status = FAILED
            logger.exception("[pid %s] Worker %s failed    %s", os.getpid(), self.worker_id, self.task)
            error_message = notifications.wrap_traceback(self.task.on_failure(ex))
            self.task.trigger_event(Event.FAILURE, self.task, ex)
            subject = "Luigi: %s FAILED" % self.task
            notifications.send_error_email(subject, error_message)
        finally:
            self.result_queue.put(
                (self.task.task_id, status, error_message, missing, new_deps))


class SingleProcessPool(object):
    """ Dummy process pool for using a single processor

    Imitates the api of multiprocessing.Pool using single-processor equivalents
    """

    def apply_async(self, function, args):
        return function(*args)


class DequeQueue(collections.deque):
    """ deque wrapper implementing the Queue interface """

    put = collections.deque.append
    get = collections.deque.pop


class AsyncCompletionException(Exception):
    """ Exception indicating that something went wrong with checking complete """
    def __init__(self, trace):
        self.trace = trace


class TracebackWrapper(object):
    """ Class to wrap tracebacks so we can know they're not just strings """
    def __init__(self, trace):
        self.trace = trace


def check_complete(task, out_queue):
    """ Checks if task is complete, puts the result to out_queue """
    logger.debug("Checking if %s is complete", task)
    try:
        is_complete = task.complete()
    except:
        is_complete = TracebackWrapper(traceback.format_exc())
    out_queue.put((task, is_complete))


class Worker(object):
    """ Worker object communicates with a scheduler.

    Simple class that talks to a scheduler and:
    - Tells the scheduler what it has to do + its dependencies
    - Asks for stuff to do (pulls it in a loop and runs it)
    """

    def __init__(self, scheduler=CentralPlannerScheduler(), worker_id=None,
                 worker_processes=1, ping_interval=None, keep_alive=None,
                 wait_interval=None, max_reschedules=None, count_uniques=None,
                 worker_timeout=None):
        self.worker_processes = int(worker_processes)
        self._worker_info = self._generate_worker_info()

        if not worker_id:
            worker_id = 'Worker(%s)' % ', '.join(['%s=%s' % (k, v) for k, v in self._worker_info])

        config = configuration.get_config()

        if ping_interval is None:
            ping_interval = config.getfloat('core', 'worker-ping-interval', 1.0)

        if keep_alive is None:
            keep_alive = config.getboolean('core', 'worker-keep-alive', False)
        self.__keep_alive = keep_alive

        # worker-count-uniques means that we will keep a worker alive only if it has a unique
        # pending task, as well as having keep-alive true
        if count_uniques is None:
            count_uniques = config.getboolean('core', 'worker-count-uniques', False)
        self.__count_uniques = count_uniques

        if wait_interval is None:
            wait_interval = config.getint('core', 'worker-wait-interval', 1)
        self.__wait_interval = wait_interval

        if max_reschedules is None:
            max_reschedules = config.getint('core', 'max-reschedules', 1)
        self.__max_reschedules = max_reschedules

        if worker_timeout is None:
            worker_timeout = configuration.get_config().getint('core', 'worker-timeout', 0)
        self.__worker_timeout = worker_timeout

        self._id = worker_id
        self._scheduler = scheduler

        self.host = socket.gethostname()
        self._scheduled_tasks = {}
        self._suspended_tasks = {}

        self._first_task = None

        self.add_succeeded = True
        self.run_succeeded = True
        self.unfulfilled_counts = collections.defaultdict(int)

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
                    fork_lock.acquire()
                    try:
                        scheduler.ping(worker=worker_id)
                    except:  # httplib.BadStatusLine:
                        logger.warning('Failed pinging scheduler')
                    finally:
                        fork_lock.release()

        self._keep_alive_thread = KeepAliveThread()
        self._keep_alive_thread.daemon = True
        self._keep_alive_thread.start()

        # Keep info about what tasks are running (could be in other processes)
        self._task_result_queue = multiprocessing.Queue()
        self._running_tasks = {}

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

    def _generate_worker_info(self):
        # Generate as much info as possible about the worker
        # Some of these calls might not be available on all OS's
        args = [('salt', '%09d' % random.randrange(0, 999999999)),
                ('workers', self.worker_processes)]
        try:
            args += [('host', socket.gethostname())]
        except:
            pass
        try:
            args += [('username', getpass.getuser())]
        except:
            pass
        try:
            args += [('pid', os.getpid())]
        except:
            pass
        try:
            sudo_user = os.getenv("SUDO_USER")
            if sudo_user:
                args.append(('sudo_user', sudo_user))
        except:
            pass
        return args

    def _validate_task(self, task):
        if not isinstance(task, Task):
            raise TaskException('Can not schedule non-task %s' % task)

        if not task.initialized():
            # we can't get the repr of it since it's not initialized...
            raise TaskException('Task of class %s not initialized. Did you override __init__ and forget to call super(...).__init__?' % task.__class__.__name__)

    def _log_complete_error(self, task, tb):
        log_msg = "Will not schedule {task} or any dependencies due to error in complete() method:\n{tb}".format(task=task, tb=tb)
        logger.warning(log_msg)

    def _log_unexpected_error(self, task):
        logger.exception("Luigi unexpected framework error while scheduling %s", task) # needs to be called from within except clause

    def _email_complete_error(self, task, formatted_traceback):
          # like logger.exception but with WARNING level
        formatted_traceback = notifications.wrap_traceback(formatted_traceback)
        subject = "Luigi: {task} failed scheduling".format(task=task)
        message = "Will not schedule {task} or any dependencies due to error in complete() method:\n{traceback}".format(task=task, traceback=formatted_traceback)
        notifications.send_error_email(subject, message)

    def _email_unexpected_error(self, task, formatted_traceback):
        formatted_traceback = notifications.wrap_traceback(formatted_traceback)
        subject = "Luigi: Framework error while scheduling {task}".format(task=task)
        message = "Luigi framework error:\n{traceback}".format(traceback=formatted_traceback)
        notifications.send_error_email(subject, message)

    def add(self, task, multiprocess=False):
        """ Add a Task for the worker to check and possibly schedule and run.
         Returns True if task and its dependencies were successfully scheduled or completed before"""
        if self._first_task is None and hasattr(task, 'task_id'):
            self._first_task = task.task_id
        self.add_succeeded = True
        if multiprocess:
            queue = multiprocessing.Manager().Queue()
            pool = multiprocessing.Pool()
        else:
            queue = DequeQueue()
            pool = SingleProcessPool()
        self._validate_task(task)
        pool.apply_async(check_complete, [task, queue])

        # we track queue size ourselves because len(queue) won't work for multiprocessing
        queue_size = 1
        try:
            seen = set([task.task_id])
            while queue_size:
                current = queue.get()
                queue_size -= 1
                item, is_complete = current
                for next in self._add(item, is_complete):
                    if next.task_id not in seen:
                        self._validate_task(next)
                        seen.add(next.task_id)
                        pool.apply_async(check_complete, [next, queue])
                        queue_size += 1
        except (KeyboardInterrupt, TaskException):
            raise
        except Exception as ex:
            self.add_succeeded = False
            formatted_traceback = traceback.format_exc()
            self._log_unexpected_error(task)
            task.trigger_event(Event.BROKEN_TASK, task, ex)
            self._email_unexpected_error(task, formatted_traceback)
        return self.add_succeeded

    def _add(self, task, is_complete):
        formatted_traceback = None
        try:
            self._check_complete_value(is_complete)
        except KeyboardInterrupt:
            raise
        except AsyncCompletionException as ex:
            formatted_traceback = ex.trace
        except:
            formatted_traceback = traceback.format_exc()

        if formatted_traceback is not None:
            self.add_succeeded = False
            self._log_complete_error(task, formatted_traceback)
            task.trigger_event(Event.DEPENDENCY_MISSING, task)
            self._email_complete_error(task, formatted_traceback)
            # abort, i.e. don't schedule any subtasks of a task with
            # failing complete()-method since we don't know if the task
            # is complete and subtasks might not be desirable to run if
            # they have already ran before
            return

        if is_complete:
            deps = None
            status = DONE
            runnable = False

            task.trigger_event(Event.DEPENDENCY_PRESENT, task)
        elif task.run == NotImplemented:
            deps = None
            status = PENDING
            runnable = False

            task.trigger_event(Event.DEPENDENCY_MISSING, task)
            logger.warning('Task %s is not complete and run() is not implemented. Probably a missing external dependency.', task.task_id)

        else:
            deps = task.deps()
            status = PENDING
            runnable = True

        if task.disabled:
            status = DISABLED

        if deps:
            for d in deps:
                self._validate_dependency(d)
                task.trigger_event(Event.DEPENDENCY_DISCOVERED, task, d)
                yield d # return additional tasks to add

            deps = [d.task_id for d in deps]

        self._scheduled_tasks[task.task_id] = task
        self._scheduler.add_task(self._id, task.task_id, status=status,
                                 deps=deps, runnable=runnable, priority=task.priority,
                                 resources=task.process_resources(),
                                 params=task.to_str_params(),
                                 family=task.task_family)

        logger.info('Scheduled %s (%s)', task.task_id, status)

    def _validate_dependency(self, dependency):
        if isinstance(dependency, Target):
            raise Exception('requires() can not return Target objects. Wrap it in an ExternalTask class')
        elif not isinstance(dependency, Task):
            raise Exception('requires() must return Task objects')

    def _check_complete_value(self, is_complete):
        if is_complete not in (True, False):
            if isinstance(is_complete, TracebackWrapper):
                raise AsyncCompletionException(is_complete.trace)
            raise Exception("Return value of Task.complete() must be boolean (was %r)" % is_complete)

    def _add_worker(self):
        self._worker_info.append(('first_task', self._first_task))
        try:
            self._scheduler.add_worker(self._id, self._worker_info)
        except:
            logger.exception('Exception adding worker - scheduler might be running an older version')

    def _log_remote_tasks(self, running_tasks, n_pending_tasks, n_unique_pending):
        logger.info("Done")
        logger.info("There are no more tasks to run at this time")
        if running_tasks:
            for r in running_tasks:
                logger.info('%s is currently run by worker %s', r['task_id'], r['worker'])
        elif n_pending_tasks:
            logger.info("There are %s pending tasks possibly being run by other workers", n_pending_tasks)
            if n_unique_pending:
                logger.info("There are %i pending tasks unique to this worker", n_unique_pending)

    def _get_work(self):
        logger.debug("Asking scheduler for work...")
        r = self._scheduler.get_work(worker=self._id, host=self.host)
        # Support old version of scheduler
        if isinstance(r, tuple) or isinstance(r, list):
            n_pending_tasks, task_id = r
            running_tasks = []
            n_unique_pending = 0
        else:
            n_pending_tasks = r['n_pending_tasks']
            task_id = r['task_id']
            running_tasks = r['running_tasks']
            # support old version of scheduler
            n_unique_pending = r.get('n_unique_pending', 0)
        return task_id, running_tasks, n_pending_tasks, n_unique_pending

    def _run_task(self, task_id):
        task = self._scheduled_tasks[task_id]
        p = TaskProcess(task, self._id, self._task_result_queue,
                        random_seed=bool(self.worker_processes > 1),
                        worker_timeout=self.__worker_timeout)
        self._running_tasks[task_id] = p

        if self.worker_processes > 1:
            fork_lock.acquire()
            try:
                p.start()
            finally:
                fork_lock.release()
        else:
            # Run in the same process
            p.run()

    def _purge_children(self):
        ''' Find dead children and put a response on the result queue '''
        for task_id, p in self._running_tasks.iteritems():
            if not p.is_alive() and p.exitcode:
                error_msg = 'Worker task %s died unexpectedly with exit code %s' % (task_id, p.exitcode)
            elif p.timeout_time is not None and time.time() > p.timeout_time and p.is_alive():
                p.terminate()
                error_msg = 'Worker task %s timed out and was terminated.' % task_id
            else:
                continue

            logger.info(error_msg)
            self._task_result_queue.put((task_id, FAILED, error_msg, [], []))


    def _handle_next_task(self):
        ''' We have to catch three ways a task can be "done"
        1. Normal execution: the task runs/fails and puts a result back on the
           queue
        2. New dependencies: the task yielded new deps that were not complete
           and will be rescheduled and dependencies added.
        3. Child process dies: we need to catch this separately
        '''
        while True:
            self._purge_children()  # Deal with subprocess failures

            try:
                task_id, status, error_message, missing, new_requirements = (
                    self._task_result_queue.get(
                        timeout=float(self.__wait_interval)))
            except Queue.Empty:
                return

            task = self._scheduled_tasks[task_id]
            if not task or task_id not in self._running_tasks:
                continue
                # Not a running task. Probably already removed.
                # Maybe it yielded something?
            new_deps = []
            if new_requirements:
                new_req = [interface.load_task(module, name, params)
                           for module, name, params in new_requirements]
                for t in new_req:
                    self.add(t)
                new_deps = [t.task_id for t in new_req]

            self._scheduler.add_task(self._id,
                                     task_id,
                                     status=status,
                                     expl=error_message,
                                     resources=task.process_resources(),
                                     runnable=None,
                                     params=task.to_str_params(),
                                     family=task.task_family,
                                     new_deps=new_deps)

            if status == RUNNING:
                continue
            self._running_tasks.pop(task_id)

            # re-add task to reschedule missing dependencies
            if missing:
                reschedule = True

                # keep out of infinite loops by not rescheduling too many times
                for task_id in missing:
                    self.unfulfilled_counts[task_id] += 1
                    if (self.unfulfilled_counts[task_id] >
                            self.__max_reschedules):
                        reschedule = False
                if reschedule:
                    self.add(task)

            self.run_succeeded &= status in (DONE, SUSPENDED)
            return

    def _sleeper(self):
        # TODO is exponential backoff necessary?
        while True:
            wait_interval = self.__wait_interval + random.randint(1, 5)
            logger.debug('Sleeping for %d seconds', wait_interval)
            time.sleep(wait_interval)
            yield

    def _keep_alive(self, n_pending_tasks, n_unique_pending):
        """ Returns true if a worker should stay alive given

        If worker-keep-alive is not set, this will always return false. Otherwise, it will return
        true for nonzero n_pending_tasks. If worker-count-uniques is true, it will also
        require that one of the tasks is unique to this worker.
        """
        return (self.__keep_alive and n_pending_tasks
                and (n_unique_pending or not self.__count_uniques))

    def run(self):
        """Returns True if all scheduled tasks were executed successfully"""
        logger.info('Running Worker with %d processes', self.worker_processes)

        sleeper = self._sleeper()
        self.run_succeeded = True

        self._add_worker()

        while True:
            while len(self._running_tasks) >= self.worker_processes:
                logger.debug('%d running tasks, waiting for next task to finish', len(self._running_tasks))
                self._handle_next_task()

            task_id, running_tasks, n_pending_tasks, n_unique_pending = self._get_work()

            if task_id is None:
                self._log_remote_tasks(running_tasks, n_pending_tasks, n_unique_pending)
                if len(self._running_tasks) == 0:
                    if self._keep_alive(n_pending_tasks, n_unique_pending):
                        sleeper.next()
                        continue
                    else:
                        break
                else:
                    self._handle_next_task()
                    continue

            # task_id is not None:
            logger.debug("Pending tasks: %s", n_pending_tasks)
            self._run_task(task_id)

        while len(self._running_tasks):
            logger.debug('Shut down Worker, %d more tasks to go', len(self._running_tasks))
            self._handle_next_task()

        return self.run_succeeded
