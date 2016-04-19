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
The worker communicates with the scheduler and does two things:

1. Sends all tasks that has to be run
2. Gets tasks from the scheduler that should be run

When running in local mode, the worker talks directly to a :py:class:`~luigi.scheduler.CentralPlannerScheduler` instance.
When you run a central server, the worker will talk to the scheduler using a :py:class:`~luigi.rpc.RemoteScheduler` instance.

Everything in this module is private to luigi and may change in incompatible
ways between versions. The exception is the exception types and the
:py:class:`worker` config class.
"""

import collections
import getpass
import logging
import multiprocessing  # Note: this seems to have some stability issues: https://github.com/spotify/luigi/pull/438
import os
import signal
import subprocess
import sys

try:
    import Queue
except ImportError:
    import queue as Queue
import random
import socket
import threading
import time
import traceback
import types
import warnings

from luigi import six

from luigi import notifications
from luigi.event import Event
from luigi.task_register import load_task
from luigi.scheduler import DISABLED, DONE, FAILED, PENDING, UNKNOWN, CentralPlannerScheduler
from luigi.target import Target
from luigi.task import Task, flatten, getpaths, Config
from luigi.task_register import TaskClassException
from luigi.task_status import RUNNING
from luigi.parameter import FloatParameter, IntParameter, BoolParameter

try:
    import simplejson as json
except ImportError:
    import json

logger = logging.getLogger('luigi-interface')

# Prevent fork() from being called during a C-level getaddrinfo() which uses a process-global mutex,
# that may not be unlocked in child process, resulting in the process being locked indefinitely.
fork_lock = threading.Lock()

# Why we assert on _WAIT_INTERVAL_EPS:
# multiprocessing.Queue.get() is undefined for timeout=0 it seems:
# https://docs.python.org/3.4/library/multiprocessing.html#multiprocessing.Queue.get.
# I also tried with really low epsilon, but then ran into the same issue where
# the test case "test_external_dependency_worker_is_patient" got stuck. So I
# unscientifically just set the final value to a floating point number that
# "worked for me".
_WAIT_INTERVAL_EPS = 0.00001


def _is_external(task):
    return task.run is None or task.run == NotImplemented


class TaskException(Exception):
    pass


class TaskProcess(multiprocessing.Process):

    """ Wrap all task execution in this class.

    Mainly for convenience since this is run in a separate process. """

    def __init__(self, task, worker_id, result_queue, tracking_url_callback,
                 status_message_callback, random_seed=False, worker_timeout=0):
        super(TaskProcess, self).__init__()
        self.task = task
        self.worker_id = worker_id
        self.result_queue = result_queue
        self.tracking_url_callback = tracking_url_callback
        self.status_message_callback = status_message_callback
        self.random_seed = random_seed
        if task.worker_timeout is not None:
            worker_timeout = task.worker_timeout
        self.timeout_time = time.time() + worker_timeout if worker_timeout else None

    def _run_get_new_deps(self):
        self.task.set_tracking_url = self.tracking_url_callback
        self.task.set_status_message = self.status_message_callback

        def deprecated_tracking_url_callback(*args, **kwargs):
            warnings.warn("tracking_url_callback in run() args is deprecated, use "
                          "set_tracking_url instead.", DeprecationWarning)
            self.tracking_url_callback(*args, **kwargs)

        run_again = False
        try:
            task_gen = self.task.run(tracking_url_callback=deprecated_tracking_url_callback)
        except TypeError as ex:
            if 'unexpected keyword argument' not in str(ex):
                raise
            run_again = True
        if run_again:
            task_gen = self.task.run()

        self.task.set_tracking_url = None
        self.task.set_status_message = None

        if not isinstance(task_gen, types.GeneratorType):
            return None

        next_send = None
        while True:
            try:
                if next_send is None:
                    requires = six.next(task_gen)
                else:
                    requires = task_gen.send(next_send)
            except StopIteration:
                return None

            new_req = flatten(requires)
            new_deps = [(t.task_module, t.task_family, t.to_str_params())
                        for t in new_req]
            if all(t.complete() for t in new_req):
                next_send = getpaths(requires)
            else:
                return new_deps

    def run(self):
        logger.info('[pid %s] Worker %s running   %s', os.getpid(), self.worker_id, self.task)

        if self.random_seed:
            # Need to have different random seeds if running in separate processes
            random.seed((os.getpid(), time.time()))

        status = FAILED
        expl = ''
        missing = []
        new_deps = []
        try:
            # Verify that all the tasks are fulfilled! For external tasks we
            # don't care about unfulfilled dependencies, because we are just
            # checking completeness of self.task so outputs of dependencies are
            # irrelevant.
            if not _is_external(self.task):
                missing = [dep.task_id for dep in self.task.deps() if not dep.complete()]
                if missing:
                    deps = 'dependency' if len(missing) == 1 else 'dependencies'
                    raise RuntimeError('Unfulfilled %s at run time: %s' % (deps, ', '.join(missing)))
            self.task.trigger_event(Event.START, self.task)
            t0 = time.time()
            status = None

            if _is_external(self.task):
                # External task
                # TODO(erikbern): We should check for task completeness after non-external tasks too!
                # This will resolve #814 and make things a lot more consistent
                if self.task.complete():
                    status = DONE
                else:
                    status = FAILED
                    expl = 'Task is an external data dependency ' \
                        'and data does not exist (yet?).'
            else:
                new_deps = self._run_get_new_deps()
                status = DONE if not new_deps else PENDING

            if new_deps:
                logger.info(
                    '[pid %s] Worker %s new requirements      %s',
                    os.getpid(), self.worker_id, self.task)
            elif status == DONE:
                self.task.trigger_event(
                    Event.PROCESSING_TIME, self.task, time.time() - t0)
                expl = self.task.on_success()
                logger.info('[pid %s] Worker %s done      %s', os.getpid(),
                            self.worker_id, self.task)
                self.task.trigger_event(Event.SUCCESS, self.task)

        except KeyboardInterrupt:
            raise
        except BaseException as ex:
            status = FAILED
            logger.exception("[pid %s] Worker %s failed    %s", os.getpid(), self.worker_id, self.task)
            self.task.trigger_event(Event.FAILURE, self.task, ex)
            raw_error_message = self.task.on_failure(ex)
            expl = raw_error_message

        finally:
            self.result_queue.put(
                (self.task.task_id, status, expl, missing, new_deps))

    def _recursive_terminate(self):
        import psutil

        try:
            parent = psutil.Process(self.pid)
            children = parent.children(recursive=True)

            # terminate parent. Give it a chance to clean up
            super(TaskProcess, self).terminate()
            parent.wait()

            # terminate children
            for child in children:
                try:
                    child.terminate()
                except psutil.NoSuchProcess:
                    continue
        except psutil.NoSuchProcess:
            return

    def terminate(self):
        """Terminate this process and its subprocesses."""
        # default terminate() doesn't cleanup child processes, it orphans them.
        try:
            return self._recursive_terminate()
        except ImportError:
            return super(TaskProcess, self).terminate()


class SingleProcessPool(object):
    """
    Dummy process pool for using a single processor.

    Imitates the api of multiprocessing.Pool using single-processor equivalents.
    """

    def apply_async(self, function, args):
        return function(*args)

    def close(self):
        pass

    def join(self):
        pass


class DequeQueue(collections.deque):
    """
    deque wrapper implementing the Queue interface.
    """

    def put(self, obj, block=None, timeout=None):
        return self.append(obj)

    def get(self, block=None, timeout=None):
        try:
            return self.pop()
        except IndexError:
            raise Queue.Empty


class AsyncCompletionException(Exception):
    """
    Exception indicating that something went wrong with checking complete.
    """

    def __init__(self, trace):
        self.trace = trace


class TracebackWrapper(object):
    """
    Class to wrap tracebacks so we can know they're not just strings.
    """

    def __init__(self, trace):
        self.trace = trace


def check_complete(task, out_queue):
    """
    Checks if task is complete, puts the result to out_queue.
    """
    logger.debug("Checking if %s is complete", task)
    try:
        is_complete = task.complete()
    except Exception:
        is_complete = TracebackWrapper(traceback.format_exc())
    out_queue.put((task, is_complete))


class worker(Config):
    # NOTE: `section.config-variable` in the config_path argument is deprecated in favor of `worker.config_variable`

    ping_interval = FloatParameter(default=1.0,
                                   config_path=dict(section='core', name='worker-ping-interval'))
    keep_alive = BoolParameter(default=False,
                               config_path=dict(section='core', name='worker-keep-alive'))
    count_uniques = BoolParameter(default=False,
                                  config_path=dict(section='core', name='worker-count-uniques'),
                                  description='worker-count-uniques means that we will keep a '
                                  'worker alive only if it has a unique pending task, as '
                                  'well as having keep-alive true')
    wait_interval = FloatParameter(default=1.0,
                                   config_path=dict(section='core', name='worker-wait-interval'))
    wait_jitter = FloatParameter(default=5.0)

    max_reschedules = IntParameter(default=1,
                                   config_path=dict(section='core', name='worker-max-reschedules'))
    timeout = IntParameter(default=0,
                           config_path=dict(section='core', name='worker-timeout'))
    task_limit = IntParameter(default=None,
                              config_path=dict(section='core', name='worker-task-limit'))
    retry_external_tasks = BoolParameter(default=False,
                                         config_path=dict(section='core', name='retry-external-tasks'),
                                         description='If true, incomplete external tasks will be '
                                         'retested for completion while Luigi is running.')
    no_install_shutdown_handler = BoolParameter(default=False,
                                                description='If true, the SIGUSR1 shutdown handler will'
                                                'NOT be install on the worker')


class KeepAliveThread(threading.Thread):
    """
    Periodically tell the scheduler that the worker still lives.
    """

    def __init__(self, scheduler, worker_id, ping_interval):
        super(KeepAliveThread, self).__init__()
        self._should_stop = threading.Event()
        self._scheduler = scheduler
        self._worker_id = worker_id
        self._ping_interval = ping_interval

    def stop(self):
        self._should_stop.set()

    def run(self):
        while True:
            self._should_stop.wait(self._ping_interval)
            if self._should_stop.is_set():
                logger.info("Worker %s was stopped. Shutting down Keep-Alive thread" % self._worker_id)
                break
            with fork_lock:
                try:
                    self._scheduler.ping(worker=self._worker_id)
                except:  # httplib.BadStatusLine:
                    logger.warning('Failed pinging scheduler')


class Worker(object):
    """
    Worker object communicates with a scheduler.

    Simple class that talks to a scheduler and:

    * tells the scheduler what it has to do + its dependencies
    * asks for stuff to do (pulls it in a loop and runs it)
    """

    def __init__(self, scheduler=None, worker_id=None, worker_processes=1, assistant=False, **kwargs):
        if scheduler is None:
            scheduler = CentralPlannerScheduler()

        self.worker_processes = int(worker_processes)
        self._worker_info = self._generate_worker_info()

        if not worker_id:
            worker_id = 'Worker(%s)' % ', '.join(['%s=%s' % (k, v) for k, v in self._worker_info])

        self._config = worker(**kwargs)

        assert self._config.wait_interval >= _WAIT_INTERVAL_EPS, "[worker] wait_interval must be positive"
        assert self._config.wait_jitter >= 0.0, "[worker] wait_jitter must be equal or greater than zero"

        self._id = worker_id
        self._scheduler = scheduler
        self._assistant = assistant
        self._stop_requesting_work = False

        self.host = socket.gethostname()
        self._scheduled_tasks = {}
        self._suspended_tasks = {}

        self._first_task = None

        self.add_succeeded = True
        self.run_succeeded = True
        self.unfulfilled_counts = collections.defaultdict(int)

        # note that ``signal.signal(signal.SIGUSR1, fn)`` only works inside the main execution thread, which is why we
        # provide the ability to conditionally install the hook.
        if not self._config.no_install_shutdown_handler:
            try:
                signal.signal(signal.SIGUSR1, self.handle_interrupt)
            except AttributeError:
                pass

        # Keep info about what tasks are running (could be in other processes)
        if worker_processes == 1:
            self._task_result_queue = DequeQueue()
        else:
            self._task_result_queue = multiprocessing.Queue()

        self._running_tasks = {}

        # Stuff for execution_summary
        self._add_task_history = []
        self._get_work_response_history = []

    def _add_task(self, *args, **kwargs):
        """
        Call ``self._scheduler.add_task``, but store the values too so we can
        implement :py:func:`luigi.execution_summary.summary`.
        """
        task_id = kwargs['task_id']
        status = kwargs['status']
        runnable = kwargs['runnable']
        task = self._scheduled_tasks.get(task_id)
        if task:
            msg = (task, status, runnable)
            self._add_task_history.append(msg)
        self._scheduler.add_task(*args, **kwargs)

        logger.info('Informed scheduler that task   %s   has status   %s', task_id, status)

    def __enter__(self):
        """
        Start the KeepAliveThread.
        """
        self._keep_alive_thread = KeepAliveThread(self._scheduler, self._id, self._config.ping_interval)
        self._keep_alive_thread.daemon = True
        self._keep_alive_thread.start()
        return self

    def __exit__(self, type, value, traceback):
        """
        Stop the KeepAliveThread and kill still running tasks.
        """
        self._keep_alive_thread.stop()
        self._keep_alive_thread.join()
        for task in self._running_tasks.values():
            if task.is_alive():
                task.terminate()
        return False  # Don't suppress exception

    def _generate_worker_info(self):
        # Generate as much info as possible about the worker
        # Some of these calls might not be available on all OS's
        args = [('salt', '%09d' % random.randrange(0, 999999999)),
                ('workers', self.worker_processes)]
        try:
            args += [('host', socket.gethostname())]
        except BaseException:
            pass
        try:
            args += [('username', getpass.getuser())]
        except BaseException:
            pass
        try:
            args += [('pid', os.getpid())]
        except BaseException:
            pass
        try:
            sudo_user = os.getenv("SUDO_USER")
            if sudo_user:
                args.append(('sudo_user', sudo_user))
        except BaseException:
            pass
        return args

    def _validate_task(self, task):
        if not isinstance(task, Task):
            raise TaskException('Can not schedule non-task %s' % task)

        if not task.initialized():
            # we can't get the repr of it since it's not initialized...
            raise TaskException('Task of class %s not initialized. Did you override __init__ and forget to call super(...).__init__?' % task.__class__.__name__)

    def _log_complete_error(self, task, tb):
        log_msg = "Will not run {task} or any dependencies due to error in complete() method:\n{tb}".format(task=task, tb=tb)
        logger.warning(log_msg)

    def _log_dependency_error(self, task, tb):
        log_msg = "Will not run {task} or any dependencies due to error in deps() method:\n{tb}".format(task=task, tb=tb)
        logger.warning(log_msg)

    def _log_unexpected_error(self, task):
        logger.exception("Luigi unexpected framework error while scheduling %s", task)  # needs to be called from within except clause

    def _email_complete_error(self, task, formatted_traceback):
        self._email_error(task, formatted_traceback,
                          subject="Luigi: {task} failed scheduling. Host: {host}",
                          headline="Will not run {task} or any dependencies due to error in complete() method",
                          )

    def _email_dependency_error(self, task, formatted_traceback):
        self._email_error(task, formatted_traceback,
                          subject="Luigi: {task} failed scheduling. Host: {host}",
                          headline="Will not run {task} or any dependencies due to error in deps() method",
                          )

    def _email_unexpected_error(self, task, formatted_traceback):
        self._email_error(task, formatted_traceback,
                          subject="Luigi: Framework error while scheduling {task}. Host: {host}",
                          headline="Luigi framework error",
                          )

    def _email_task_failure(self, task, formatted_traceback):
        self._email_error(task, formatted_traceback,
                          subject="Luigi: {task} FAILED. Host: {host}",
                          headline="A task failed when running. Most likely run() raised an exception.",
                          )

    def _email_error(self, task, formatted_traceback, subject, headline):
        formatted_subject = subject.format(task=task, host=self.host)
        command = subprocess.list2cmdline(sys.argv)
        message = notifications.format_task_error(headline, task, command, formatted_traceback)
        notifications.send_error_email(formatted_subject, message, task.owner_email)

    def add(self, task, multiprocess=False):
        """
        Add a Task for the worker to check and possibly schedule and run.

        Returns True if task and its dependencies were successfully scheduled or completed before.
        """
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
            raise
        finally:
            pool.close()
            pool.join()
        return self.add_succeeded

    def _add(self, task, is_complete):
        if self._config.task_limit is not None and len(self._scheduled_tasks) >= self._config.task_limit:
            logger.warning('Will not schedule %s or any dependencies due to exceeded task-limit of %d', task, self._config.task_limit)
            return

        formatted_traceback = None
        try:
            self._check_complete_value(is_complete)
        except KeyboardInterrupt:
            raise
        except AsyncCompletionException as ex:
            formatted_traceback = ex.trace
        except BaseException:
            formatted_traceback = traceback.format_exc()

        if formatted_traceback is not None:
            self.add_succeeded = False
            self._log_complete_error(task, formatted_traceback)
            task.trigger_event(Event.DEPENDENCY_MISSING, task)
            self._email_complete_error(task, formatted_traceback)
            deps = None
            status = UNKNOWN
            runnable = False

        elif is_complete:
            deps = None
            status = DONE
            runnable = False

            task.trigger_event(Event.DEPENDENCY_PRESENT, task)
        elif _is_external(task):
            deps = None
            status = PENDING
            runnable = worker().retry_external_tasks

            task.trigger_event(Event.DEPENDENCY_MISSING, task)
            logger.warning('Data for %s does not exist (yet?). The task is an '
                           'external data depedency, so it can not be run from'
                           ' this luigi process.', task)

        else:
            try:
                deps = task.deps()
            except Exception as ex:
                formatted_traceback = traceback.format_exc()
                self.add_succeeded = False
                self._log_dependency_error(task, formatted_traceback)
                task.trigger_event(Event.BROKEN_TASK, task, ex)
                self._email_dependency_error(task, formatted_traceback)
                deps = None
                status = UNKNOWN
                runnable = False
            else:
                status = PENDING
                runnable = True

        if task.disabled:
            status = DISABLED

        if deps:
            for d in deps:
                self._validate_dependency(d)
                task.trigger_event(Event.DEPENDENCY_DISCOVERED, task, d)
                yield d  # return additional tasks to add

            deps = [d.task_id for d in deps]

        self._scheduled_tasks[task.task_id] = task
        self._add_task(worker=self._id, task_id=task.task_id, status=status,
                       deps=deps, runnable=runnable, priority=task.priority,
                       resources=task.process_resources(),
                       params=task.to_str_params(),
                       family=task.task_family,
                       module=task.task_module)

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
        self._scheduler.add_worker(self._id, self._worker_info)

    def _log_remote_tasks(self, running_tasks, n_pending_tasks, n_unique_pending):
        logger.debug("Done")
        logger.debug("There are no more tasks to run at this time")
        if running_tasks:
            for r in running_tasks:
                logger.debug('%s is currently run by worker %s', r['task_id'], r['worker'])
        elif n_pending_tasks:
            logger.debug("There are %s pending tasks possibly being run by other workers", n_pending_tasks)
            if n_unique_pending:
                logger.debug("There are %i pending tasks unique to this worker", n_unique_pending)

    def _get_work(self):
        if self._stop_requesting_work:
            return None, 0, 0, 0
        logger.debug("Asking scheduler for work...")
        r = self._scheduler.get_work(
            worker=self._id,
            host=self.host,
            assistant=self._assistant,
            current_tasks=list(self._running_tasks.keys()),
        )
        n_pending_tasks = r['n_pending_tasks']
        task_id = r['task_id']
        running_tasks = r['running_tasks']
        n_unique_pending = r['n_unique_pending']

        self._get_work_response_history.append(dict(
            task_id=task_id,
            running_tasks=running_tasks,
        ))

        if task_id is not None and task_id not in self._scheduled_tasks:
            logger.info('Did not schedule %s, will load it dynamically', task_id)

            try:
                # TODO: we should obtain the module name from the server!
                self._scheduled_tasks[task_id] = \
                    load_task(module=r.get('task_module'),
                              task_name=r['task_family'],
                              params_str=r['task_params'])
            except TaskClassException as ex:
                msg = 'Cannot find task for %s' % task_id
                logger.exception(msg)
                subject = 'Luigi: %s' % msg
                error_message = notifications.wrap_traceback(ex)
                notifications.send_error_email(subject, error_message)
                self._add_task(worker=self._id, task_id=task_id, status=FAILED, runnable=False,
                               assistant=self._assistant)
                task_id = None
                self.run_succeeded = False

        return task_id, running_tasks, n_pending_tasks, n_unique_pending

    def _run_task(self, task_id):
        task = self._scheduled_tasks[task_id]

        p = self._create_task_process(task)

        self._running_tasks[task_id] = p

        if self.worker_processes > 1:
            with fork_lock:
                p.start()
        else:
            # Run in the same process
            p.run()

    def _create_task_process(self, task):
        def update_tracking_url(tracking_url):
            self._scheduler.add_task(
                task_id=task.task_id,
                worker=self._id,
                status=RUNNING,
                tracking_url=tracking_url,
            )

        def update_status_message(message):
            self._scheduler.set_task_status_message(task.task_id, message)

        return TaskProcess(
            task, self._id, self._task_result_queue, update_tracking_url, update_status_message,
            random_seed=bool(self.worker_processes > 1),
            worker_timeout=self._config.timeout
        )

    def _purge_children(self):
        """
        Find dead children and put a response on the result queue.

        :return:
        """
        for task_id, p in six.iteritems(self._running_tasks):
            if not p.is_alive() and p.exitcode:
                error_msg = 'Task %s died unexpectedly with exit code %s' % (task_id, p.exitcode)
            elif p.timeout_time is not None and time.time() > float(p.timeout_time) and p.is_alive():
                p.terminate()
                error_msg = 'Task %s timed out and was terminated.' % task_id
            else:
                continue

            logger.info(error_msg)
            self._task_result_queue.put((task_id, FAILED, error_msg, [], []))

    def _handle_next_task(self):
        """
        We have to catch three ways a task can be "done":

        1. normal execution: the task runs/fails and puts a result back on the queue,
        2. new dependencies: the task yielded new deps that were not complete and
           will be rescheduled and dependencies added,
        3. child process dies: we need to catch this separately.
        """
        while True:
            self._purge_children()  # Deal with subprocess failures

            try:
                task_id, status, expl, missing, new_requirements = (
                    self._task_result_queue.get(
                        timeout=self._config.wait_interval))
            except Queue.Empty:
                return

            task = self._scheduled_tasks[task_id]
            if not task or task_id not in self._running_tasks:
                continue
                # Not a running task. Probably already removed.
                # Maybe it yielded something?

            # external task if run not implemented, retry-able if config option is enabled.
            external_task_retryable = _is_external(task) and self._config.retry_external_tasks
            if status == FAILED and not external_task_retryable:
                self._email_task_failure(task, expl)

            new_deps = []
            if new_requirements:
                new_req = [load_task(module, name, params)
                           for module, name, params in new_requirements]
                for t in new_req:
                    self.add(t)
                new_deps = [t.task_id for t in new_req]

            self._add_task(worker=self._id,
                           task_id=task_id,
                           status=status,
                           expl=json.dumps(expl),
                           resources=task.process_resources(),
                           runnable=None,
                           params=task.to_str_params(),
                           family=task.task_family,
                           module=task.task_module,
                           new_deps=new_deps,
                           assistant=self._assistant)

            self._running_tasks.pop(task_id)

            # re-add task to reschedule missing dependencies
            if missing:
                reschedule = True

                # keep out of infinite loops by not rescheduling too many times
                for task_id in missing:
                    self.unfulfilled_counts[task_id] += 1
                    if (self.unfulfilled_counts[task_id] >
                            self._config.max_reschedules):
                        reschedule = False
                if reschedule:
                    self.add(task)

            self.run_succeeded &= (status == DONE) or (len(new_deps) > 0)
            return

    def _sleeper(self):
        # TODO is exponential backoff necessary?
        while True:
            jitter = self._config.wait_jitter
            wait_interval = self._config.wait_interval + random.uniform(0, jitter)
            logger.debug('Sleeping for %f seconds', wait_interval)
            time.sleep(wait_interval)
            yield

    def _keep_alive(self, n_pending_tasks, n_unique_pending):
        """
        Returns true if a worker should stay alive given.

        If worker-keep-alive is not set, this will always return false.
        For an assistant, it will always return the value of worker-keep-alive.
        Otherwise, it will return true for nonzero n_pending_tasks.

        If worker-count-uniques is true, it will also
        require that one of the tasks is unique to this worker.
        """
        if not self._config.keep_alive:
            return False
        elif self._assistant:
            return True
        else:
            return n_pending_tasks and (n_unique_pending or not self._config.count_uniques)

    def handle_interrupt(self, signum, _):
        """
        Stops the assistant from asking for more work on SIGUSR1
        """
        if signum == signal.SIGUSR1:
            self._config.keep_alive = False
            self._stop_requesting_work = True

    def run(self):
        """
        Returns True if all scheduled tasks were executed successfully.
        """
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
                if not self._stop_requesting_work:
                    self._log_remote_tasks(running_tasks, n_pending_tasks, n_unique_pending)
                if len(self._running_tasks) == 0:
                    if self._keep_alive(n_pending_tasks, n_unique_pending):
                        six.next(sleeper)
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
