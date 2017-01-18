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
This module contains the bindings for command line integration and dynamic loading of tasks

If you don't want to run luigi from the command line. You may use the methods
defined in this module to programatically run luigi.
"""

import logging
import logging.config
import os
import sys
import tempfile
import signal
import warnings

from luigi import configuration
from luigi import lock
from luigi import parameter
from luigi import rpc
from luigi import scheduler
from luigi import task
from luigi import worker
from luigi import execution_summary
from luigi.cmdline_parser import CmdlineParser


def setup_interface_logging(conf_file='', level_name='DEBUG'):
    # use a variable in the function object to determine if it has run before
    if getattr(setup_interface_logging, "has_run", False):
        return

    if conf_file == '':
        # no log config given, setup default logging
        level = getattr(logging, level_name, logging.DEBUG)

        logger = logging.getLogger('luigi-interface')
        logger.setLevel(level)

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(level)

        formatter = logging.Formatter('%(levelname)s: %(message)s')
        stream_handler.setFormatter(formatter)

        logger.addHandler(stream_handler)
    else:
        logging.config.fileConfig(conf_file, disable_existing_loggers=False)

    setup_interface_logging.has_run = True


class core(task.Config):

    ''' Keeps track of a bunch of environment params.

    Uses the internal luigi parameter mechanism.
    The nice thing is that we can instantiate this class
    and get an object with all the environment variables set.
    This is arguably a bit of a hack.
    '''
    use_cmdline_section = False

    local_scheduler = parameter.BoolParameter(
        default=False,
        description='Use an in-memory central scheduler. Useful for testing.',
        always_in_help=True)
    scheduler_host = parameter.Parameter(
        default='localhost',
        description='Hostname of machine running remote scheduler',
        config_path=dict(section='core', name='default-scheduler-host'))
    scheduler_port = parameter.IntParameter(
        default=8082,
        description='Port of remote scheduler api process',
        config_path=dict(section='core', name='default-scheduler-port'))
    scheduler_url = parameter.Parameter(
        default='',
        description='Full path to remote scheduler',
        config_path=dict(section='core', name='default-scheduler-url'),
    )
    lock_size = parameter.IntParameter(
        default=1,
        description="Maximum number of workers running the same command")
    no_lock = parameter.BoolParameter(
        default=False,
        description='Ignore if similar process is already running')
    lock_pid_dir = parameter.Parameter(
        default=os.path.join(tempfile.gettempdir(), 'luigi'),
        description='Directory to store the pid file')
    take_lock = parameter.BoolParameter(
        default=False,
        description='Signal other processes to stop getting work if already running')
    workers = parameter.IntParameter(
        default=1,
        description='Maximum number of parallel tasks to run')
    logging_conf_file = parameter.Parameter(
        default='',
        description='Configuration file for logging')
    log_level = parameter.ChoiceParameter(
        default='DEBUG',
        choices=['NOTSET', 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        description="Default log level to use when logging_conf_file is not set")
    module = parameter.Parameter(
        default='',
        description='Used for dynamic loading of modules',
        always_in_help=True)
    parallel_scheduling = parameter.BoolParameter(
        default=False,
        description='Use multiprocessing to do scheduling in parallel.')
    assistant = parameter.BoolParameter(
        default=False,
        description='Run any task from the scheduler.')
    help = parameter.BoolParameter(
        default=False,
        description='Show most common flags and all task-specific flags',
        always_in_help=True)
    help_all = parameter.BoolParameter(
        default=False,
        description='Show all command line flags',
        always_in_help=True)


class _WorkerSchedulerFactory(object):

    def create_local_scheduler(self):
        return scheduler.Scheduler(prune_on_get_work=True, record_task_history=False)

    def create_remote_scheduler(self, url):
        return rpc.RemoteScheduler(url)

    def create_worker(self, scheduler, worker_processes, assistant=False):
        return worker.Worker(
            scheduler=scheduler, worker_processes=worker_processes, assistant=assistant)


def _schedule_and_run(tasks, worker_scheduler_factory=None, override_defaults=None):
    """
    :param tasks:
    :param worker_scheduler_factory:
    :param override_defaults:
    :return: True if all tasks and their dependencies were successfully run (or already completed);
             False if any error occurred.
    """

    if worker_scheduler_factory is None:
        worker_scheduler_factory = _WorkerSchedulerFactory()
    if override_defaults is None:
        override_defaults = {}
    env_params = core(**override_defaults)
    # search for logging configuration path first on the command line, then
    # in the application config file
    logging_conf = env_params.logging_conf_file
    if logging_conf != '' and not os.path.exists(logging_conf):
        raise Exception(
            "Error: Unable to locate specified logging configuration file!"
        )

    if not configuration.get_config().getboolean(
            'core', 'no_configure_logging', False):
        setup_interface_logging(logging_conf, env_params.log_level)

    kill_signal = signal.SIGUSR1 if env_params.take_lock else None
    if (not env_params.no_lock and
            not(lock.acquire_for(env_params.lock_pid_dir, env_params.lock_size, kill_signal))):
        raise PidLockAlreadyTakenExit()

    if env_params.local_scheduler:
        sch = worker_scheduler_factory.create_local_scheduler()
    else:
        if env_params.scheduler_url != '':
            url = env_params.scheduler_url
        else:
            url = 'http://{host}:{port:d}/'.format(
                host=env_params.scheduler_host,
                port=env_params.scheduler_port,
            )
        sch = worker_scheduler_factory.create_remote_scheduler(url=url)

    worker = worker_scheduler_factory.create_worker(
        scheduler=sch, worker_processes=env_params.workers, assistant=env_params.assistant)

    success = True
    logger = logging.getLogger('luigi-interface')
    with worker:
        for t in tasks:
            success &= worker.add(t, env_params.parallel_scheduling)
        logger.info('Done scheduling tasks')
        success &= worker.run()
    logger.info(execution_summary.summary(worker))
    return dict(success=success, worker=worker)


class PidLockAlreadyTakenExit(SystemExit):
    """
    The exception thrown by :py:func:`luigi.run`, when the lock file is inaccessible
    """
    pass


def run(*args, **kwargs):
    return _run(*args, **kwargs)['success']


def _run(cmdline_args=None, main_task_cls=None,
         worker_scheduler_factory=None, use_dynamic_argparse=None, local_scheduler=False):
    """
    Please dont use. Instead use `luigi` binary.

    Run from cmdline using argparse.

    :param cmdline_args:
    :param main_task_cls:
    :param worker_scheduler_factory:
    :param use_dynamic_argparse: Deprecated and ignored
    :param local_scheduler:
    """
    if use_dynamic_argparse is not None:
        warnings.warn("use_dynamic_argparse is deprecated, don't set it.",
                      DeprecationWarning, stacklevel=2)
    if cmdline_args is None:
        cmdline_args = sys.argv[1:]

    if main_task_cls:
        cmdline_args.insert(0, main_task_cls.task_family)
    if local_scheduler:
        cmdline_args.insert(0, '--local-scheduler')

    with CmdlineParser.global_instance(cmdline_args) as cp:
        return _schedule_and_run([cp.get_task_obj()], worker_scheduler_factory)


def build(tasks, worker_scheduler_factory=None, **env_params):
    """
    Run internally, bypassing the cmdline parsing.

    Useful if you have some luigi code that you want to run internally.
    Example:

    .. code-block:: python

        luigi.build([MyTask1(), MyTask2()], local_scheduler=True)

    One notable difference is that `build` defaults to not using
    the identical process lock. Otherwise, `build` would only be
    callable once from each process.

    :param tasks:
    :param worker_scheduler_factory:
    :param env_params:
    :return: True if there were no scheduling errors, even if tasks may fail.
    """
    if "no_lock" not in env_params:
        env_params["no_lock"] = True

    return _schedule_and_run(tasks, worker_scheduler_factory, override_defaults=env_params)['success']
