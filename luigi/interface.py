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
"""

import argparse
import logging
import logging.config
import os
import sys
import tempfile

from luigi import configuration
from luigi import lock
from luigi import parameter
from luigi import rpc
from luigi import scheduler
from luigi import task
from luigi import worker
from luigi.task_register import Register


def setup_interface_logging(conf_file=None):
    # use a variable in the function object to determine if it has run before
    if getattr(setup_interface_logging, "has_run", False):
        return

    if conf_file is None:
        logger = logging.getLogger('luigi-interface')
        logger.setLevel(logging.DEBUG)

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.DEBUG)

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
        description='Use local scheduling')
    scheduler_host = parameter.Parameter(
        default='localhost',
        description='Hostname of machine running remote scheduler',
        config_path=dict(section='core', name='default-scheduler-host'))
    scheduler_port = parameter.IntParameter(
        default=8082,
        description='Port of remote scheduler api process',
        config_path=dict(section='core', name='default-scheduler-port'))
    lock_size = parameter.IntParameter(
        default=1,
        description="Maximum number of workers running the same command")
    no_lock = parameter.BoolParameter(
        default=False,
        description='Ignore if similar process is already running')
    lock_pid_dir = parameter.Parameter(
        default=os.path.join(tempfile.gettempdir(), 'luigi'),
        description='Directory to store the pid file')
    workers = parameter.IntParameter(
        default=1,
        description='Maximum number of parallel tasks to run')
    logging_conf_file = parameter.Parameter(
        default=None,
        description='Configuration file for logging')
    module = parameter.Parameter(
        default=None,
        description='Used for dynamic loading of modules')  # see DynamicArgParseInterface
    parallel_scheduling = parameter.BoolParameter(
        default=False,
        description='Use multiprocessing to do scheduling in parallel.')
    assistant = parameter.BoolParameter(
        default=False,
        description='Run any task from the scheduler.')


class WorkerSchedulerFactory(object):

    def create_local_scheduler(self):
        return scheduler.CentralPlannerScheduler(prune_on_get_work=True)

    def create_remote_scheduler(self, host, port):
        return rpc.RemoteScheduler(host=host, port=port)

    def create_worker(self, scheduler, worker_processes, assistant=False):
        return worker.Worker(
            scheduler=scheduler, worker_processes=worker_processes, assistant=assistant)


class Interface(object):

    def parse(self):
        raise NotImplementedError

    @staticmethod
    def run(tasks, worker_scheduler_factory=None, override_defaults=None):
        """
        :param tasks:
        :param worker_scheduler_factory:
        :param override_defaults:
        :return: True if all tasks and their dependencies were successfully run (or already completed);
                 False if any error occurred.
        """

        if worker_scheduler_factory is None:
            worker_scheduler_factory = WorkerSchedulerFactory()
        if override_defaults is None:
            override_defaults = {}
        env_params = core(**override_defaults)
        # search for logging configuration path first on the command line, then
        # in the application config file
        logging_conf = env_params.logging_conf_file
        if logging_conf is not None and not os.path.exists(logging_conf):
            raise Exception(
                "Error: Unable to locate specified logging configuration file!"
            )

        if not configuration.get_config().getboolean(
                'core', 'no_configure_logging', False):
            setup_interface_logging(logging_conf)

        if (not env_params.no_lock and
                not(lock.acquire_for(env_params.lock_pid_dir, env_params.lock_size))):
            sys.exit(1)

        if env_params.local_scheduler:
            sch = worker_scheduler_factory.create_local_scheduler()
        else:
            sch = worker_scheduler_factory.create_remote_scheduler(
                host=env_params.scheduler_host,
                port=env_params.scheduler_port)

        w = worker_scheduler_factory.create_worker(
            scheduler=sch, worker_processes=env_params.workers, assistant=env_params.assistant)

        success = True
        for t in tasks:
            success &= w.add(t, env_params.parallel_scheduling)
        logger = logging.getLogger('luigi-interface')
        logger.info('Done scheduling tasks')
        if env_params.workers != 0:
            success &= w.run()
        w.stop()
        return success


def add_task_parameters(parser, task_cls):
    for param_name, param in task_cls.get_params():
        param.add_to_cmdline_parser(parser, param_name, task_cls.task_family, glob=False)


def get_global_parameters():
    seen_params = set()
    for task_name, is_without_section, param_name, param in Register.get_all_params():
        if param in seen_params:
            continue
        seen_params.add(param)
        yield task_name, is_without_section, param_name, param


def add_global_parameters(parser):
    for task_name, is_without_section, param_name, param in get_global_parameters():
        param.add_to_cmdline_parser(parser, param_name, task_name, glob=True, is_without_section=is_without_section)


def get_task_parameters(task_cls, args):
    # Parse a str->str dict to the correct types
    params = {}
    for param_name, param in task_cls.get_params():
        param.parse_from_args(param_name, task_cls.task_family, args, params)
    return params


def set_global_parameters(args):
    # Note that this is not side effect free
    for task_name, is_without_section, param_name, param in get_global_parameters():
        param.set_global_from_args(param_name, task_name, args, is_without_section=is_without_section)


class ArgParseInterface(Interface):
    """
    Takes the task as the command, with parameters specific to it.
    """

    def parse_task(self, cmdline_args=None):
        if cmdline_args is None:
            cmdline_args = sys.argv[1:]

        parser = argparse.ArgumentParser()

        add_global_parameters(parser)

        task_names = Register.task_names()

        # Parse global arguments and pull out the task name.
        # We used to do this using subparsers+command, but some issues with
        # argparse across different versions of Python (2.7.9) made it hard.
        args, unknown = parser.parse_known_args(args=[a for a in cmdline_args if a != '--help'])
        if len(unknown) == 0:
            # In case it included a --help argument, run again
            parser.parse_known_args(args=cmdline_args)
            raise SystemExit('No task specified')

        task_name = unknown[0]
        task_cls = Register.get_task_cls(task_name)

        # Add a subparser to parse task-specific arguments
        subparsers = parser.add_subparsers(dest='command')
        subparser = subparsers.add_parser(task_name)

        # Add both task and global params here so that we can support both:
        # test.py --global-param xyz Test --n 42
        # test.py Test --n 42 --global-param xyz
        add_global_parameters(subparser)
        add_task_parameters(subparser, task_cls)

        # Workaround for bug in argparse for Python 2.7.9
        # See https://mail.python.org/pipermail/python-dev/2015-January/137699.html
        subargs = parser.parse_args(args=cmdline_args)
        for key, value in vars(subargs).items():
            if value:  # Either True (for boolean args) or non-None (everything else)
                setattr(args, key, value)

        # Notice that this is not side effect free because it might set global params
        set_global_parameters(args)
        task_params = get_task_parameters(task_cls, args)

        return [task_cls(**task_params)]

    def parse(self, cmdline_args=None):
        return self.parse_task(cmdline_args)


class DynamicArgParseInterface(ArgParseInterface):
    """
    Uses --module as a way to load modules dynamically

    Usage:

    .. code-block:: console

        python whatever.py --module foo_module FooTask --blah xyz --x 123

    This will dynamically import foo_module and then try to create FooTask from this.
    """

    def parse(self, cmdline_args=None):
        if cmdline_args is None:
            cmdline_args = sys.argv[1:]

        parser = argparse.ArgumentParser()

        add_global_parameters(parser)

        args, unknown = parser.parse_known_args(args=[a for a in cmdline_args if a != '--help'])
        module = args.module

        if module:
            __import__(module)

        return self.parse_task(cmdline_args)


def run(cmdline_args=None, main_task_cls=None,
        worker_scheduler_factory=None, use_dynamic_argparse=False, local_scheduler=False):
    """
    Please dont use. Instead use `luigi` binary.

    Run from cmdline using argparse.

    :param cmdline_args:
    :param main_task_cls:
    :param worker_scheduler_factory:
    :param use_dynamic_argparse:
    :param local_scheduler:
    """
    if cmdline_args is None:
        cmdline_args = sys.argv[1:]

    if use_dynamic_argparse:
        interface = DynamicArgParseInterface()
    else:
        interface = ArgParseInterface()
    if main_task_cls:
        cmdline_args.insert(0, main_task_cls.task_family)
    if local_scheduler:
        cmdline_args.insert(0, '--local-scheduler')
    tasks = interface.parse(cmdline_args)
    return interface.run(tasks, worker_scheduler_factory)


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
    :return:
    """
    if "no_lock" not in env_params:
        env_params["no_lock"] = True

    Interface.run(tasks, worker_scheduler_factory, override_defaults=env_params)
