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

import worker
import lock
import logging
import logging.config
import rpc
import optparse
import scheduler
import warnings
import configuration
import task
import parameter
import re
import argparse
import sys
import os
from task import Register


def setup_interface_logging(conf_file=None):
    # use a variable in the function object to determine if it has run before
    if getattr(setup_interface_logging, "has_run", False):
        return

    if conf_file is None:
        logger = logging.getLogger('luigi-interface')
        logger.setLevel(logging.DEBUG)

        streamHandler = logging.StreamHandler()
        streamHandler.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(levelname)s: %(message)s')
        streamHandler.setFormatter(formatter)

        logger.addHandler(streamHandler)
    else:
        logging.config.fileConfig(conf_file, disable_existing_loggers=False)

    setup_interface_logging.has_run = True


class EnvironmentParamsContainer(task.Task):
    ''' Keeps track of a bunch of environment params.

    Uses the internal luigi parameter mechanism.
    The nice thing is that we can instantiate this class
    and get an object with all the environment variables set.
    This is arguably a bit of a hack.'''

    local_scheduler = parameter.BooleanParameter(
        is_global=True, default=False,
        description='Use local scheduling')
    scheduler_host = parameter.Parameter(
        is_global=True,
        default='localhost',
        description='Hostname of machine running remote scheduler',
        config_path=dict(section='core', name='default-scheduler-host'))
    scheduler_port = parameter.IntParameter(
        is_global=True, default=8082,
        description='Port of remote scheduler api process',
        config_path=dict(section='core', name='default-scheduler-port'))
    lock_size = parameter.IntParameter(
        is_global=True, default=1,
        description="Maximum number of workers running the same command")
    no_lock = parameter.BooleanParameter(
        is_global=True, default=False,
        description='Ignore if similar process is already running')
    lock_pid_dir = parameter.Parameter(
        is_global=True, default='/var/tmp/luigi',
        description='Directory to store the pid file')
    workers = parameter.IntParameter(
        is_global=True, default=1,
        description='Maximum number of parallel tasks to run')
    logging_conf_file = parameter.Parameter(
        is_global=True, default=None,
        description='Configuration file for logging',
        config_path=dict(section='core', name='logging_conf_file'))
    module = parameter.Parameter(
        is_global=True, default=None,
        description='Used for dynamic loading of modules') # see DynamicArgParseInterface
    parallel_scheduling = parameter.BooleanParameter(
        is_global=True, default=False,
        description='Use multiprocessing to do scheduling in parallel.',
        config_path={'section': 'core', 'name': 'parallel-scheduling'},
    )

    @classmethod
    def env_params(cls, override_defaults={}):
        # Override any global parameter with whatever is in override_defaults
        for param_name, param_obj in cls.get_global_params():
            if param_name in override_defaults:
                param_obj.set_global(override_defaults[param_name])

        return cls()  # instantiate an object with the global params set on it


class WorkerSchedulerFactory(object):
    def create_local_scheduler(self):
        return scheduler.CentralPlannerScheduler()

    def create_remote_scheduler(self, host, port):
        return rpc.RemoteScheduler(host=host, port=port)

    def create_worker(self, scheduler, worker_processes):
        return worker.Worker(
            scheduler=scheduler, worker_processes=worker_processes)


class Interface(object):
    def parse(self):
        raise NotImplementedError

    @staticmethod
    def run(tasks, worker_scheduler_factory=None, override_defaults={}):
        """
        :return: True if all tasks and their dependencies were successfully run (or already completed)
        False if any error occurred
        """

        if worker_scheduler_factory is None:
            worker_scheduler_factory = WorkerSchedulerFactory()
        env_params = EnvironmentParamsContainer.env_params(override_defaults)
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
            scheduler=sch, worker_processes=env_params.workers)

        success = True
        for t in tasks:
            success &= w.add(t, env_params.parallel_scheduling)
        logger = logging.getLogger('luigi-interface')
        logger.info('Done scheduling tasks')
        success &= w.run()
        w.stop()
        return success


class ErrorWrappedArgumentParser(argparse.ArgumentParser):
    ''' Wraps ArgumentParser's error message to suggested similar tasks
    '''

    # Simple unweighted Levenshtein distance
    def _editdistance(self, a, b):
        r0 = range(0, len(b) + 1)
        r1 = [0] * (len(b) + 1)

        for i in range(0, len(a)):
            r1[0] = i + 1

            for j in range(0, len(b)):
                c = 0 if a[i] is b[j] else 1
                r1[j + 1] = min(r1[j] + 1, r0[j + 1] + 1, r0[j] + c)

            r0 = r1[:]

        return r1[len(b)]

    def error(self, message):
        result = re.match("argument .+: invalid choice: '(\w+)'.+", message)
        if result:
            arg = result.group(1)
            weightedTasks = [(self._editdistance(arg, task), task) for task in Register.get_reg().keys()]
            orderedTasks = sorted(weightedTasks, key=lambda pair: pair[0])
            candidates = [task for (dist, task) in orderedTasks if dist <= 5 and dist < len(task)]
            displaystring = ""
            if candidates:
                displaystring = "No task %s. Did you mean:\n%s" % (arg, '\n'.join(candidates))
            else:
                displaystring = "No task %s." % arg
            super(ErrorWrappedArgumentParser, self).error(displaystring)
        else:
            super(ErrorWrappedArgumentParser, self).error(message)


class ArgParseInterface(Interface):
    ''' Takes the task as the command, with parameters specific to it
    '''
    @classmethod
    def add_task_parameters(cls, parser, task_cls):
        for param_name, param in task_cls.get_nonglobal_params():
            param.add_to_cmdline_parser(parser, param_name, task_cls.task_family)

    @classmethod
    def add_global_parameters(cls, parser):
        for param_name, param in Register.get_global_params():
            param.add_to_cmdline_parser(parser, param_name)

    def parse_task(self, cmdline_args=None, main_task_cls=None):
        parser = ErrorWrappedArgumentParser()

        self.add_global_parameters(parser)

        if main_task_cls:
            self.add_task_parameters(parser, main_task_cls)

        else:
            orderedtasks = '{%s}' % ','.join(sorted(Register.get_reg().keys()))
            subparsers = parser.add_subparsers(dest='command', metavar=orderedtasks)

            for name, cls in Register.get_reg().iteritems():
                subparser = subparsers.add_parser(name)
                if cls == Register.AMBIGUOUS_CLASS:
                    continue
                self.add_task_parameters(subparser, cls)

                # Add global params here as well so that we can support both:
                # test.py --global-param xyz Test --n 42
                # test.py Test --n 42 --global-param xyz
                self.add_global_parameters(subparser)

        args = parser.parse_args(args=cmdline_args)
        params = vars(args)  # convert to a str -> str hash

        if main_task_cls:
            task_cls = main_task_cls
        else:
            task_cls = Register.get_task_cls(args.command)

        # Notice that this is not side effect free because it might set global params
        task = task_cls.from_str_params(params, Register.get_global_params())

        return [task]

    def parse(self, cmdline_args=None, main_task_cls=None):
        return self.parse_task(cmdline_args, main_task_cls)


class DynamicArgParseInterface(ArgParseInterface):
    ''' Uses --module as a way to load modules dynamically

    Usage:
    python whatever.py --module foo_module FooTask --blah xyz --x 123

    This will dynamically import foo_module and then try to create FooTask from this
    '''

    def parse(self, cmdline_args=None, main_task_cls=None):
        parser = ErrorWrappedArgumentParser()

        self.add_global_parameters(parser)

        args, unknown = parser.parse_known_args(args=cmdline_args)
        module = args.module

        __import__(module)

        return self.parse_task(cmdline_args, main_task_cls)


class PassThroughOptionParser(optparse.OptionParser):
    '''
    An unknown option pass-through implementation of OptionParser.

    When unknown arguments are encountered, bundle with largs and try again,
    until rargs is depleted.

    sys.exit(status) will still be called if a known argument is passed
    incorrectly (e.g. missing arguments or bad argument types, etc.)
    '''
    def _process_args(self, largs, rargs, values):
        while rargs:
            try:
                optparse.OptionParser._process_args(self, largs, rargs, values)
            except (optparse.BadOptionError, optparse.AmbiguousOptionError), e:
                largs.append(e.opt_str)


class OptParseInterface(Interface):
    ''' Supported for legacy reasons where it's necessary to interact with an existing parser.

    Takes the task using --task. All parameters to all possible tasks will be defined globally
    in a big unordered soup.
    '''
    def __init__(self, existing_optparse):
        self.__existing_optparse = existing_optparse

    def parse(self, cmdline_args=None, main_task_cls=None):
        global_params = list(Register.get_global_params())

        parser = PassThroughOptionParser()

        def add_task_option(p):
            if main_task_cls:
                p.add_option('--task', help='Task to run (one of ' + Register.tasks_str() + ') [default: %default]', default=main_task_cls.task_family)
            else:
                p.add_option('--task', help='Task to run (one of %s)' % Register.tasks_str())

        for param_name, param in global_params:
            param.add_to_cmdline_parser(parser, param_name, optparse=True)

        add_task_option(parser)
        options, args = parser.parse_args(args=cmdline_args)

        task_cls_name = options.task
        if self.__existing_optparse:
            parser = self.__existing_optparse
        else:
            parser = optparse.OptionParser()
        add_task_option(parser)

        task_cls = Register.get_task_cls(task_cls_name)

        # Register all parameters as a big mess
        params = task_cls.get_nonglobal_params()

        for param_name, param in global_params:
            param.add_to_cmdline_parser(parser, param_name, optparse=True)

        for param_name, param in params:
            param.add_to_cmdline_parser(parser, param_name, optparse=True)

        # Parse and run
        options, args = parser.parse_args(args=cmdline_args)

        params = {}
        for k, v in vars(options).iteritems():
            if k != 'task':
                params[k] = v

        task = task_cls.from_str_params(params, global_params)

        return [task]


def load_task(module, task_name, params_str):
    """ Imports task dynamically given a module and a task name"""
    __import__(module)
    task_cls = Register.get_task_cls(task_name)
    return task_cls.from_str_params(params_str)


def run(cmdline_args=None, existing_optparse=None, use_optparse=False, main_task_cls=None, worker_scheduler_factory=None, use_dynamic_argparse=False):
    ''' Run from cmdline.

    The default parser uses argparse.
    However for legacy reasons we support optparse that optionally allows for
    overriding an existing option parser with new args.
    '''
    if use_optparse:
        interface = OptParseInterface(existing_optparse)
    elif use_dynamic_argparse:
        interface = DynamicArgParseInterface()
    else:
        interface = ArgParseInterface()
    tasks = interface.parse(cmdline_args, main_task_cls=main_task_cls)
    return interface.run(tasks, worker_scheduler_factory)


def build(tasks, worker_scheduler_factory=None, **env_params):
    ''' Run internally, bypassing the cmdline parsing.

    Useful if you have some luigi code that you want to run internally.
    Example
    luigi.build([MyTask1(), MyTask2()], local_scheduler=True)

    One notable difference is that `build` defaults to not using
    the identical process lock. Otherwise, `build` would only be
    callable once from each process.
    '''
    if "no_lock" not in env_params:
        # TODO(erikbern): should we really override args here?
        env_params["no_lock"] = True

    Interface.run(tasks, worker_scheduler_factory, env_params)
