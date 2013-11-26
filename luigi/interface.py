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
        logging.config.fileConfig(conf_file)

    setup_interface_logging.has_run = True


def get_config():
    warnings.warn('Use luigi.configuration.get_config() instead')
    return configuration.get_config()


class EnvironmentParamsContainer(task.Task):
    ''' Keeps track of a bunch of environment params.

    Uses the internal luigi parameter mechanism. The nice thing is that we can instantiate this class
    and get an object with all the environment variables set. This is arguably a bit of a hack.'''
    # TODO(erikbern): would be cleaner if we don't have to read config in global scope
    local_scheduler = parameter.BooleanParameter(is_global=True, default=False,
                                                 description='Use local scheduling')
    scheduler_host = parameter.Parameter(is_global=True, default=configuration.get_config().get('core', 'default-scheduler-host', default='localhost'),
                                         description='Hostname of machine running remote scheduler')
    scheduler_port = parameter.IntParameter(is_global=True, default=8082,
                                            description='Port of remote scheduler api process')
    lock = parameter.BooleanParameter(is_global=True, default=False,
                                      description='Do not run if the task is already running')
    lock_pid_dir = parameter.Parameter(is_global=True, default='/var/tmp/luigi',
                                       description='Directory to store the pid file')
    workers = parameter.IntParameter(is_global=True, default=1,
                                     description='Maximum number of parallel tasks to run')
    logging_conf_file = parameter.Parameter(is_global=True, default=None,
                                     description='Configuration file for logging')

    @classmethod
    def env_params(cls, override_defaults):
        # Override any global parameter with whatever is in override_defaults
        for param_name, param_obj in cls.get_global_params():
            if param_name in override_defaults:
                param_obj.set_default(override_defaults[param_name])

        return cls()  # instantiate an object with the global params set on it


def expose(cls):
    warnings.warn('expose is no longer used, everything is autoexposed', DeprecationWarning)
    return cls


def expose_main(cls):
    warnings.warn('expose_main is no longer supported, use luigi.run(..., main_task_cls=cls) instead', DeprecationWarning)
    return cls


def reset():
    warnings.warn('reset is no longer supported')


class WorkerSchedulerFactory(object):
    def create_local_scheduler(self):
        return scheduler.CentralPlannerScheduler()

    def create_remote_scheduler(self, host, port):
        return rpc.RemoteScheduler(host=host, port=port)

    def create_worker(self, scheduler, worker_processes):
        return worker.Worker(scheduler=scheduler, worker_processes=worker_processes)


class Interface(object):
    def parse(self):
        raise NotImplementedError

    @staticmethod
    def run(tasks, worker_scheduler_factory=None, override_defaults={}):

        if worker_scheduler_factory is None:
            worker_scheduler_factory = WorkerSchedulerFactory()

        env_params = EnvironmentParamsContainer.env_params(override_defaults)
        # search for logging configuration path first on the command line, then
        # in the application config file
        logging_conf = env_params.logging_conf_file or \
            configuration.get_config().get('core', 'logging_conf_file', None)
        if logging_conf is not None and not os.path.exists(logging_conf):
            raise Exception("Error: Unable to locate specified logging configuration file!")

        if not configuration.get_config().getboolean('core', 'no_configure_logging', False):
            setup_interface_logging(logging_conf)

        if env_params.lock and not(lock.acquire_for(env_params.lock_pid_dir)):
            sys.exit(1)

        if env_params.local_scheduler:
            sch = worker_scheduler_factory.create_local_scheduler()
        else:
            sch = worker_scheduler_factory.create_remote_scheduler(host=env_params.scheduler_host, port=env_params.scheduler_port)

        w = worker_scheduler_factory.create_worker(scheduler=sch, worker_processes=env_params.workers)

        for task in tasks:
            w.add(task)
        logger = logging.getLogger('luigi-interface')
        logger.info('Done scheduling tasks')
        w.run()
        w.stop()


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
    def parse(self, cmdline_args=None, main_task_cls=None):
        parser = ErrorWrappedArgumentParser()

        def _add_parameter(parser, param_name, param, prefix=None):
            description = []
            if prefix:
                description.append('%s.%s' % (prefix, param_name))
            else:
                description.append(param_name)
            if param.description:
                description.append(param.description)
            if param.has_default:
                description.append(" [default: %s]" % (param.default,))

            if param.is_list:
                action = "append"
            elif param.is_boolean:
                action = "store_true"
            else:
                action = "store"
            parser.add_argument('--' + param_name.replace('_', '-'), help=' '.join(description), default=None, action=action)

        def _add_task_parameters(parser, cls):
            for param_name, param in cls.get_nonglobal_params():
                _add_parameter(parser, param_name, param, cls.task_family)

        def _add_global_parameters(parser):
            for param_name, param in Register.get_global_params():
                _add_parameter(parser, param_name, param)

        _add_global_parameters(parser)

        if main_task_cls:
            _add_task_parameters(parser, main_task_cls)

        else:
            orderedtasks = '{%s}' % ','.join(sorted(Register.get_reg().keys()))
            subparsers = parser.add_subparsers(dest='command', metavar=orderedtasks)

            for name, cls in Register.get_reg().iteritems():
                subparser = subparsers.add_parser(name)
                if cls == Register.AMBIGUOUS_CLASS:
                    continue
                _add_task_parameters(subparser, cls)

                # Add global params here as well so that we can support both:
                # test.py --global-param xyz Test --n 42
                # test.py Test --n 42 --global-param xyz
                _add_global_parameters(subparser)

        args = parser.parse_args(args=cmdline_args)
        params = vars(args)  # convert to a str -> str hash

        if main_task_cls:
            task_cls = main_task_cls
        else:
            task_cls = Register.get_reg()[args.command]

        if task_cls == Register.AMBIGUOUS_CLASS:
            raise Exception('%s is ambigiuous' % args.command)

        # Notice that this is not side effect free because it might set global params
        task = task_cls.from_input(params, Register.get_global_params())

        return [task]


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
        tasks_str = '/'.join(sorted([name for name in Register.get_reg()]))

        def add_task_option(p):
            if main_task_cls:
                p.add_option('--task', help='Task to run (' + tasks_str + ') [default: %default]', default=main_task_cls.task_family)
            else:
                p.add_option('--task', help='Task to run (%s)' % tasks_str)

        def _add_parameter(parser, param_name, param):
            description = [param_name]
            if param.description:
                description.append(param.description)
            if param.has_default:
                description.append(" [default: %s]" % (param.default,))

            if param.is_list:
                action = "append"
            elif param.is_boolean:
                action = "store_true"
            else:
                action = "store"
            parser.add_option('--' + param_name.replace('_', '-'),
                              help=' '.join(description),
                              default=None,
                              action=action)

        for param_name, param in global_params:
            _add_parameter(parser, param_name, param)

        add_task_option(parser)
        options, args = parser.parse_args(args=cmdline_args)

        task_cls_name = options.task
        if self.__existing_optparse:
            parser = self.__existing_optparse
        else:
            parser = optparse.OptionParser()
        add_task_option(parser)

        if task_cls_name not in Register.get_reg():
            raise Exception('Error: %s is not a valid tasks (must be %s)' % (task_cls_name, tasks_str))

        # Register all parameters as a big mess
        task_cls = Register.get_reg()[task_cls_name]
        if task_cls == Register.AMBIGUOUS_CLASS:
            raise Exception('%s is ambiguous' % task_cls_name)

        params = task_cls.get_nonglobal_params()

        for param_name, param in global_params:
            _add_parameter(parser, param_name, param)

        for param_name, param in params:
            _add_parameter(parser, param_name, param)

        # Parse and run
        options, args = parser.parse_args(args=cmdline_args)

        params = {}
        for k, v in vars(options).iteritems():
            if k != 'task':
                params[k] = v

        task = task_cls.from_input(params, global_params)

        return [task]


class LuigiConfigParser(configuration.LuigiConfigParser):
    ''' Deprecated class, use configuration.LuigiConfigParser instead. Left for backwards compatibility '''
    pass


def run(cmdline_args=None, existing_optparse=None, use_optparse=False, main_task_cls=None, worker_scheduler_factory=None):
    ''' Run from cmdline.

    The default parser uses argparse.
    However for legacy reasons we support optparse that optionally allows for
    overriding an existing option parser with new args.
    '''
    if use_optparse:
        interface = OptParseInterface(existing_optparse)
    else:
        interface = ArgParseInterface()
    tasks = interface.parse(cmdline_args, main_task_cls=main_task_cls)
    interface.run(tasks, worker_scheduler_factory)


def build(tasks, worker_scheduler_factory=None, **env_params):
    ''' Run internally, bypassing the cmdline parsing.

    Useful if you have some luigi code that you want to run internally.
    Example
    luigi.build([MyTask1(), MyTask2()], local_scheduler=True)
    '''
    Interface.run(tasks, worker_scheduler_factory, env_params)
