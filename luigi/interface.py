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
import rpc
import optparse
import scheduler

from ConfigParser import RawConfigParser, NoOptionError, NoSectionError


class LuigiConfigParser(RawConfigParser):
    NO_DEFAULT = object()
    _instance = None

    @classmethod
    def instance(cls, *args, **kwargs):
        """ Singleton getter """
        if cls._instance is None:
            config = cls(*args, **kwargs)

            config.read(['/etc/luigi/client.cfg', 'client.cfg'])
            cls._instance = config

        return cls._instance

    def get(self, section, option, default=NO_DEFAULT):
        try:
            return RawConfigParser.get(self, section, option)
        except (NoOptionError, NoSectionError):
            if default is LuigiConfigParser.NO_DEFAULT:
                raise
            return default


def get_config():
    """ Convenience method (for backwards compatibility) for accessing config singleton """
    return LuigiConfigParser.instance()


class Register(object):
    def __init__(self):
        self.__reg = {}
        self.__main = None
        self.__global_params = {}

    def expose(self, cls, main=False):
        name = cls.task_family
        if main:
            assert self.__main == None
            self.__main = cls
        assert name not in self.__reg  # TODO: raise better exception
        self.__reg[name] = cls
        for param_name, param_obj in cls.get_global_params():
            if param_name in self.__global_params and self.__global_params[param_name] != param_obj:
                # Could be registered multiple times in case there's subclasses
                raise Exception('Global parameter %r registered by multiple classes' % param_name)
            self.__global_params[param_name] = param_obj
        return cls

    def get_reg(self):
        return self.__reg

    def get_main(self):
        return self.__main

    def get_global_params(self):
        return self.__global_params.iteritems()

register = Register()


def expose(cls):
    return register.expose(cls)


def expose_main(cls):
    return register.expose(cls, True)


class Interface(object):
    def run(self):
        raise NotImplementedError


class ArgParseInterface(Interface):
    ''' Takes the task as the command, with parameters specific to it
    '''
    def run(self, cmdline_args=None):
        import argparse
        parser = argparse.ArgumentParser()
        config = get_config()
        default_scheduler_host = config.get('core', 'default-scheduler-host', default='localhost')

        parser.add_argument('--local-scheduler', help='Use local scheduling', action='store_true')
        parser.add_argument('--scheduler-host', help='Hostname of machine running remote scheduler [default: %(default)s]', default=default_scheduler_host)
        parser.add_argument('--lock', help='Do not run if the task is already running', action='store_true')
        parser.add_argument('--lock-pid-dir', help='Directory to store the pid file [default: %(default)s]', default='/var/tmp/luigi')
        parser.add_argument('--workers', help='Maximum number of parallel tasks to run [default: %(default)s]', default=1, type=int)

        def _add_parameter(parser, param_name, param, prefix=''):
            if param.has_default:
                defaulthelp = "[default: %s]" % (param.default,)
            else:
                defaulthelp = ""

            if param.is_list:
                action = "append"
            elif param.is_boolean:
                action = "store_true"
            else:
                action = "store"
            parser.add_argument('--' + param_name.replace('_', '-'), help='%s%s%s' % (prefix, param_name, defaulthelp), default=None, action=action)

        def _add_task_parameters(parser, cls):
            for param_name, param in cls.get_nonglobal_params():
                _add_parameter(parser, param_name, param, cls.task_family + '.')

        def _add_global_parameters(parser):
            for param_name, param in register.get_global_params():
                _add_parameter(parser, param_name, param)

        if register.get_main():
            _add_task_parameters(parser, register.get_main())
            _add_global_parameters(parser)

        else:
            subparsers = parser.add_subparsers(dest='command')

            for name, cls in register.get_reg().iteritems():
                subparser = subparsers.add_parser(name)
                _add_task_parameters(subparser, cls)
                _add_global_parameters(subparser)

        args = parser.parse_args(args=cmdline_args)
        if args.lock:
            lock.run_once(args.lock_pid_dir)
        params = vars(args)  # convert to a str -> str hash

        if register.get_main():
            task_cls = register.get_main()
        else:
            task_cls = register.get_reg()[args.command]

        task = task_cls.from_input(params, register.get_global_params())

        if args.local_scheduler:
            sch = scheduler.CentralPlannerScheduler()
        else:
            sch = rpc.RemoteScheduler(host=args.scheduler_host)

        w = worker.Worker(scheduler=sch, worker_processes=args.workers)

        w.add(task)
        w.run()


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

    def run(self, cmdline_args=None):
        config = get_config()
        parser = PassThroughOptionParser()
        tasks_str = '/'.join(sorted([name for name in register.get_reg()]))

        def add_task_option(p):
            if register.get_main():
                p.add_option('--task', help='Task to run (' + tasks_str + ') [default: %default]', default=register.get_main().task_family)
            else:
                p.add_option('--task', help='Task to run (%s)' % tasks_str)
        add_task_option(parser)
        options, args = parser.parse_args(args=cmdline_args)

        task_cls_name = options.task
        if self.__existing_optparse:
            parser = self.__existing_optparse
        else:
            parser = optparse.OptionParser()
        add_task_option(parser)
        default_scheduler_host = config.get('core', 'default-scheduler-host', default='localhost')

        parser.add_option('--local-scheduler', help='Use local scheduling', action='store_true')
        parser.add_option('--scheduler-host', help='Hostname of machine running remote scheduler [default: %default]', default=default_scheduler_host)
        parser.add_option('--lock', help='Do not run if the task is already running', action='store_true')
        parser.add_option('--lock-pid-dir', help='Directory to store the pid file [default: %default]', default='/var/tmp/luigi')
        parser.add_option('--workers', help='Maximum number of parallel tasks to run [default: %default]', default=1, type=int)

        if task_cls_name not in register.get_reg():
            raise Exception('Error: %s is not a valid tasks (must be %s)' % (task_cls_name, tasks_str))

        # Register all parameters as a big mess
        parameter_defaults = {}
        task_cls = register.get_reg()[task_cls_name]
        params = task_cls.get_nonglobal_params()
        global_params = list(register.get_global_params())

        for param_name, param in global_params:
            parameter_defaults[param_name] = param.default

        for param_name, param in params:
            if param.has_default:
                parameter_defaults[param_name] = param.default  # Will override with whatever: TODO: do more sensibly!

        def _add_parameter(parser, param_name, param, parameter_defaults):
            if param.has_default:
                help_text = '%s [default: %s]' % (param_name, parameter_defaults)
            else:
                help_text = param_name
            if param.is_list:
                action = "append"
            elif param.is_boolean:
                action = "store_true"
            else:
                action = "store"
            parser.add_option('--' + param_name.replace('_', '-'),
                              help=help_text,
                              default=None,
                              action=action)

        for param_name, param in global_params:
            _add_parameter(parser, param_name, param, parameter_defaults)

        for param_name, param in params:
            _add_parameter(parser, param_name, param, parameter_defaults)

        # Parse and run
        options, args = parser.parse_args(args=cmdline_args)
        if options.lock:
            lock.run_once(options.lock_pid_dir)
        params = {}
        for k, v in vars(options).iteritems():
            if k not in ['task', 'local_scheduler']:
                params[k] = v
        task = task_cls.from_input(params, global_params)

        if options.local_scheduler:
            sch = scheduler.CentralPlannerScheduler()
        else:
            sch = rpc.RemoteScheduler(host=options.scheduler_host)

        # Run
        w = worker.Worker(scheduler=sch, worker_processes=options.workers)

        w.add(task)
        w.run()


def run(cmdline_args=None, existing_optparse=None, use_optparse=False):
    ''' Run from cmdline.

    The default parser uses argparse.
    However for legacy reasons we support optparse that optinally allows for
    overriding an existing option parser with new args.
    '''
    setup_interface_logging()
    if use_optparse:
        interface = OptParseInterface(existing_optparse)
    else:
        interface = ArgParseInterface()
    interface.run(cmdline_args)


def setup_interface_logging():
    logger = logging.getLogger('luigi-interface')
    logger.setLevel(logging.DEBUG)

    streamHandler = logging.StreamHandler()
    streamHandler.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(levelname)s: %(message)s')
    streamHandler.setFormatter(formatter)

    logger.addHandler(streamHandler)
