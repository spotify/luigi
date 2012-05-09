import worker
import lock
import scheduler
import logging
import ConfigParser
import rpc
import optparse

class Register(object):
    def __init__(self):
        self.__reg = {}
        self.__main = None

    def expose(self, cls, main=False):
        name = cls.__name__
        if main:
            assert self.__main == None
            self.__main = cls
        assert name not in self.__reg  # TODO: raise better exception
        self.__reg[name] = cls
        return cls

    def get_reg(self):
        return self.__reg

    def get_main(self):
        return self.__main

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
    def run(self, cmdline_args=None, config=None):
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument('--local-scheduler', help='Use local scheduling', action='store_true')
        parser.add_argument('--scheduler-host', help='Hostname of machine running remote scheduler [default: %(default)s]', default='localhost')
        parser.add_argument('--lock', help='Do not run if the task is already running', action='store_true')
        parser.add_argument('--lock-pid-dir', help='Directory to store the pid file [default: %(default)s]', default='/var/tmp/luigi')

        def _add_task_parameters(parser, cls):
            params = cls.get_params()
            for param_name, param in params:
                if param.has_default:
                    defaulthelp = "[default: %s]" % param.default
                else:
                    defaulthelp = ""

                parser.add_argument('--' + param_name.replace('_', '-'), help='%s.%s%s' % (cls.__name__, param_name, defaulthelp), default=None)

        if register.get_main():
            _add_task_parameters(parser, register.get_main())

        else:
            subparsers = parser.add_subparsers(dest='command')

            for name, cls in register.get_reg().iteritems():
                subparser = subparsers.add_parser(name)
                _add_task_parameters(subparser, cls)

        args = parser.parse_args(args=cmdline_args)
        if args.lock:
            lock.run_once(args.lock_pid_dir)
        params = vars(args)  # convert to a str -> str hash

        if register.get_main():
            task_cls = register.get_main()
        else:
            task_cls = register.get_reg()[args.command]

        task = task_cls.from_input(params)

        if not args.local_scheduler:
            sch = rpc.RemoteScheduler(host=args.scheduler_host)
        else:
            sch = None

        w = worker.Worker(sch=sch, locally=args.local_scheduler)

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
                optparse.OptionParser._process_args(self,largs,rargs,values)
            except (optparse.BadOptionError,optparse.AmbiguousOptionError), e:
                largs.append(e.opt_str)


class OptParseInterface(Interface):
    ''' Supported for legacy reasons where it's necessary to interact with an existing parser.

    Takes the task using --task. All parameters to all possible tasks will be defined globally
    in a big unordered soup.
    '''
    def __init__(self, existing_optparse):
        self.__existing_optparse = existing_optparse

    def run(self, cmdline_args=None, config=None):
        parser = PassThroughOptionParser()
        tasks_str = '/'.join([name for name in register.get_reg()])
        def add_task_option(p):
            if register.get_main():
                p.add_option('--task', help='Task to run (' + tasks_str + ') [default: %default]', default=register.get_main().__name__)
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
        if config:
            default_scheduler = config.get('luigi', 'scheduler-host')
        else:
            default_scheduler = 'localhost'
        parser.add_option('--local-scheduler', help='Use local scheduling', action='store_true')
        parser.add_option('--scheduler-host', help='Hostname of machine running remote scheduler [default: %default]', default=default_scheduler)
        parser.add_option('--lock', help='Do not run if the task is already running', action='store_true')
        parser.add_option('--lock-pid-dir', help='Directory to store the pid file [default: %default]', default='/var/tmp/luigi')


        # Register all parameters as a big mess
        parameter_defaults = {}
        task_cls = register.get_reg()[task_cls_name]
        params = task_cls.get_params()
        for param_name, param in params:
            if param.has_default:
                parameter_defaults[param_name] = param.default  # Will override with whatever: TODO: do more sensibly!

        for param_name, param in params:
            if param.has_default:
                help_text = '%s [default: %s]' % (param_name, parameter_defaults)
            else:
                help_text = param_name
            parser.add_option('--' + param_name.replace('_', '-'),
                              help=help_text,
                              default=None)
                

        # Parse and run
        options, args = parser.parse_args(args=cmdline_args)
        if options.lock:
            lock.run_once(options.lock_pid_dir)
        params = {}
        for k, v in vars(options).iteritems():
            if k not in ['task', 'local_scheduler']:
                params[k] = v
        task = task_cls.from_input(params)

        if not options.local_scheduler:
            sch = rpc.RemoteScheduler(host=options.scheduler_host)
        else:
            sch = None

        # Run
        w = worker.Worker(sch=sch, locally=options.local_scheduler)

        w.add(task)
        w.run()


def run(cmdline_args=None, existing_optparse=None, use_optparse=False):
    ''' Run from cmdline.

    The default parser uses argparse.
    However for legacy reasons we support optparse that optinally allows for
    overriding an existing option parser with new args.
    '''
    setup_interface_logging()
    config = load_config()
    if use_optparse:
        interface = OptParseInterface(existing_optparse)
    else:
        interface = ArgParseInterface()

    interface.run(cmdline_args, config)

def setup_interface_logging():
    logger = logging.getLogger('luigi-interface')
    logger.setLevel(logging.DEBUG)

    streamHandler = logging.StreamHandler()
    streamHandler.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(levelname)s: %(message)s')
    streamHandler.setFormatter(formatter)

    logger.addHandler(streamHandler)
    

def load_config():
    config = ConfigParser.ConfigParser()
    result = config.read('/etc/luigi/client.cfg')
    if result == []:
        return None
    else:
        return config

if __name__ == '__main__':
    config = load_config()
    print config.get('luigi', 'scheduler-host')
