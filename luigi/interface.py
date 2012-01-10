import worker, lock

class Register:
    def __init__(self):
        self.__reg = {}
        self.__main = None

    def expose(self, cls, main=False):
        name = cls.__name__
        if main:
            assert self.__main == None
            self.__main = cls
        assert name not in self.__reg # TODO: raise better exception
        self.__reg[name] = cls
        return cls

    def get_reg(self): return self.__reg
    def get_main(self): return self.__main

register = Register()

def expose(cls): return register.expose(cls)
def expose_main(cls): return register.expose(cls, True)

class Interface(object):
    def run(self): raise NotImplementedError

class ArgParseInterface(Interface):
    ''' Takes the task as the command, with parameters specific to it
    '''
    def run(self, cmdline_args=None):
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument('--local-scheduler', help='Use local scheduling', action='store_true')
        parser.add_argument('--lock', help='Do not run if the task is already running', action='store_true')
        parser.add_argument('--lock-pid-dir', help='Directory to store the pid file [default: %(default)]', default='/var/tmp/luigi')

        def _add_task_parameters(parser, cls):
            params = cls.get_params()
            for param_name, param in params:
                if param.has_default: default = param.default
                else: default = None
                parser.add_argument('--' + param_name.replace('_', '-'), help='%s.%s [default: %%(default)s]' % (cls.__name__, param_name), default=default)
                
        if register.get_main():
            _add_task_parameters(parser, register.get_main())
        
        else:
            subparsers = parser.add_subparsers(dest='command')
        
            for name, cls in register.get_reg().iteritems():
                subparser = subparsers.add_parser(name)
                _add_task_parameters(subparser, cls)

        args = parser.parse_args(args=cmdline_args)
        if args.lock: lock.run_once(args.lock_pid_dir)
        params = vars(args) # convert to a str -> str hash

        if register.get_main():
            task_cls = register.get_main()
        else:
            task_cls = register.get_reg()[args.command]

        task = task_cls.from_input(params)

        w = worker.Worker(locally=args.local_scheduler)
    
        w.add(task)
        w.run()

class OptParseInterface(Interface):
    ''' Supported for legacy reasons where it's necessary to interact with an existing parser.

    Takes the task using --task. All parameters to all possible tasks will be defined globally
    in a big unordered soup.
    '''
    def __init__(self, existing_optparse):
        self.__existing_optparse = existing_optparse

    def run(self, cmdline_args=None):
        import optparse
        if self.__existing_optparse: parser = self.__existing_optparse
        else: parser = optparse.OptionParser()

        parser.add_option('--local-scheduler', help='Use local scheduling', action='store_true')
        parser.add_option('--lock', help='Do not run if the task is already running', action='store_true')
        parser.add_option('--lock-pid-dir', help='Directory to store the pid file [default: %default]', default='/var/tmp/luigi')

        tasks_str = '/'.join([name for name in register.get_reg()])
        
        if register.get_main():
            parser.add_option('--task', help='Task to run (' + tasks_str + ') [default: %default]', default=register.get_main().__name__)
        else:
            parser.add_option('--task', help='Task to run (%s)' % tasks_str)

        # Register all parameters as a big mess
        parameter_clses = {}
        parameter_defaults = {}
        for name, cls in register.get_reg().iteritems():
            params = cls.get_params()
            for param_name, param in params:
                parameter_clses.setdefault(param_name, []).append(cls)
                if param.has_default: parameter_defaults[param_name] = param.default # Will override with whatever: TODO: do more sensibly!
                
        for param_name, clses in parameter_clses.iteritems():
            tasks_str = ','.join([cls.__name__ for cls in clses])
            parser.add_option('--' + param_name.replace('_', '-'),
                              help='{%s}.%s [default: %%default]' % (tasks_str, param_name),
                              default=parameter_defaults.get(param_name, None))

        # Parse and run
        args, _ = parser.parse_args(args=cmdline_args)
        if args.lock: lock.run_once(args.lock_pid_dir)
        task_cls = register.get_reg()[args.task]
        params = {}
        for k, v in vars(args).iteritems():
            if k not in ['task', 'local_scheduler']:
                params[k] = str(v)
        task = task_cls.from_input(params)

        # Run
        w = worker.Worker(locally=args.local_scheduler)

        w.add(task)
        w.run()

def run(cmdline_args=None, existing_optparse=None, use_optparse=False):
    ''' Run from cmdline.

    The default parser uses argparse.
    However for legacy reasons we support optparse that optinally allows for
    overriding an existing option parser with new args.
    '''
    if use_optparse: interface = OptParseInterface(existing_optparse)
    else: interface = ArgParseInterface()

    interface.run(cmdline_args)
        
