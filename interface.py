import argparse
import scheduler

_reg = []

def expose(cls):
    _reg.append(cls)
    return cls

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--local-scheduler', help = 'Use local scheduling', action='store_true')
    
    subparsers = parser.add_subparsers()

    tasks = []

    def add_obj(cls, params, args):
        kwargs = {}
        args = vars(args)
        for param_name, param in params:
            if args[param_name] == None: arg = param.default
            else: arg = param.parse(args[param_name])

            kwargs[param_name] = arg

        task = cls(**kwargs)
        tasks.append(task)

    for cls in _reg:
        subparser = subparsers.add_parser(cls.__name__)

        params = cls.get_params()
        for param_name, param in params:
            subparser.add_argument('--' + param_name.replace('_', '-'))

        subparser.set_defaults(func = lambda args: add_obj(cls, params, args))

    args = parser.parse_args()
    args.func(args)

    if args.local_scheduler: s = scheduler.LocalScheduler()
    else: s = scheduler.RemoteScheduler()

    for task in tasks:
        s.add(task)
    
    s.run()
