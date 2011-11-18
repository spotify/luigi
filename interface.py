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

    rules = []

    def add_obj(cls, params, args):
        kwargs = {}
        args = vars(args)
        for param_name, param in params:
            kwargs[param_name] = param.parse(args[param_name])

        rule = cls(**kwargs)
        rules.append(rule)

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

    for rule in rules:
        s.add(rule)
    
    s.run()
