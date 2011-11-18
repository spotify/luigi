import argparse
from scheduler import RemoteScheduler

_reg = []

def expose(cls):
    _reg.append(cls)
    return cls

def run():
    parser = argparse.ArgumentParser()
    
    subparsers = parser.add_subparsers()

    s = RemoteScheduler()

    def add_obj(cls, params, args):
        kwargs = {}
        args = vars(args)
        for param_name, param in params:
            kwargs[param_name] = param.parse(args[param_name])

        rule = cls(**kwargs)
        s.add(rule)

    for cls in _reg:
        subparser = subparsers.add_parser(cls.__name__)

        params = cls.get_params()
        for param_name, param in params:
            subparser.add_argument('--' + param_name.replace('_', '-'))

        subparser.set_defaults(func = lambda args: add_obj(cls, params, args))

    args = parser.parse_args()
    args.func(args)
    
    s.run()
