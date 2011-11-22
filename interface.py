import argparse
import scheduler

_reg = {}

def expose(cls, main = False):
    name = cls.__name__
    assert name not in _reg
    _reg[name] = cls
    return cls

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--local-scheduler', help = 'Use local scheduling', action='store_true')
    
    subparsers = parser.add_subparsers(dest = 'command')

    for name, cls in _reg.iteritems():
        subparser = subparsers.add_parser(name)

        params = cls.get_params()
        for param_name, param in params:
            subparser.add_argument('--' + param_name.replace('_', '-'))

    args = parser.parse_args()
    parames = vars(args) # convert to a str -> str hash
    task = _reg[args.command].from_input(params)
        
    if args.local_scheduler: s = scheduler.LocalScheduler()
    else: s = scheduler.RemoteScheduler()
    
    s.add(task)
    s.run()
