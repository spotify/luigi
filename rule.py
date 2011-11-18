import parameter

Parameter = parameter.Parameter

class Rule(object):
    # Something like this...

    @classmethod
    def get_params(cls):
        # Extract all Argument instances from the class
        params = []
        for param_name in dir(cls):
            param = getattr(cls, param_name)
            if not isinstance(param, Parameter): continue
            
            params.append((param_name, param))

        params.sort(key = lambda t: t[1].counter)
        return params
    
    def __init__(self, *args, **kwargs):
        params = self.get_params()
        
        result = {}

        params_dict = dict(params)

        for i, arg in enumerate(args):
            param_name, param = params[i]
            result[param_name] = arg

        for param_name, arg in kwargs.iteritems():
            assert param_name not in result
            assert param_name in params_dict
            result[param_name] = arg

        for param_name, param in params:
            if param_name not in result:
                result[param_name] = param.default

        for key, value in result.iteritems():
            setattr(self, key, value)

        self.__params = list(result.iteritems())

    def __hash__(self):
        return hash(tuple(self.__params))

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, ', '.join(['%s=%s' % (str(k), str(v)) for k, v in self.__params]))

    def exists(self):
        outputs = flatten(self.output())
        if len(outputs) == 0: return False
        
        for output in outputs:
            if not output.exists():
                return False
        else:
            return True
        
    def output(self):
        return [] # default impl
    
    def requires(self):
        return [] # default impl

    def input(self):
        return getpaths(self.requires())
    
    def run(self):
        pass # default impl

def getpaths(struct):
    """ Maps all Rules in a structured data object to their .output()"""
    if isinstance(struct, Rule):
        return struct.output()
    elif isinstance(struct, dict):
        r = {}
        for k, v in struct.iteritems():
            r[k] = getpaths(v)
        return r
    try:
        # if iterable
        r = []
        for s in struct:
            r.append(getpaths(s))
        return r
    except TypeError:
        pass
    return struct

def flatten(struct):
    """Cleates a flat list of all all items in structured output (dicts, lists, items)
    Examples:
    > _flatten({'a': foo, b: bar})
    [foo, bar]
    > _flatten([foo, [bar, troll]])
    [foo, bar, troll]
    > _flatten(foo)
    [foo]
    """
    flat = []
    if isinstance(struct, dict):
        for key, result in struct.iteritems():
            flat += flatten(result)
        return flat

    try:
        # if iterable
        for result in struct:
            flat += flatten(result)
        return flat
    except TypeError:
        pass

    return [struct]
