import target, task, parameter

Parameter = parameter.Parameter

class Rule(object):
    # Something like this...

    def __get_params(self):
        # Extract all Argument instances from the class
        params = []
        for param_name in dir(self.__class__):
            param = getattr(self.__class__, param_name)
            if not isinstance(param, Parameter): continue
            
            params.append((param_name, param))

        params.sort(key = lambda t: t[1].counter)
        return params
    
    def __init__(self, *args, **kwargs):
        params = self.__get_params()
        
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
                result[param_name] = result.default

        for key, value in result.iteritems():
            setattr(self, key, value)

        self.__params = tuple(result.iteritems())

    def __hash__(self):
        return hash(self.__params)

    def exists(self):
        outputs = self.output()
        if not isinstance(outputs, list): outputs = [outputs]
        for output in outputs:
            if not output.exists(): return False
        else:
            return True

    def output(self):
        pass # default impl

    def input(self):
        pass # default impl

    def run(self):
        pass # default impl
