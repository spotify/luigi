import target, task

class Rule(object):
    # Something like this...

    __insts = {}
    
    def __init__(self, args, kwargs):
        for arg in dir(self.__class__):
            if isinstance(Argument, arg):
                
        self.__task = task.Task(self, args, kwargs)
        self.__task.get_output()

    def add_input(self, input):
        return self.__task.add_input(input)

    def add_output(self, output):
        return self.__task.add_output(output)

    def get_task(self):
        return self.__task

    def get_output(self, *args, **kwargs):
        pass # default impl

    def get_input(self, *args, **kwargs):
        pass # default impl

    def run(self, *args, **kwargs):
        pass # default impl

    @classmethod
    def make(cls, *args, **kwargs):
        # Lookup in cache and return if existing...

        k = (cls, tuple(args), tuple(kwargs.iteritems()))

        if k not in cls.__insts:
            cls.__insts[k] = cls(args, kwargs)
            
        return cls.__insts[k]
