_no_default = object()

class Parameter(object):
    counter = 0

    def __init__(self, default = _no_default, parser = None):
        # The default default is no default
        self.__default = default

        # We need to keep track of this to get the order right (see Task class)
        self.counter = Parameter.counter
        Parameter.counter += 1

        # Handles input/output
        self.__parser = parser

    @property
    def has_default(self):
        return self.__default != _no_default

    @property
    def default(self):
        assert self.__default != _no_default # TODO: exception
        return self.__default

    def parse(self, x):
        return x # default impl

class DateParameter(Parameter):
    def parse(self, s):
        import datetime
        return datetime.date(*map(int, s.split('-')))
    
class IntParameter(Parameter):
    def parse(self, s):
        return int(s)
