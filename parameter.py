_no_default = object()

class Parameter(object):
    counter = 0

    class Date:
        def parse(self, s):
            return datetime.date(*map(int, s.split('-')))

    class Int:
        def parse(self, s):
            return int(s)

    def __init__(self, default = _no_default, parser = None):
        # The default default is no default
        self.__default = default

        # We need to keep track of this to get the order right (see Task class)
        self.counter = Parameter.counter
        Parameter.counter += 1

        # Handles input/output
        self.__parser = parser

    @property
    def default(self):
        assert self.__default != _no_default # TODO: exception
        return self.__default

    def parse(self, x):
        if self.__parser == None: return x
        else: return self.__parser.parse(x)
