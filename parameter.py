_no_default = object()

class Parameter(object):
    counter = 0

    def __init__(self, default = _no_default):
        self.__default = default # The default default is no default
        self.counter = Parameter.counter # We need to keep track of this to get the order right
        Parameter.counter += 1

    @property
    def default(self):
        assert self.__default != _no_default # TODO: exception
        return self.__default
