import datetime

_no_default = object()


class Parameter(object):
    counter = 0

    def __init__(self, default=_no_default, parser=None):
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
        assert self.__default != _no_default  # TODO: exception
        return self.__default

    def parse(self, x):
        return x  # default impl


class DateHourParameter(Parameter):
    def parse(self, s):
        # TODO(erikbern): we should probably use an internal class for arbitary
        # time intervals (similar to date_interval). Or what do you think?
        return datetime.datetime.strptime(s, "%Y-%m-%dT%H")  # ISO 8601 is to use 'T'


class DateParameter(Parameter):
    def parse(self, s):
        return datetime.date(*map(int, s.split('-')))


class IntParameter(Parameter):
    def parse(self, s):
        return int(s)


class BooleanParameter(Parameter):
    # TODO: the command line interaction is not perfect here.
    # Ideally we want this to be exposed using a store_true attribute so that
    # default is False and flag presence sets it to True
    def parse(self, s):
        return {'true': True, 'false': False}[s.lower()]


class DateIntervalParameter(Parameter):
    # Class that maps to/from dates using ISO 8601 standard
    # Also gives some helpful interval algebra

    def parse(self, s):
        # TODO: can we use xml.utils.iso8601 or something similar?

        import date_interval as d

        for cls in [d.Year, d.Month, d.Week, d.Date, d.Custom]:
            i = cls.parse(s)
            if i:
                return i
        else:
            raise ValueError('Invalid date interval - could not be parsed')
