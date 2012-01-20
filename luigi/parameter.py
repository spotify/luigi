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

class BooleanParameter(Parameter):
    # TODO: the command line interaction is not perfect here.
    # Ideally we want this to be exposed using a store_true attribute so that
    # default is False and flag presence sets it to True
    def parse(self, s):
        return {'true': True, 'false': False}[s.lower()]

class DateIntervalParameter(Parameter):
    # Class that maps to/from dates using ISO 8601 standard
    # Also gives some helpful interval algebra

    class DateInterval(object):
        def __init__(self, date_a, date_b):
            # Represents all date d such that date_a <= d < date_b
            self.date_a = date_a
            self.date_b = date_b

        def dates(self):
            import datetime
            dates = []
            d = self.date_a
            while d < self.date_b:
                dates.append(d)
                d += datetime.timedelta(1)

            return dates

        # TODO: subclass for Date, Week, Month, Year, CustomInterval...
        # TODO: method for next, prev (will depend on class not just date_a and date_b)
        # TODO: method for converting back to string
        # TODO: move to date_interval.py?

    def parse(self, s):
        # TODO: can we use xml.utils.iso8601 or something similar?
        # TODO: only do the parsing here, then do any logic in the constructor of each class

        import re, datetime

        if re.match(r'\d\d\d\d\-\d\d\-\d\d$', s): # Date
            date_a = datetime.date(*map(int, s.split('-')))
            date_b = date_a + datetime.timedelta(1)

        elif re.match(r'\d\d\d\d$', s): # Year
            date_a = datetime.date(int(s), 1, 1)
            date_b = datetime.date(int(s) + 1, 1, 1)

        elif re.match(r'\d\d\d\d\-\d\d$', s): # Month
            y, m = map(int, s.split('-'))
            date_a = datetime.date(y, m, 1)
            date_b = datetime.date(y + m/12, 1 + m%12, 1)
            
        elif re.match(r'\d\d\d\d\-W\d\d$', s): # Week
            y, w = map(int, s.split('-W'))
            # Python datetime does not have a method to convert from ISO weeks!
            for d in xrange(-10, 370):
                date = datetime.date(y, 1, 1) + datetime.timedelta(d)
                if date.isocalendar() == (y, w, 1):
                    date_a = date
                    break
            else:
                raise ValueError('Invalid week')
            date_b = date_a + datetime.timedelta(7)

        elif re.match('\d\d\d\d\-\d\d\-\d\d\-\d\d\d\d\-\d\d\-\d\d$', s): # Custom interval
            # Actually the ISO 8601 specifies <start>/<end> as the time interval format
            # Not sure if this goes for date intervals as well. In any case slashes will
            # most likely cause problems with paths etc.
            x = map(int, s.split('-'))
            date_a = datetime.date(*x[:3])
            date_b = datetime.date(*x[3:])

        else:
            raise ValueError('Invalid date interval')

        return DateIntervalParameter.DateInterval(date_a, date_b)
            
