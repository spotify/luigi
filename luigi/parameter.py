# Copyright (c) 2012 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import datetime

_no_default = object()


class ParameterException(Exception):
    pass


class MissingParameterException(ParameterException):
    pass


class UnknownParameterException(ParameterException):
    pass


class DuplicateParameterException(ParameterException):
    pass


class Parameter(object):
    counter = 0

    def __init__(self, default=_no_default, is_list=False, is_boolean=False, is_global=False, significant=True):
        # The default default is no default
        self.__default = default  # We also use this to store global values
        self.is_list = is_list
        self.is_boolean = is_boolean and not is_list  # Only BooleanParameter should ever use this. TODO(erikbern): should we raise some kind of exception?
        self.is_global = is_global  # It just means that the default value is exposed and you can override it
        self.significant = significant
        if is_global and default == _no_default:
            raise ParameterException('Global parameters need default values')
        self.counter = Parameter.counter  # We need to keep track of this to get the order right (see Task class)
        Parameter.counter += 1

    @property
    def has_default(self):
        return self.__default != _no_default

    @property
    def default(self):
        assert self.__default != _no_default  # TODO: exception
        return self.__default

    def set_default(self, value):
        self.__default = value

    def parse(self, x):
        return x  # default impl

    def parse_from_input(self, param_name, x):
        if not x:
            if self.has_default:
                return self.default
            elif self.is_boolean:
                return False
            elif self.is_list:
                return []
            else:
                raise MissingParameterException("No value for '%s' submitted and no default value has been assigned." % param_name)
        elif self.is_list:
            return tuple(self.parse(p) for p in x)
        else:
            return self.parse(x)


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
    def __init__(self, *args, **kwargs):
        super(BooleanParameter, self).__init__(*args, is_boolean=True, **kwargs)

    def parse(self, s):
        return {'true': True, 'false': False}[str(s).lower()]


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
