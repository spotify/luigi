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

import configuration
import datetime
from ConfigParser import NoSectionError, NoOptionError

_no_default = object()


class ParameterException(Exception):
    pass


class MissingParameterException(ParameterException):
    pass


class UnknownParameterException(ParameterException):
    pass


class DuplicateParameterException(ParameterException):
    pass


class UnknownConfigException(Exception):
  pass


class Parameter(object):
    counter = 0

    def __init__(self, default=_no_default, is_list=False, is_boolean=False, is_global=False, significant=True, description=None,
                 default_from_config=None):
        # The default default is no default
        self.__default = default  # We also use this to store global values
        self.is_list = is_list
        self.is_boolean = is_boolean and not is_list  # Only BooleanParameter should ever use this. TODO(erikbern): should we raise some kind of exception?
        self.is_global = is_global  # It just means that the default value is exposed and you can override it
        self.significant = significant
        if is_global and default == _no_default and default_from_config is None:
            raise ParameterException('Global parameters need default values')
        self.description = description

        if default != _no_default and default_from_config is not None:
            raise ParameterException('Can only specify either a default or a default_from_config')
        if default_from_config is not None and (not 'section' in default_from_config or not 'name' in default_from_config):
            raise ParameterException('default_from_config must be a hash containing entries for section and name')
        self.default_from_config = default_from_config

        self.counter = Parameter.counter  # We need to keep track of this to get the order right (see Task class)
        Parameter.counter += 1

    def _get_default_from_config(self, safe):
        """Loads the default from the config. If safe=True, then returns None if missing. Otherwise,
           raises an UnknownConfigException."""

        conf = configuration.get_config()
        (section, name) = (self.default_from_config['section'], self.default_from_config['name'])
        try:
            return conf.get(section, name)
        except (NoSectionError, NoOptionError), e:
            if safe:
                return None
            raise UnknownConfigException("Couldn't find value for section={0} name={1}. Search config files: '{2}'".format(
                section, name, ", ".join(conf._config_paths)), e)

    @property
    def has_default(self):
        """True if a default was specified or if default_from_config references a valid entry in the conf."""
        if self.default_from_config is not None:
            return self._get_default_from_config(safe=True) is not None
        return self.__default != _no_default

    @property
    def default(self):
        if self.__default == _no_default and self.default_from_config is None:
            raise MissingParameterException("No default specified")
        if self.__default != _no_default:
            return self.__default
        return self.parse(self._get_default_from_config(safe=False))

    def set_default(self, value):
        self.__default = value

    def parse(self, x):
        return x  # default impl

    def serialize(self, x): # opposite of parse
        return str(x)

    def parse_from_input(self, param_name, x):
        if not x:
            if self.has_default:
                return self.default
            elif self.is_boolean:
                return False
            elif self.is_list:
                return []
            else:
                raise MissingParameterException("No value for '%s' (%s) submitted and no default value has been assigned." % \
                    (param_name, "--" + param_name.replace('_', '-')))
        elif self.is_list:
            return tuple(self.parse(p) for p in x)
        else:
            return self.parse(x)


class DateHourParameter(Parameter):
    def parse(self, s):
        # TODO(erikbern): we should probably use an internal class for arbitary
        # time intervals (similar to date_interval). Or what do you think?
        return datetime.datetime.strptime(s, "%Y-%m-%dT%H")  # ISO 8601 is to use 'T'

    def serialize(self, dt):
        return dt.strftime('%Y-%m-%dT%H')


class DateParameter(Parameter):
    def parse(self, s):
        return datetime.date(*map(int, s.split('-')))


class IntParameter(Parameter):
    def parse(self, s):
        return int(s)

class FloatParameter(Parameter):
    def parse(self, s):
        return float(s)

class BooleanParameter(Parameter):
    # TODO(erikbern): why do we call this "boolean" instead of "bool"?
    # The integer parameter is called "int" so calling this "bool" would be
    # more consistent, especially given the Python type names.
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
