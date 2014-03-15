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
    """Base exception."""
    pass


class MissingParameterException(ParameterException):
    """Exception signifying that there was a missing Parameter."""
    pass


class UnknownParameterException(ParameterException):
    """Exception signifying that an unknown Parameter was supplied."""
    pass


class DuplicateParameterException(ParameterException):
    """Exception signifying that a Parameter was specified multiple times."""
    pass


class UnknownConfigException(Exception):
    """Exception signifying that the ``default_from_config`` for the Parameter could not be found."""
    pass


class Parameter(object):
    """An untyped Parameter"""
    counter = 0
    """non-atomically increasing counter used for ordering parameters."""

    def __init__(self, default=_no_default, is_list=False, is_boolean=False, is_global=False, significant=True, description=None,
                 default_from_config=None):
        """
        :param default: the default value for this parameter. This should match the type of the
                        Parameter, i.e. ``datetime.date`` for ``DateParameter`` or ``int`` for
                        ``IntParameter``. You may only specify either ``default`` or
                        ``default_from_config`` and not both. By default, no default is stored and
                        the value must be specified at runtime.
        :param bool is_list: specify ``True`` if the parameter should allow a list of values rather
                             than a single value. Default: ``False``. A list has an implicit default
                             value of ``[]``.
        :param bool is_boolean: specify ``True`` if the parameter is a boolean value. Default:
                                ``False``. Boolean's have an implicit default value of ``False``.
        :param bool is_global: specify ``True`` if the parameter is global (i.e. used by multiple
                               Tasks). Default: ``False``.
        :param bool significant: specify ``False`` if the parameter should not be treated as part of
                                 the unique identifier for a Task. An insignificant Parameter might
                                 also be used to specify a password or other sensitive information
                                 that should not be made public via the scheduler. Default:
                                 ``True``.
        :param str description: A human-readable string describing the purpose of this Parameter.
                                For command-line invocations, this will be used as the `help` string
                                shown to users. Default: ``None``.
        :param dict default_from_config: a dictionary with entries ``section`` and ``name``
                                         specifying a config file entry from which to read the
                                         default value for this parameter. You may only specify
                                         either ``default`` or ``default_from_config`` and not both.
                                         Default: ``None``.
        """
        # The default default is no default
        self.__default = default  # We also use this to store global values
        self.is_list = is_list
        self.is_boolean = is_boolean and not is_list  # Only BooleanParameter should ever use this. TODO(erikbern): should we raise some kind of exception?
        self.is_global = is_global  # It just means that the default value is exposed and you can override it
        self.significant = significant # Whether different values for this parameter will differentiate otherwise equal tasks
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
        """``True`` if a default was specified or if default_from_config references a valid entry in the conf."""
        if self.default_from_config is not None:
            return self._get_default_from_config(safe=True) is not None
        return self.__default != _no_default

    @property
    def default(self):
        """The default value for this Parameter.

        :raises MissingParameterException: if a default is not set.
        :return: the parsed default value.
        """
        if self.__default == _no_default and self.default_from_config is None:
            raise MissingParameterException("No default specified")
        if self.__default != _no_default:
            return self.__default

        value = self._get_default_from_config(safe=False)
        if self.is_list:
            return tuple(self.parse(p.strip()) for p in value.strip().split('\n'))
        else:
            return self.parse(value)

    def set_default(self, value):
        """Set the default value of this Parameter.

        :param value: the new default value.
        """
        self.__default = value

    def parse(self, x):
        """Parse an individual value from the input.

        The default implementation is an identify (it returns ``x``), but subclasses should override
        this method for specialized parsing. This method is called by :py:meth:`parse_from_input`
        if ``x`` exists. If this Parameter was specified with ``is_list=True``, then ``parse`` is
        called once for each item in the list.

        :param str x: the value to parse.
        :return: the parsed value.
        """
        return x  # default impl

    def serialize(self, x): # opposite of parse
        """Opposite of :py:meth:`parse`.

        Converts the value ``x`` to a string.

        :param x: the value to serialize.
        """
        return str(x)

    def parse_from_input(self, param_name, x):
        """
        Parses the parameter value from input ``x``, handling defaults and is_list.

        :param param_name: the name of the parameter. This is used for the message in
                           ``MissingParameterException``.
        :param x: the input value to parse.
        :raises MissingParameterException: if x is false-y and no default is specified.
        """
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
    """Parameter whose value is a :py:class:`~datetime.datetime` specified to the hour.

    A DateHourParameter is a `ISO 8601 <http://en.wikipedia.org/wiki/ISO_8601>`_ formatted
    date and time specified to the hour. For example, ``2013-07-10T19`` specifies July 10, 2013 at
    19:00.
    """

    def parse(self, s):
        """
        Parses a string to a :py:class:`~datetime.datetime` using the format string ``%Y-%m-%dT%H``.
        """
        # TODO(erikbern): we should probably use an internal class for arbitary
        # time intervals (similar to date_interval). Or what do you think?
        return datetime.datetime.strptime(s, "%Y-%m-%dT%H")  # ISO 8601 is to use 'T'

    def serialize(self, dt):
        """
        Converts the datetime to a string usnig the format string ``%Y-%m-%dT%H``.
        """
        if dt is None: return str(dt)
        return dt.strftime('%Y-%m-%dT%H')


class DateParameter(Parameter):
    """Parameter whose value is a :py:class:`~datetime.date`.

    A DateParameter is a Date string formatted ``YYYY-MM-DD``. For example, ``2013-07-10`` specifies
    July 10, 2013.
    """
    def parse(self, s):
        """Parses a date string formatted as ``YYYY-MM-DD``."""
        return datetime.date(*map(int, s.split('-')))


class IntParameter(Parameter):
    """Parameter whose value is an ``int``."""
    def parse(self, s):
        """Parses an ``int`` from the string using ``int()``."""
        return int(s)

class FloatParameter(Parameter):
    """Parameter whose value is a ``float``."""
    def parse(self, s):
        """Parses a ``float`` from the string using ``float()``."""
        return float(s)

class BooleanParameter(Parameter):
    """A Parameter whose value is a ``bool``."""
    # TODO(erikbern): why do we call this "boolean" instead of "bool"?
    # The integer parameter is called "int" so calling this "bool" would be
    # more consistent, especially given the Python type names.
    def __init__(self, *args, **kwargs):
        """This constructor passes along args and kwargs to ctor for :py:class:`Parameter` but
        specifies ``is_boolean=True``.
        """
        super(BooleanParameter, self).__init__(*args, is_boolean=True, **kwargs)

    def parse(self, s):
        """Parses a ``boolean`` from the string, matching 'true' or 'false' ignoring case."""
        return {'true': True, 'false': False}[str(s).lower()]


class DateIntervalParameter(Parameter):
    """A Parameter whose value is a :py:class:`~luigi.date_interval.DateInterval`.

    Date Intervals are specified using the ISO 8601 `Time Interval
    <http://en.wikipedia.org/wiki/ISO_8601#Time_intervals>`_ notation.
    """
    # Class that maps to/from dates using ISO 8601 standard
    # Also gives some helpful interval algebra

    def parse(self, s):
        """Parses a `:py:class:`~luigi.date_interval.DateInterval` from the input.

        see :py:mod:`luigi.date_interval`
          for details on the parsing of DateIntervals.
        """
        # TODO: can we use xml.utils.iso8601 or something similar?

        import date_interval as d

        for cls in [d.Year, d.Month, d.Week, d.Date, d.Custom]:
            i = cls.parse(s)
            if i:
                return i
        else:
            raise ValueError('Invalid date interval - could not be parsed')


class TimeDeltaParameter(Parameter):
    """Class that maps to timedelta using strings in any of the following forms:

     - ``n {w[eek[s]]|d[ay[s]]|h[our[s]]|m[inute[s]|s[second[s]]}`` (e.g. "1 week 2 days" or "1 h")
        Note: multiple arguments must be supplied in longest to shortest unit order
     - ISO 8601 duration ``PnDTnHnMnS`` (each field optional, years and months not supported)
     - ISO 8601 duration ``PnW``

    See https://en.wikipedia.org/wiki/ISO_8601#Durations
    """

    def _apply_regex(self, regex, input):
        from datetime import timedelta
        import re
        re_match = re.match(regex, input)
        if re_match:
            kwargs = {}
            has_val = False
            for k,v in re_match.groupdict(default="0").items():
                val = int(v)
                has_val = has_val or val != 0
                kwargs[k] = val
            if has_val:
                return timedelta(**kwargs)

    def _parseIso8601(self, input):
        def field(key):
            return "(?P<%s>\d+)%s" % (key, key[0].upper())
        def optional_field(key):
            return "(%s)?" % field(key)
        # A little loose: ISO 8601 does not allow weeks in combination with other fields, but this regex does (as does python timedelta)
        regex = "P(%s|%s(T%s)?)" % (field("weeks"), optional_field("days"), "".join([optional_field(key) for key in ["hours", "minutes", "seconds"]]))
        return self._apply_regex(regex,input)

    def _parseSimple(self, input):
        keys = ["weeks", "days", "hours", "minutes", "seconds"]
        # Give the digits a regex group name from the keys, then look for text with the first letter of the key,
        # optionally followed by the rest of the word, with final char (the "s") optional
        regex = "".join(["((?P<%s>\d+) ?%s(%s)?(%s)? ?)?" % (k, k[0], k[1:-1], k[-1]) for k in keys])
        return self._apply_regex(regex, input)

    def parse(self, input):
        """Parses a time delta from the input.

        See :py:class:`TimeDeltaParameter` for details on supported formats.
        """
        result = self._parseIso8601(input)
        if not result:
            result = self._parseSimple(input)
        if result:
            return result
        else:
            raise ParameterException("Invalid time delta - could not parse %s" % input)
