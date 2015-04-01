# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

''' Parameters are one of the core concepts of Luigi.
All Parameters sit on :class:`~luigi.task.Task` classes.
See :ref:`Parameter` for more info on how to define parameters.
'''

import datetime
import warnings
try:
    from ConfigParser import NoOptionError, NoSectionError
except ImportError:
    from configparser import NoOptionError, NoSectionError

from luigi import six

from luigi import configuration
from luigi.deprecate_kwarg import deprecate_kwarg

_no_value = object()


class ParameterException(Exception):
    """
    Base exception.
    """
    pass


class MissingParameterException(ParameterException):
    """
    Exception signifying that there was a missing Parameter.
    """
    pass


class UnknownParameterException(ParameterException):
    """
    Exception signifying that an unknown Parameter was supplied.
    """
    pass


class DuplicateParameterException(ParameterException):
    """
    Exception signifying that a Parameter was specified multiple times.
    """
    pass


class UnknownConfigException(ParameterException):
    """
    Exception signifying that the ``config_path`` for the Parameter could not be found.
    """
    pass


class Parameter(object):
    """
    An untyped Parameter

    Parameters are objects set on the Task class level to make it possible to parameterize tasks.
    For instance:

        class MyTask(luigi.Task):
            foo = luigi.Parameter()

    This makes it possible to instantiate multiple tasks, eg ``MyTask(foo='bar')`` and
    ``My(foo='baz')``. The task will then have the ``foo`` attribute set appropriately.

    There are subclasses of ``Parameter`` that define what type the parameter has. This is not
    enforced within Python, but are used for command line interaction.

    The ``config_path`` argument lets you specify a place where the parameter is read from config
    in case no value is provided.

    When a task is instantiated, it will first use any argument as the value of the parameter, eg.
    if you instantiate a = TaskA(x=44) then a.x == 44. If this does not exist, it will use the value
    of the Parameter object, which is defined on a class level. This will be resolved in this
    order of falling priority:

    * Any value provided on the command line on the class level (eg. ``--TaskA-param xyz``)
    * Any value provided via config (using the ``config_path`` argument)
    * Any default value set using the ``default`` flag.
    """
    counter = 0
    """non-atomically increasing counter used for ordering parameters."""

    @deprecate_kwarg('is_boolean', 'is_bool', False)
    def __init__(self, default=_no_value, is_list=False, is_boolean=False, is_global=False, significant=True, description=None,
                 config_path=None):
        """
        :param default: the default value for this parameter. This should match the type of the
                        Parameter, i.e. ``datetime.date`` for ``DateParameter`` or ``int`` for
                        ``IntParameter``. By default, no default is stored and
                        the value must be specified at runtime.
        :param bool is_list: specify ``True`` if the parameter should allow a list of values rather
                             than a single value. Default: ``False``. A list has an implicit default
                             value of ``[]``.
        :param bool is_bool: specify ``True`` if the parameter is a bool value. Default:
                                ``False``. Bool's have an implicit default value of ``False``.
        :param bool is_global: specify ``True`` if the parameter is global (i.e. used by multiple
                               Tasks). Default: ``False``. DEPRECATED.
        :param bool significant: specify ``False`` if the parameter should not be treated as part of
                                 the unique identifier for a Task. An insignificant Parameter might
                                 also be used to specify a password or other sensitive information
                                 that should not be made public via the scheduler. Default:
                                 ``True``.
        :param str description: A human-readable string describing the purpose of this Parameter.
                                For command-line invocations, this will be used as the `help` string
                                shown to users. Default: ``None``.
        :param dict config_path: a dictionary with entries ``section`` and ``name``
                                 specifying a config file entry from which to read the
                                 default value for this parameter. DEPRECATED.
                                 Default: ``None``.
        """
        # The default default is no default
        self.__default = default
        self.__global = _no_value

        self.is_list = is_list
        self.is_bool = is_boolean and not is_list  # Only BoolParameter should ever use this. TODO(erikbern): should we raise some kind of exception?
        self.is_global = is_global  # It just means that the default value is exposed and you can override it
        self.significant = significant  # Whether different values for this parameter will differentiate otherwise equal tasks

        if is_global:
            warnings.warn(
                'is_global is deprecated and will be removed. Please use either '
                ' (a) class level config (eg. --MyTask-my-param 42)'
                ' (b) a separate Config class with global settings on it',
                DeprecationWarning,
                stacklevel=2)

        if is_global and default == _no_value and config_path is None:
            raise ParameterException('Global parameters need default values')

        self.description = description

        if config_path is not None and ('section' not in config_path or 'name' not in config_path):
            raise ParameterException('config_path must be a hash containing entries for section and name')
        self.__config = config_path

        self.counter = Parameter.counter  # We need to keep track of this to get the order right (see Task class)
        Parameter.counter += 1

    def _get_value_from_config(self, section, name):
        """Loads the default from the config. Returns _no_value if it doesn't exist"""

        conf = configuration.get_config()

        try:
            value = conf.get(section, name)
        except (NoSectionError, NoOptionError):
            return _no_value

        if self.is_list:
            return tuple(self.parse(p.strip()) for p in value.strip().split('\n'))
        else:
            return self.parse(value)

    def _get_value(self, task_name=None, param_name=None):
        if self.__global != _no_value:
            return self.__global
        if task_name and param_name:
            v = self._get_value_from_config(task_name, param_name)
            if v != _no_value:
                return v
            v = self._get_value_from_config(task_name, param_name.replace('_', '-'))
            if v != _no_value:
                warnings.warn(
                    'The use of the configuration [%s] %s (with dashes) should be avoided. Please use underscores.' %
                    (task_name, param_name), DeprecationWarning, stacklevel=2)
                return v
        if self.__config:
            v = self._get_value_from_config(self.__config['section'], self.__config['name'])
            if v != _no_value and task_name and param_name:
                warnings.warn(
                    'The use of the configuration [%s] %s is deprecated. Please use [%s] %s' %
                    (self.__config['section'], self.__config['name'], task_name, param_name),
                    DeprecationWarning, stacklevel=2)
            if v != _no_value:
                return v
        if self.__default != _no_value:
            return self.__default

        return _no_value

    @property
    def has_value(self):
        """
        ``True`` if a default was specified or if config_path references a valid entry in the conf.

        Note that "value" refers to the Parameter object itself - it can be either

        1. The default value for this parameter
        2. A value read from the config
        3. A global value

        Any Task instance can have its own value set that overrides this.
        """
        return self._get_value() != _no_value

    @property
    def value(self):
        """
        The value for this Parameter.

        This refers to any value defined by a default, a config option, or
        a global value.

        :raises MissingParameterException: if a value is not set.
        :return: the parsed value.
        """
        value = self._get_value()
        if value == _no_value:
            raise MissingParameterException("No default specified")
        else:
            return value

    def has_task_value(self, task_name, param_name):
        return self._get_value(task_name, param_name) != _no_value

    def task_value(self, task_name, param_name):
        value = self._get_value(task_name, param_name)
        if value == _no_value:
            raise MissingParameterException("No default specified")
        else:
            return value

    def set_global(self, value):
        """
        Set the global value of this Parameter.

        :param value: the new global value.
        """
        self.__global = value

    def reset_global(self):
        self.__global = _no_value

    def parse(self, x):
        """
        Parse an individual value from the input.

        The default implementation is an identify (it returns ``x``), but subclasses should override
        this method for specialized parsing. This method is called by :py:meth:`parse_from_input`
        if ``x`` exists. If this Parameter was specified with ``is_list=True``, then ``parse`` is
        called once for each item in the list.

        :param str x: the value to parse.
        :return: the parsed value.
        """
        return x  # default impl

    def serialize(self, x):  # opposite of parse
        """
        Opposite of :py:meth:`parse`.

        Converts the value ``x`` to a string.

        :param x: the value to serialize.
        """
        if self.is_list:
            return [str(v) for v in x]
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
            if self.has_value:
                return self.value
            elif self.is_bool:
                return False
            elif self.is_list:
                return []
            else:
                raise MissingParameterException("No value for '%s' (%s) submitted and no default value has been assigned." %
                                                (param_name, "--" + param_name.replace('_', '-')))
        elif self.is_list:
            return tuple(self.parse(p) for p in x)
        else:
            return self.parse(x)

    def serialize_to_input(self, x):
        if self.is_list:
            return tuple(self.serialize(p) for p in x)
        else:
            return self.serialize(x)

    def parser_dest(self, param_name, task_name, glob=False, is_without_section=False):
        if self.is_global or is_without_section:
            if glob:
                return param_name
            else:
                return None
        else:
            if glob:
                return task_name + '_' + param_name
            else:
                return param_name

    def add_to_cmdline_parser(self, parser, param_name, task_name, optparse=False, glob=False, is_without_section=False):
        dest = self.parser_dest(param_name, task_name, glob, is_without_section=is_without_section)
        if not dest:
            return
        flag = '--' + dest.replace('_', '-')

        description = []
        description.append('%s.%s' % (task_name, param_name))
        if glob:
            description.append('for all instances of class %s' % task_name)
        elif self.description:
            description.append(self.description)
        if self.has_value:
            description.append(" [default: %s]" % (self.value,))

        if self.is_list:
            action = "append"
        elif self.is_bool:
            action = "store_true"
        else:
            action = "store"
        if optparse:
            f = parser.add_option
        else:
            f = parser.add_argument
        f(flag,
          help=' '.join(description),
          action=action,
          dest=dest)

    def parse_from_args(self, param_name, task_name, args, params):
        # Note: modifies arguments
        dest = self.parser_dest(param_name, task_name, glob=False)
        if dest is not None:
            value = getattr(args, dest, None)
            params[param_name] = self.parse_from_input(param_name, value)

    def set_global_from_args(self, param_name, task_name, args, is_without_section=False):
        # Note: side effects
        dest = self.parser_dest(param_name, task_name, glob=True, is_without_section=is_without_section)
        if dest is not None:
            value = getattr(args, dest, None)
            if value:
                self.set_global(self.parse_from_input(param_name, value))
            else:  # either False (bools) or None (everything else)
                self.reset_global()


class DateHourParameter(Parameter):
    """
    Parameter whose value is a :py:class:`~datetime.datetime` specified to the hour.

    A DateHourParameter is a `ISO 8601 <http://en.wikipedia.org/wiki/ISO_8601>`_ formatted
    date and time specified to the hour. For example, ``2013-07-10T19`` specifies July 10, 2013 at
    19:00.
    """

    date_format = '%Y-%m-%dT%H'  # ISO 8601 is to use 'T'

    def parse(self, s):
        """
        Parses a string to a :py:class:`~datetime.datetime` using the format string ``%Y-%m-%dT%H``.
        """
        # TODO(erikbern): we should probably use an internal class for arbitary
        # time intervals (similar to date_interval). Or what do you think?
        return datetime.datetime.strptime(s, self.date_format)

    def serialize(self, dt):
        """
        Converts the datetime to a string usnig the format string ``%Y-%m-%dT%H``.
        """
        if dt is None:
            return str(dt)
        return dt.strftime(self.date_format)


class DateMinuteParameter(DateHourParameter):
    """
    Parameter whose value is a :py:class:`~datetime.datetime` specified to the minute.

    A DateMinuteParameter is a `ISO 8601 <http://en.wikipedia.org/wiki/ISO_8601>`_ formatted
    date and time specified to the minute. For example, ``2013-07-10T19H07`` specifies July 10, 2013 at
    19:07.
    """

    date_format = '%Y-%m-%dT%HH%M'  # ISO 8601 is to use 'T' and 'H'


class DateParameter(Parameter):
    """
    Parameter whose value is a :py:class:`~datetime.date`.

    A DateParameter is a Date string formatted ``YYYY-MM-DD``. For example, ``2013-07-10`` specifies
    July 10, 2013.
    """

    def parse(self, s):
        """Parses a date string formatted as ``YYYY-MM-DD``."""
        return datetime.date(*map(int, s.split('-')))


class IntParameter(Parameter):
    """
    Parameter whose value is an ``int``.
    """

    def parse(self, s):
        """
        Parses an ``int`` from the string using ``int()``.
        """
        return int(s)


class FloatParameter(Parameter):
    """
    Parameter whose value is a ``float``.
    """

    def parse(self, s):
        """
        Parses a ``float`` from the string using ``float()``.
        """
        return float(s)


class BoolParameter(Parameter):
    """
    A Parameter whose value is a ``bool``.
    """

    def __init__(self, *args, **kwargs):
        """
        This constructor passes along args and kwargs to ctor for :py:class:`Parameter` but
        specifies ``is_bool=True``.
        """
        super(BoolParameter, self).__init__(*args, is_bool=True, **kwargs)

    def parse(self, s):
        """
        Parses a ``bool`` from the string, matching 'true' or 'false' ignoring case.
        """
        return {'true': True, 'false': False}[str(s).lower()]


class BooleanParameter(BoolParameter):

    def __init__(self, *args, **kwargs):
        warnings.warn(
            'BooleanParameter is deprecated, use BoolParameter instead',
            DeprecationWarning,
            stacklevel=2
        )
        super(BooleanParameter, self).__init__(*args, **kwargs)


class DateIntervalParameter(Parameter):
    """
    A Parameter whose value is a :py:class:`~luigi.date_interval.DateInterval`.

    Date Intervals are specified using the ISO 8601 `Time Interval
    <http://en.wikipedia.org/wiki/ISO_8601#Time_intervals>`_ notation.
    """
    # Class that maps to/from dates using ISO 8601 standard
    # Also gives some helpful interval algebra

    def parse(self, s):
        """
        Parses a `:py:class:`~luigi.date_interval.DateInterval` from the input.

        see :py:mod:`luigi.date_interval`
          for details on the parsing of DateIntervals.
        """
        # TODO: can we use xml.utils.iso8601 or something similar?

        from luigi import date_interval as d

        for cls in [d.Year, d.Month, d.Week, d.Date, d.Custom]:
            i = cls.parse(s)
            if i:
                return i
        else:
            raise ValueError('Invalid date interval - could not be parsed')


class TimeDeltaParameter(Parameter):
    """
    Class that maps to timedelta using strings in any of the following forms:

     * ``n {w[eek[s]]|d[ay[s]]|h[our[s]]|m[inute[s]|s[second[s]]}`` (e.g. "1 week 2 days" or "1 h")
        Note: multiple arguments must be supplied in longest to shortest unit order
     * ISO 8601 duration ``PnDTnHnMnS`` (each field optional, years and months not supported)
     * ISO 8601 duration ``PnW``

    See https://en.wikipedia.org/wiki/ISO_8601#Durations
    """

    def _apply_regex(self, regex, input):
        from datetime import timedelta
        import re
        re_match = re.match(regex, input)
        if re_match:
            kwargs = {}
            has_val = False
            for k, v in six.iteritems(re_match.groupdict(default="0")):
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
        return self._apply_regex(regex, input)

    def _parseSimple(self, input):
        keys = ["weeks", "days", "hours", "minutes", "seconds"]
        # Give the digits a regex group name from the keys, then look for text with the first letter of the key,
        # optionally followed by the rest of the word, with final char (the "s") optional
        regex = "".join(["((?P<%s>\d+) ?%s(%s)?(%s)? ?)?" % (k, k[0], k[1:-1], k[-1]) for k in keys])
        return self._apply_regex(regex, input)

    def parse(self, input):
        """
        Parses a time delta from the input.

        See :py:class:`TimeDeltaParameter` for details on supported formats.
        """
        result = self._parseIso8601(input)
        if not result:
            result = self._parseSimple(input)
        if result:
            return result
        else:
            raise ParameterException("Invalid time delta - could not parse %s" % input)
