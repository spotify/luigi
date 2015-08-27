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

import abc
import datetime
import warnings
try:
    from ConfigParser import NoOptionError, NoSectionError
except ImportError:
    from configparser import NoOptionError, NoSectionError

from luigi import task_register
from luigi import six

from luigi import configuration
from luigi.deprecate_kwarg import deprecate_kwarg
from datetime import timedelta

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
    ``MyTask(foo='baz')``. The task will then have the ``foo`` attribute set appropriately.

    There are subclasses of ``Parameter`` that define what type the parameter has. This is not
    enforced within Python, but are used for command line interaction.

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
    def __init__(self, default=_no_value, is_boolean=False, is_global=False, significant=True, description=None,
                 config_path=None, positional=True):
        """
        :param default: the default value for this parameter. This should match the type of the
                        Parameter, i.e. ``datetime.date`` for ``DateParameter`` or ``int`` for
                        ``IntParameter``. By default, no default is stored and
                        the value must be specified at runtime.
        :param bool is_bool: specify ``True`` if the parameter is a bool value. Default:
                                ``False``. Bool's have an implicit default value of ``False``.
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
        :param bool positional: If true, you can set the argument as a
                                positional argument. Generally we recommend ``positional=False``
                                as positional arguments become very tricky when
                                you have inheritance and whatnot.
        """
        # The default default is no default
        self.__default = default
        self.__global = _no_value

        self.is_bool = is_boolean  # Only BoolParameter should ever use this. TODO(erikbern): should we raise some kind of exception?
        if is_global:
            warnings.warn("is_global support is removed. Assuming positional=False",
                          DeprecationWarning,
                          stacklevel=2)
            positional = False
        self.significant = significant  # Whether different values for this parameter will differentiate otherwise equal tasks
        self.positional = positional

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

        return self.parse(value)

    def _get_value(self, task_name, param_name):
        for value, warn in self._value_iterator(task_name, param_name):
            if value != _no_value:
                if warn:
                    warnings.warn(warn, DeprecationWarning, stacklevel=2)
                return value
        return _no_value

    def _value_iterator(self, task_name, param_name):
        """
        Yield the parameter values, with optional deprecation warning as second tuple value.

        The parameter value will be whatever non-_no_value that is yielded first.
        """
        yield (self.__global, None)
        yield (self._get_value_from_config(task_name, param_name), None)
        yield (self._get_value_from_config(task_name, param_name.replace('_', '-')),
               'Configuration [{}] {} (with dashes) should be avoided. Please use underscores.'.format(
               task_name, param_name))
        if self.__config:
            yield (self._get_value_from_config(self.__config['section'], self.__config['name']),
                   'The use of the configuration [{}] {} is deprecated. Please use [{}] {}'.format(
                   self.__config['section'], self.__config['name'], task_name, param_name))
        yield (self.__default, None)

    def has_task_value(self, task_name, param_name):
        return self._get_value(task_name, param_name) != _no_value

    def task_value(self, task_name, param_name):
        value = self._get_value(task_name, param_name)
        if value == _no_value:
            raise MissingParameterException("No default specified")
        else:
            return value

    def _set_global(self, value):
        """
        Set the global value of this Parameter.

        :param value: the new global value.
        """
        self.__global = value

    def _reset_global(self):
        self.__global = _no_value

    def parse(self, x):
        """
        Parse an individual value from the input.

        The default implementation is an identify (it returns ``x``), but subclasses should override
        this method for specialized parsing. This method is called by :py:meth:`parse_from_input`
        if ``x`` exists.

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
        return str(x)

    @classmethod
    def next_in_enumeration(_cls, _value):
        """
        If your Parameter type has an enumerable ordering of values. You can
        choose to override this method. This method is used by the
        :py:mod:`luigi.execution_summary` module for pretty printing
        purposes. Enabling it to pretty print tasks like ``MyTask(num=1),
        MyTask(num=2), MyTask(num=3)`` to ``MyTask(num=1..3)``.

        :param value: The value
        :return: The next value, like "value + 1". Or ``None`` if there's no enumerable ordering.
        """
        return None

    def parse_from_input(self, param_name, x, task_name=None):
        """
        Parses the parameter value from input ``x``, handling defaults.

        :param param_name: the name of the parameter. This is used for the message in
                           ``MissingParameterException``.
        :param x: the input value to parse.
        :raises MissingParameterException: if x is false-y and no default is specified.
        """
        if not x:
            if self.has_task_value(param_name=param_name, task_name=task_name):
                return self.task_value(param_name=param_name, task_name=task_name)
            elif self.is_bool:
                return False
            else:
                raise MissingParameterException("No value for '%s' (%s) submitted and no default value has been assigned." %
                                                (param_name, "--" + param_name.replace('_', '-')))
        else:
            return self.parse(x)

    def _parser_dest(self, param_name, task_name, glob=False, is_without_section=False):
        if is_without_section:
            if glob:
                return param_name
            else:
                return None
        else:
            if glob:
                return task_name + '_' + param_name
            else:
                return param_name

    def add_to_cmdline_parser(self, parser, param_name, task_name, glob=False, is_without_section=False):
        """
        Internally used from interface.py, this method will probably be removed.
        """
        dest = self._parser_dest(param_name, task_name, glob, is_without_section=is_without_section)
        if not dest:
            return
        flag = '--' + dest.replace('_', '-')

        description = []
        description.append('%s.%s' % (task_name, param_name))
        if glob:
            description.append('for all instances of class %s' % task_name)
        elif self.description:
            description.append(self.description)
        if self.has_task_value(param_name=param_name, task_name=task_name):
            value = self.task_value(param_name=param_name, task_name=task_name)
            description.append(" [default: %s]" % (value,))

        if self.is_bool:
            action = "store_true"
        else:
            action = "store"

        parser.add_argument(flag,
                            help=' '.join(description),
                            action=action,
                            dest=dest)

    def parse_from_args(self, param_name, task_name, args, params):
        """
        Internally used from interface.py, this method will probably be removed.
        """
        # Note: modifies arguments
        dest = self._parser_dest(param_name, task_name, glob=False)
        if dest is not None:
            value = getattr(args, dest, None)
            params[param_name] = self.parse_from_input(param_name, value, task_name=task_name)

    def set_global_from_args(self, param_name, task_name, args, is_without_section=False):
        """
        Internally used from interface.py, this method will probably be removed.
        """
        # Note: side effects
        dest = self._parser_dest(param_name, task_name, glob=True, is_without_section=is_without_section)
        if dest is not None:
            value = getattr(args, dest, None)
            if value:
                self._set_global(self.parse_from_input(param_name, value, task_name=task_name))
            else:  # either False (bools) or None (everything else)
                self._reset_global()


class DateParameterBase(Parameter):
    """
    Base class Parameter for dates. Code reuse is made possible since all date
    parameters are serialized in the same way.
    """
    @abc.abstractproperty
    def date_format(self):
        """
        Override me with a :py:meth:`~datetime.date.strftime` string.
        """
        pass

    @abc.abstractproperty
    def _timedelta(self):
        """
        Either override me with a :py:class:`~datetime.timedelta` value or
        implement :py:meth:`~Parameter.next_in_enumeration` to return ``None``.
        """
        pass

    def serialize(self, dt):
        """
        Converts the date to a string using the :py:attr:`~DateParameterBase.date_format`.
        """
        if dt is None:
            return str(dt)
        return dt.strftime(self.date_format)

    @classmethod
    def next_in_enumeration(cls, value):
        return value + cls._timedelta


class DateParameter(DateParameterBase):
    """
    Parameter whose value is a :py:class:`~datetime.date`.

    A DateParameter is a Date string formatted ``YYYY-MM-DD``. For example, ``2013-07-10`` specifies
    July 10, 2013.
    """

    date_format = '%Y-%m-%d'
    _timedelta = timedelta(days=1)

    def parse(self, s):
        """
        Parses a date string formatted as ``YYYY-MM-DD``.
        """
        return datetime.datetime.strptime(s, self.date_format).date()


class MonthParameter(DateParameter):
    """
    Parameter whose value is a :py:class:`~datetime.date`, specified to the month
    (day of :py:class:`~datetime.date` is "rounded" to first of the month).

    A MonthParameter is a Date string formatted ``YYYY-MM``. For example, ``2013-07`` specifies
    July of 2013.
    """

    date_format = '%Y-%m'

    @staticmethod
    def next_in_enumeration(_value):
        return None


class YearParameter(DateParameter):
    """
    Parameter whose value is a :py:class:`~datetime.date`, specified to the year
    (day and month of :py:class:`~datetime.date` is "rounded" to first day of the year).

    A YearParameter is a Date string formatted ``YYYY``.
    """

    date_format = '%Y'

    @staticmethod
    def next_in_enumeration(_value):
        return None


class DateHourParameter(DateParameterBase):
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
        return datetime.datetime.strptime(s, self.date_format)


class DateMinuteParameter(DateHourParameter):
    """
    Parameter whose value is a :py:class:`~datetime.datetime` specified to the minute.

    A DateMinuteParameter is a `ISO 8601 <http://en.wikipedia.org/wiki/ISO_8601>`_ formatted
    date and time specified to the minute. For example, ``2013-07-10T1907`` specifies July 10, 2013 at
    19:07.
    """

    date_format = '%Y-%m-%dT%H%M'
    _timedelta = timedelta(minutes=1)
    deprecated_date_format = '%Y-%m-%dT%HH%M'

    def parse(self, s):
        try:
            value = datetime.datetime.strptime(s, self.deprecated_date_format)
            warnings.warn(
                'Using "H" between hours and minutes is deprecated, omit it instead.',
                DeprecationWarning,
                stacklevel=2
            )
            return value
        except ValueError:
            return super(DateMinuteParameter, self).parse(s)


class IntParameter(Parameter):
    """
    Parameter whose value is an ``int``.
    """

    def parse(self, s):
        """
        Parses an ``int`` from the string using ``int()``.
        """
        return int(s)

    @staticmethod
    def next_in_enumeration(value):
        return value + 1


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


class TaskParameter(Parameter):
    """
    A parameter that takes another luigi task class.

    When used programatically, the parameter should be specified
    directly with the :py:class:`luigi.task.Task` (sub) class. Like
    ``MyMetaTask(my_task_param=my_tasks.MyTask)``. On the command line,
    you specify the :py:attr:`luigi.task.Task.task_family`. Like

    .. code:: console

            $ luigi --module my_tasks MyMetaTask --my_task_param my_namespace.MyTask

    Where ``my_namespace.MyTask`` is defined in the ``my_tasks`` python module.

    When the :py:class:`luigi.task.Task` class is instantiated to an object.
    The value will always be a task class (and not a string).
    """

    def parse(self, input):
        """
        Parse a task_famly using the :class:`~luigi.task_register.Register`
        """
        return task_register.Register.get_task_cls(input)
