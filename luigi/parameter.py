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
import json
from json import JSONEncoder
from collections import OrderedDict, Mapping
import operator
import functools
from ast import literal_eval

try:
    from ConfigParser import NoOptionError, NoSectionError
except ImportError:
    from configparser import NoOptionError, NoSectionError

from luigi import task_register
from luigi import six
from luigi import configuration
from luigi.cmdline_parser import CmdlineParser


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


class Parameter(object):
    """
    Parameter whose value is a ``str``, and a base class for other parameter types.

    Parameters are objects set on the Task class level to make it possible to parameterize tasks.
    For instance:

    .. code:: python

        class MyTask(luigi.Task):
            foo = luigi.Parameter()

        class RequiringTask(luigi.Task):
            def requires(self):
                return MyTask(foo="hello")

            def run(self):
                print(self.requires().foo)  # prints "hello"

    This makes it possible to instantiate multiple tasks, eg ``MyTask(foo='bar')`` and
    ``MyTask(foo='baz')``. The task will then have the ``foo`` attribute set appropriately.

    When a task is instantiated, it will first use any argument as the value of the parameter, eg.
    if you instantiate ``a = TaskA(x=44)`` then ``a.x == 44``. When the value is not provided, the
    value  will be resolved in this order of falling priority:

        * Any value provided on the command line:

          - To the root task (eg. ``--param xyz``)

          - Then to the class, using the qualified task name syntax (eg. ``--TaskA-param xyz``).

        * With ``[TASK_NAME]>PARAM_NAME: <serialized value>`` syntax. See :ref:`ParamConfigIngestion`

        * Any default value set using the ``default`` flag.

    Parameter objects may be reused, but you must then set the ``positional=False`` flag.
    """
    _counter = 0  # non-atomically increasing counter used for ordering parameters.

    def __init__(self, default=_no_value, is_global=False, significant=True, description=None,
                 config_path=None, positional=True, always_in_help=False, batch_method=None):
        """
        :param default: the default value for this parameter. This should match the type of the
                        Parameter, i.e. ``datetime.date`` for ``DateParameter`` or ``int`` for
                        ``IntParameter``. By default, no default is stored and
                        the value must be specified at runtime.
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
                                positional argument. It's true by default but we recommend
                                ``positional=False`` for abstract base classes and similar cases.
        :param bool always_in_help: For the --help option in the command line
                                    parsing. Set true to always show in --help.
        :param function(iterable[A])->A batch_method: Method to combine an iterable of parsed
                                                        parameter values into a single value. Used
                                                        when receiving batched parameter lists from
                                                        the scheduler. See :ref:`batch_method`
        """
        self._default = default
        self._batch_method = batch_method
        if is_global:
            warnings.warn("is_global support is removed. Assuming positional=False",
                          DeprecationWarning,
                          stacklevel=2)
            positional = False
        self.significant = significant  # Whether different values for this parameter will differentiate otherwise equal tasks
        self.positional = positional

        self.description = description
        self.always_in_help = always_in_help

        if config_path is not None and ('section' not in config_path or 'name' not in config_path):
            raise ParameterException('config_path must be a hash containing entries for section and name')
        self._config_path = config_path

        self._counter = Parameter._counter  # We need to keep track of this to get the order right (see Task class)
        Parameter._counter += 1

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
                    warnings.warn(warn, DeprecationWarning)
                return value
        return _no_value

    def _value_iterator(self, task_name, param_name):
        """
        Yield the parameter values, with optional deprecation warning as second tuple value.

        The parameter value will be whatever non-_no_value that is yielded first.
        """
        cp_parser = CmdlineParser.get_instance()
        if cp_parser:
            dest = self._parser_global_dest(param_name, task_name)
            found = getattr(cp_parser.known_args, dest, None)
            yield (self._parse_or_no_value(found), None)
        yield (self._get_value_from_config(task_name, param_name), None)
        yield (self._get_value_from_config(task_name, param_name.replace('_', '-')),
               'Configuration [{}] {} (with dashes) should be avoided. Please use underscores.'.format(
               task_name, param_name))
        if self._config_path:
            yield (self._get_value_from_config(self._config_path['section'], self._config_path['name']),
                   'The use of the configuration [{}] {} is deprecated. Please use [{}] {}'.format(
                   self._config_path['section'], self._config_path['name'], task_name, param_name))
        yield (self._default, None)

    def has_task_value(self, task_name, param_name):
        return self._get_value(task_name, param_name) != _no_value

    def task_value(self, task_name, param_name):
        value = self._get_value(task_name, param_name)
        if value == _no_value:
            raise MissingParameterException("No default specified")
        else:
            return self.normalize(value)

    def _is_batchable(self):
        return self._batch_method is not None

    def parse(self, x):
        """
        Parse an individual value from the input.

        The default implementation is the identity function, but subclasses should override
        this method for specialized parsing.

        :param str x: the value to parse.
        :return: the parsed value.
        """
        return x  # default impl

    def _parse_list(self, xs):
        """
        Parse a list of values from the scheduler.

        Only possible if this is_batchable() is True. This will combine the list into a single
        parameter value using batch method. This should never need to be overridden.

        :param xs: list of values to parse and combine
        :return: the combined parsed values
        """
        if not self._is_batchable():
            raise NotImplementedError('No batch method found')
        elif not xs:
            raise ValueError('Empty parameter list passed to parse_list')
        else:
            return self._batch_method(map(self.parse, xs))

    def serialize(self, x):
        """
        Opposite of :py:meth:`parse`.

        Converts the value ``x`` to a string.

        :param x: the value to serialize.
        """
        return str(x)

    def _warn_on_wrong_param_type(self, param_name, param_value):
        if self.__class__ != Parameter:
            return
        if not isinstance(param_value, six.string_types):
            warnings.warn('Parameter "{}" with value "{}" is not of type string.'.format(param_name, param_value))

    def normalize(self, x):
        """
        Given a parsed parameter value, normalizes it.

        The value can either be the result of parse(), the default value or
        arguments passed into the task's constructor by instantiation.

        This is very implementation defined, but can be used to validate/clamp
        valid values. For example, if you wanted to only accept even integers,
        and "correct" odd values to the nearest integer, you can implement
        normalize as ``x // 2 * 2``.
        """
        return x  # default impl

    def next_in_enumeration(self, _value):
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

    def _parse_or_no_value(self, x):
        if not x:
            return _no_value
        else:
            return self.parse(x)

    @staticmethod
    def _parser_global_dest(param_name, task_name):
        return task_name + '_' + param_name

    @staticmethod
    def _parser_action():
        return "store"


class OptionalParameter(Parameter):
    """ A Parameter that treats empty string as None """

    def serialize(self, x):
        if x is None:
            return ''
        else:
            return str(x)

    def parse(self, x):
        return x or None

    def _warn_on_wrong_param_type(self, param_name, param_value):
        if self.__class__ != OptionalParameter:
            return
        if not isinstance(param_value, six.string_types) and param_value is not None:
            warnings.warn('OptionalParameter "{}" with value "{}" is not of type string or None.'.format(
                param_name, param_value))


_UNIX_EPOCH = datetime.datetime.utcfromtimestamp(0)


class _DateParameterBase(Parameter):
    """
    Base class Parameter for date (not datetime).
    """

    def __init__(self, interval=1, start=None, **kwargs):
        super(_DateParameterBase, self).__init__(**kwargs)
        self.interval = interval
        self.start = start if start is not None else _UNIX_EPOCH.date()

    @abc.abstractproperty
    def date_format(self):
        """
        Override me with a :py:meth:`~datetime.date.strftime` string.
        """
        pass

    def parse(self, s):
        """
        Parses a date string formatted like ``YYYY-MM-DD``.
        """
        return datetime.datetime.strptime(s, self.date_format).date()

    def serialize(self, dt):
        """
        Converts the date to a string using the :py:attr:`~_DateParameterBase.date_format`.
        """
        if dt is None:
            return str(dt)
        return dt.strftime(self.date_format)


class DateParameter(_DateParameterBase):
    """
    Parameter whose value is a :py:class:`~datetime.date`.

    A DateParameter is a Date string formatted ``YYYY-MM-DD``. For example, ``2013-07-10`` specifies
    July 10, 2013.

    DateParameters are 90% of the time used to be interpolated into file system paths or the like.
    Here is a gentle reminder of how to interpolate date parameters into strings:

    .. code:: python

        class MyTask(luigi.Task):
            date = luigi.DateParameter()

            def run(self):
                templated_path = "/my/path/to/my/dataset/{date:%Y/%m/%d}/"
                instantiated_path = templated_path.format(date=self.date)
                # print(instantiated_path) --> /my/path/to/my/dataset/2016/06/09/
                # ... use instantiated_path ...

    To set this parameter to default to the current day. You can write code like this:

    .. code:: python

        import datetime

        class MyTask(luigi.Task):
            date = luigi.DateParameter(default=datetime.date.today())
    """

    date_format = '%Y-%m-%d'

    def next_in_enumeration(self, value):
        return value + datetime.timedelta(days=self.interval)

    def normalize(self, value):
        if value is None:
            return None

        if isinstance(value, datetime.datetime):
            value = value.date()

        delta = (value - self.start).days % self.interval
        return value - datetime.timedelta(days=delta)


class MonthParameter(DateParameter):
    """
    Parameter whose value is a :py:class:`~datetime.date`, specified to the month
    (day of :py:class:`~datetime.date` is "rounded" to first of the month).

    A MonthParameter is a Date string formatted ``YYYY-MM``. For example, ``2013-07`` specifies
    July of 2013.
    """

    date_format = '%Y-%m'

    def _add_months(self, date, months):
        """
        Add ``months`` months to ``date``.

        Unfortunately we can't use timedeltas to add months because timedelta counts in days
        and there's no foolproof way to add N months in days without counting the number of
        days per month.
        """
        year = date.year + (date.month + months - 1) // 12
        month = (date.month + months - 1) % 12 + 1
        return datetime.date(year=year, month=month, day=1)

    def next_in_enumeration(self, value):
        return self._add_months(value, self.interval)

    def normalize(self, value):
        if value is None:
            return None

        months_since_start = (value.year - self.start.year) * 12 + (value.month - self.start.month)
        months_since_start -= months_since_start % self.interval

        return self._add_months(self.start, months_since_start)


class YearParameter(DateParameter):
    """
    Parameter whose value is a :py:class:`~datetime.date`, specified to the year
    (day and month of :py:class:`~datetime.date` is "rounded" to first day of the year).

    A YearParameter is a Date string formatted ``YYYY``.
    """

    date_format = '%Y'

    def next_in_enumeration(self, value):
        return value.replace(year=value.year + self.interval)

    def normalize(self, value):
        if value is None:
            return None

        delta = (value.year - self.start.year) % self.interval
        return datetime.date(year=value.year - delta, month=1, day=1)


class _DatetimeParameterBase(Parameter):
    """
    Base class Parameter for datetime
    """

    def __init__(self, interval=1, start=None, **kwargs):
        super(_DatetimeParameterBase, self).__init__(**kwargs)
        self.interval = interval
        self.start = start if start is not None else _UNIX_EPOCH

    @abc.abstractproperty
    def date_format(self):
        """
        Override me with a :py:meth:`~datetime.date.strftime` string.
        """
        pass

    @abc.abstractproperty
    def _timedelta(self):
        """
        How to move one interval of this type forward (i.e. not counting self.interval).
        """
        pass

    def parse(self, s):
        """
        Parses a string to a :py:class:`~datetime.datetime`.
        """
        return datetime.datetime.strptime(s, self.date_format)

    def serialize(self, dt):
        """
        Converts the date to a string using the :py:attr:`~_DatetimeParameterBase.date_format`.
        """
        if dt is None:
            return str(dt)
        return dt.strftime(self.date_format)

    @staticmethod
    def _convert_to_dt(dt):
        if not isinstance(dt, datetime.datetime):
            dt = datetime.datetime.combine(dt, datetime.time.min)
        return dt

    def normalize(self, dt):
        """
        Clamp dt to every Nth :py:attr:`~_DatetimeParameterBase.interval` starting at
        :py:attr:`~_DatetimeParameterBase.start`.
        """
        if dt is None:
            return None

        dt = self._convert_to_dt(dt)

        dt = dt.replace(microsecond=0)  # remove microseconds, to avoid float rounding issues.
        delta = (dt - self.start).total_seconds()
        granularity = (self._timedelta * self.interval).total_seconds()
        return dt - datetime.timedelta(seconds=delta % granularity)

    def next_in_enumeration(self, value):
        return value + self._timedelta * self.interval


class DateHourParameter(_DatetimeParameterBase):
    """
    Parameter whose value is a :py:class:`~datetime.datetime` specified to the hour.

    A DateHourParameter is a `ISO 8601 <http://en.wikipedia.org/wiki/ISO_8601>`_ formatted
    date and time specified to the hour. For example, ``2013-07-10T19`` specifies July 10, 2013 at
    19:00.
    """

    date_format = '%Y-%m-%dT%H'  # ISO 8601 is to use 'T'
    _timedelta = datetime.timedelta(hours=1)


class DateMinuteParameter(_DatetimeParameterBase):
    """
    Parameter whose value is a :py:class:`~datetime.datetime` specified to the minute.

    A DateMinuteParameter is a `ISO 8601 <http://en.wikipedia.org/wiki/ISO_8601>`_ formatted
    date and time specified to the minute. For example, ``2013-07-10T1907`` specifies July 10, 2013 at
    19:07.

    The interval parameter can be used to clamp this parameter to every N minutes, instead of every minute.
    """

    date_format = '%Y-%m-%dT%H%M'
    _timedelta = datetime.timedelta(minutes=1)
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


class DateSecondParameter(_DatetimeParameterBase):
    """
    Parameter whose value is a :py:class:`~datetime.datetime` specified to the second.

    A DateSecondParameter is a `ISO 8601 <http://en.wikipedia.org/wiki/ISO_8601>`_ formatted
    date and time specified to the second. For example, ``2013-07-10T190738`` specifies July 10, 2013 at
    19:07:38.

    The interval parameter can be used to clamp this parameter to every N seconds, instead of every second.
    """

    date_format = '%Y-%m-%dT%H%M%S'
    _timedelta = datetime.timedelta(seconds=1)


class IntParameter(Parameter):
    """
    Parameter whose value is an ``int``.
    """

    def parse(self, s):
        """
        Parses an ``int`` from the string using ``int()``.
        """
        return int(s)

    def next_in_enumeration(self, value):
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
    A Parameter whose value is a ``bool``. This parameter have an implicit
    default value of ``False``.
    """

    def __init__(self, *args, **kwargs):
        super(BoolParameter, self).__init__(*args, **kwargs)
        if self._default == _no_value:
            self._default = False

    def parse(self, s):
        """
        Parses a ``bool`` from the string, matching 'true' or 'false' ignoring case.
        """
        return {'true': True, 'false': False}[str(s).lower()]

    def normalize(self, value):
        # coerce anything truthy to True
        return bool(value) if value is not None else None

    @staticmethod
    def _parser_action():
        return 'store_true'


class DateIntervalParameter(Parameter):
    """
    A Parameter whose value is a :py:class:`~luigi.date_interval.DateInterval`.

    Date Intervals are specified using the ISO 8601 date notation for dates
    (eg. "2015-11-04"), months (eg. "2015-05"), years (eg. "2015"), or weeks
    (eg. "2015-W35"). In addition, it also supports arbitrary date intervals
    provided as two dates separated with a dash (eg. "2015-11-04-2015-12-04").
    """
    def parse(self, s):
        """
        Parses a :py:class:`~luigi.date_interval.DateInterval` from the input.

        see :py:mod:`luigi.date_interval`
          for details on the parsing of DateIntervals.
        """
        # TODO: can we use xml.utils.iso8601 or something similar?

        from luigi import date_interval as d

        for cls in [d.Year, d.Month, d.Week, d.Date, d.Custom]:
            i = cls.parse(s)
            if i:
                return i

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
        if re_match and any(re_match.groups()):
            kwargs = {}
            has_val = False
            for k, v in six.iteritems(re_match.groupdict(default="0")):
                val = int(v)
                if val > -1:
                    has_val = True
                    kwargs[k] = val
            if has_val:
                return datetime.timedelta(**kwargs)

    def _parseIso8601(self, input):
        def field(key):
            return r"(?P<%s>\d+)%s" % (key, key[0].upper())

        def optional_field(key):
            return "(%s)?" % field(key)
        # A little loose: ISO 8601 does not allow weeks in combination with other fields, but this regex does (as does python timedelta)
        regex = "P(%s|%s(T%s)?)" % (field("weeks"), optional_field("days"), "".join([optional_field(key) for key in ["hours", "minutes", "seconds"]]))
        return self._apply_regex(regex, input)

    def _parseSimple(self, input):
        keys = ["weeks", "days", "hours", "minutes", "seconds"]
        # Give the digits a regex group name from the keys, then look for text with the first letter of the key,
        # optionally followed by the rest of the word, with final char (the "s") optional
        regex = "".join([r"((?P<%s>\d+) ?%s(%s)?(%s)? ?)?" % (k, k[0], k[1:-1], k[-1]) for k in keys])
        return self._apply_regex(regex, input)

    def parse(self, input):
        """
        Parses a time delta from the input.

        See :py:class:`TimeDeltaParameter` for details on supported formats.
        """
        result = self._parseIso8601(input)
        if not result:
            result = self._parseSimple(input)
        if result is not None:
            return result
        else:
            raise ParameterException("Invalid time delta - could not parse %s" % input)

    def serialize(self, x):
        """
        Converts datetime.timedelta to a string

        :param x: the value to serialize.
        """
        weeks = x.days // 7
        days = x.days % 7
        hours = x.seconds // 3600
        minutes = (x.seconds % 3600) // 60
        seconds = (x.seconds % 3600) % 60
        result = "{} w {} d {} h {} m {} s".format(weeks, days, hours, minutes, seconds)
        return result

    def _warn_on_wrong_param_type(self, param_name, param_value):
        if self.__class__ != TimeDeltaParameter:
            return
        if not isinstance(param_value, datetime.timedelta):
            warnings.warn('Parameter "{}" with value "{}" is not of type timedelta.'.format(param_name, param_value))


class TaskParameter(Parameter):
    """
    A parameter that takes another luigi task class.

    When used programatically, the parameter should be specified
    directly with the :py:class:`luigi.task.Task` (sub) class. Like
    ``MyMetaTask(my_task_param=my_tasks.MyTask)``. On the command line,
    you specify the :py:meth:`luigi.task.Task.get_task_family`. Like

    .. code-block:: console

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

    def serialize(self, cls):
        """
        Converts the :py:class:`luigi.task.Task` (sub) class to its family name.
        """
        return cls.get_task_family()


class EnumParameter(Parameter):
    """
    A parameter whose value is an :class:`~enum.Enum`.

    In the task definition, use

    .. code-block:: python

        class Model(enum.Enum):
          Honda = 1
          Volvo = 2

        class MyTask(luigi.Task):
          my_param = luigi.EnumParameter(enum=Model)

    At the command line, use,

    .. code-block:: console

        $ luigi --module my_tasks MyTask --my-param Honda

    """

    def __init__(self, *args, **kwargs):
        if 'enum' not in kwargs:
            raise ParameterException('An enum class must be specified.')
        self._enum = kwargs.pop('enum')
        super(EnumParameter, self).__init__(*args, **kwargs)

    def parse(self, s):
        try:
            return self._enum[s]
        except KeyError:
            raise ValueError('Invalid enum value - could not be parsed')

    def serialize(self, e):
        return e.name


class _FrozenOrderedDict(Mapping):
    """
    It is an immutable wrapper around ordered dictionaries that implements the complete :py:class:`collections.Mapping`
    interface. It can be used as a drop-in replacement for dictionaries where immutability and ordering are desired.
    """

    def __init__(self, *args, **kwargs):
        self.__dict = OrderedDict(*args, **kwargs)
        self.__hash = None

    def __getitem__(self, key):
        return self.__dict[key]

    def __iter__(self):
        return iter(self.__dict)

    def __len__(self):
        return len(self.__dict)

    def __repr__(self):
        return '<FrozenOrderedDict %s>' % repr(self.__dict)

    def __hash__(self):
        if self.__hash is None:
            hashes = map(hash, self.items())
            self.__hash = functools.reduce(operator.xor, hashes, 0)

        return self.__hash

    def get_wrapped(self):
        return self.__dict


def _recursively_freeze(value):
    """
    Recursively walks ``Mapping``s and ``list``s and converts them to ``_FrozenOrderedDict`` and ``tuples``, respectively.
    """
    if isinstance(value, Mapping):
        return _FrozenOrderedDict(((k, _recursively_freeze(v)) for k, v in value.items()))
    elif isinstance(value, list) or isinstance(value, tuple):
        return tuple(_recursively_freeze(v) for v in value)
    return value


class _DictParamEncoder(JSONEncoder):
    """
    JSON encoder for :py:class:`~DictParameter`, which makes :py:class:`~_FrozenOrderedDict` JSON serializable.
    """
    def default(self, obj):
        if isinstance(obj, _FrozenOrderedDict):
            return obj.get_wrapped()
        return json.JSONEncoder.default(self, obj)


class DictParameter(Parameter):
    """
    Parameter whose value is a ``dict``.

    In the task definition, use

    .. code-block:: python

        class MyTask(luigi.Task):
          tags = luigi.DictParameter()

            def run(self):
                logging.info("Find server with role: %s", self.tags['role'])
                server = aws.ec2.find_my_resource(self.tags)


    At the command line, use

    .. code-block:: console

        $ luigi --module my_tasks MyTask --tags <JSON string>

    Simple example with two tags:

    .. code-block:: console

        $ luigi --module my_tasks MyTask --tags '{"role": "web", "env": "staging"}'

    It can be used to define dynamic parameters, when you do not know the exact list of your parameters (e.g. list of
    tags, that are dynamically constructed outside Luigi), or you have a complex parameter containing logically related
    values (like a database connection config).
    """
    def normalize(self, value):
        """
        Ensure that dictionary parameter is converted to a _FrozenOrderedDict so it can be hashed.
        """
        return _recursively_freeze(value)

    def parse(self, s):
        """
        Parses an immutable and ordered ``dict`` from a JSON string using standard JSON library.

        We need to use an immutable dictionary, to create a hashable parameter and also preserve the internal structure
        of parsing. The traversal order of standard ``dict`` is undefined, which can result various string
        representations of this parameter, and therefore a different task id for the task containing this parameter.
        This is because task id contains the hash of parameters' JSON representation.

        :param s: String to be parse
        """
        return json.loads(s, object_pairs_hook=_FrozenOrderedDict)

    def serialize(self, x):
        return json.dumps(x, cls=_DictParamEncoder)


class ListParameter(Parameter):
    """
    Parameter whose value is a ``list``.

    In the task definition, use

    .. code-block:: python

        class MyTask(luigi.Task):
          grades = luigi.ListParameter()

            def run(self):
                sum = 0
                for element in self.grades:
                    sum += element
                avg = sum / len(self.grades)


    At the command line, use

    .. code-block:: console

        $ luigi --module my_tasks MyTask --grades <JSON string>

    Simple example with two grades:

    .. code-block:: console

        $ luigi --module my_tasks MyTask --grades '[100,70]'
    """
    def normalize(self, x):
        """
        Ensure that struct is recursively converted to a tuple so it can be hashed.

        :param str x: the value to parse.
        :return: the normalized (hashable/immutable) value.
        """
        return _recursively_freeze(x)

    def parse(self, x):
        """
        Parse an individual value from the input.

        :param str x: the value to parse.
        :return: the parsed value.
        """
        return list(json.loads(x, object_pairs_hook=_FrozenOrderedDict))

    def serialize(self, x):
        """
        Opposite of :py:meth:`parse`.

        Converts the value ``x`` to a string.

        :param x: the value to serialize.
        """
        return json.dumps(x, cls=_DictParamEncoder)


class TupleParameter(ListParameter):
    """
    Parameter whose value is a ``tuple`` or ``tuple`` of tuples.

    In the task definition, use

    .. code-block:: python

        class MyTask(luigi.Task):
          book_locations = luigi.TupleParameter()

            def run(self):
                for location in self.book_locations:
                    print("Go to page %d, line %d" % (location[0], location[1]))


    At the command line, use

    .. code-block:: console

        $ luigi --module my_tasks MyTask --book_locations <JSON string>

    Simple example with two grades:

    .. code-block:: console

        $ luigi --module my_tasks MyTask --book_locations '((12,3),(4,15),(52,1))'
    """
    def parse(self, x):
        """
        Parse an individual value from the input.

        :param str x: the value to parse.
        :return: the parsed value.
        """
        # Since the result of json.dumps(tuple) differs from a tuple string, we must handle either case.
        # A tuple string may come from a config file or from cli execution.

        # t = ((1, 2), (3, 4))
        # t_str = '((1,2),(3,4))'
        # t_json_str = json.dumps(t)
        # t_json_str == '[[1, 2], [3, 4]]'
        # json.loads(t_json_str) == t
        # json.loads(t_str) == ValueError: No JSON object could be decoded

        # Therefore, if json.loads(x) returns a ValueError, try ast.literal_eval(x).
        # ast.literal_eval(t_str) == t
        try:
            # loop required to parse tuple of tuples
            return tuple(tuple(x) for x in json.loads(x, object_pairs_hook=_FrozenOrderedDict))
        except ValueError:
            return literal_eval(x)  # if this causes an error, let that error be raised.


class NumericalParameter(Parameter):
    """
    Parameter whose value is a number of the specified type, e.g. ``int`` or
    ``float`` and in the range specified.

    In the task definition, use

    .. code-block:: python

        class MyTask(luigi.Task):
            my_param_1 = luigi.NumericalParameter(
                var_type=int, min_value=-3, max_value=7) # -3 <= my_param_1 < 7
            my_param_2 = luigi.NumericalParameter(
                var_type=int, min_value=-3, max_value=7, left_op=operator.lt, right_op=operator.le) # -3 < my_param_2 <= 7

    At the command line, use

    .. code-block:: console

        $ luigi --module my_tasks MyTask --my-param-1 -3 --my-param-2 -2
    """
    def __init__(self, left_op=operator.le, right_op=operator.lt, *args, **kwargs):
        """
        :param function var_type: The type of the input variable, e.g. int or float.
        :param min_value: The minimum value permissible in the accepted values
                          range.  May be inclusive or exclusive based on left_op parameter.
                          This should be the same type as var_type.
        :param max_value: The maximum value permissible in the accepted values
                          range.  May be inclusive or exclusive based on right_op parameter.
                          This should be the same type as var_type.
        :param function left_op: The comparison operator for the left-most comparison in
                                 the expression ``min_value left_op value right_op value``.
                                 This operator should generally be either
                                 ``operator.lt`` or ``operator.le``.
                                 Default: ``operator.le``.
        :param function right_op: The comparison operator for the right-most comparison in
                                  the expression ``min_value left_op value right_op value``.
                                  This operator should generally be either
                                  ``operator.lt`` or ``operator.le``.
                                  Default: ``operator.lt``.
        """
        if "var_type" not in kwargs:
            raise ParameterException("var_type must be specified")
        self._var_type = kwargs.pop("var_type")
        if "min_value" not in kwargs:
            raise ParameterException("min_value must be specified")
        self._min_value = kwargs.pop("min_value")
        if "max_value" not in kwargs:
            raise ParameterException("max_value must be specified")
        self._max_value = kwargs.pop("max_value")
        self._left_op = left_op
        self._right_op = right_op
        self._permitted_range = (
            "{var_type} in {left_endpoint}{min_value}, {max_value}{right_endpoint}".format(
                var_type=self._var_type.__name__,
                min_value=self._min_value, max_value=self._max_value,
                left_endpoint="[" if left_op == operator.le else "(",
                right_endpoint=")" if right_op == operator.lt else "]"))
        super(NumericalParameter, self).__init__(*args, **kwargs)
        if self.description:
            self.description += " "
        else:
            self.description = ""
        self.description += "permitted values: " + self._permitted_range

    def parse(self, s):
        value = self._var_type(s)
        if (self._left_op(self._min_value, value) and self._right_op(value, self._max_value)):
            return value
        else:
            raise ValueError(
                "{s} is not in the set of {permitted_range}".format(
                    s=s, permitted_range=self._permitted_range))


class ChoiceParameter(Parameter):
    """
    A parameter which takes two values:
        1. an instance of :class:`~collections.Iterable` and
        2. the class of the variables to convert to.

    In the task definition, use

    .. code-block:: python

        class MyTask(luigi.Task):
            my_param = luigi.ChoiceParameter(choices=[0.1, 0.2, 0.3], var_type=float)

    At the command line, use

    .. code-block:: console

        $ luigi --module my_tasks MyTask --my-param 0.1

    Consider using :class:`~luigi.EnumParameter` for a typed, structured
    alternative.  This class can perform the same role when all choices are the
    same type and transparency of parameter value on the command line is
    desired.
    """
    def __init__(self, var_type=str, *args, **kwargs):
        """
        :param function var_type: The type of the input variable, e.g. str, int,
                                  float, etc.
                                  Default: str
        :param choices: An iterable, all of whose elements are of `var_type` to
                        restrict parameter choices to.
        """
        if "choices" not in kwargs:
            raise ParameterException("A choices iterable must be specified")
        self._choices = set(kwargs.pop("choices"))
        self._var_type = var_type
        assert all(type(choice) is self._var_type for choice in self._choices), "Invalid type in choices"
        super(ChoiceParameter, self).__init__(*args, **kwargs)
        if self.description:
            self.description += " "
        else:
            self.description = ""
        self.description += (
            "Choices: {" + ", ".join(str(choice) for choice in self._choices) + "}")

    def parse(self, s):
        var = self._var_type(s)
        if var in self._choices:
            return var
        else:
            raise ValueError("{s} is not a valid choice from {choices}".format(
                s=s, choices=self._choices))
