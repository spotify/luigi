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
    An untyped Parameter

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

    There are subclasses of ``Parameter`` that define what type the parameter has. This is not
    enforced within Python, but are used for command line interaction.

    Parameter objects may be reused, but you must then set the ``positional=False`` flag.
    """
    _counter = 0  # non-atomically increasing counter used for ordering parameters.

    def __init__(self, default=_no_value, is_global=False, significant=True, description=None,
                 config_path=None, positional=True, always_in_help=False):
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
        """
        self._default = default
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

    def parse(self, x):
        """
        Parse an individual value from the input.

        The default implementation is the identity function, but subclasses should override
        this method for specialized parsing.

        :param str x: the value to parse.
        :return: the parsed value.
        """
        return x  # default impl

    def serialize(self, x):
        """
        Opposite of :py:meth:`parse`.

        Converts the value ``x`` to a string.

        :param x: the value to serialize.
        """
        if not isinstance(x, six.string_types) and self.__class__ == Parameter:
            warnings.warn("Parameter {0} is not of type string.".format(str(x)))
        return str(x)

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
                // print(instantiated_path) --> /my/path/to/my/dataset/2016/06/09/
                // ... use instantiated_path ...
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

    def normalize(self, dt):
        """
        Clamp dt to every Nth :py:attr:`~_DatetimeParameterBase.interval` starting at
        :py:attr:`~_DatetimeParameterBase.start`.
        """
        if dt is None:
            return None

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


class BooleanParameter(BoolParameter):
    """
    DEPRECATED. Use :py:class:`~BoolParameter`
    """

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
        if re_match:
            kwargs = {}
            has_val = False
            for k, v in six.iteritems(re_match.groupdict(default="0")):
                val = int(v)
                has_val = has_val or val != 0
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
        return cls.task_family


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


class FrozenOrderedDict(Mapping):
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

    class DictParamEncoder(JSONEncoder):
        """
        JSON encoder for :py:class:`~DictParameter`, which makes :py:class:`~FrozenOrderedDict` JSON serializable.
        """
        def default(self, obj):
            if isinstance(obj, FrozenOrderedDict):
                return obj.get_wrapped()
            return json.JSONEncoder.default(self, obj)

    def normalize(self, value):
        """
        Ensure that dictionary parameter is converted to a FrozenOrderedDict so it can be hashed.
        """
        return FrozenOrderedDict(value)

    def parse(self, s):
        """
        Parses an immutable and ordered ``dict`` from a JSON string using standard JSON library.

        We need to use an immutable dictionary, to create a hashable parameter and also preserve the internal structure
        of parsing. The traversal order of standard ``dict`` is undefined, which can result various string
        representations of this parameter, and therefore a different task id for the task containing this parameter.
        This is because task id contains the hash of parameters' JSON representation.

        :param s: String to be parse
        """
        return json.loads(s, object_pairs_hook=FrozenOrderedDict)

    def serialize(self, x):
        return json.dumps(x, cls=DictParameter.DictParamEncoder)


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
    def parse(self, x):
        """
        Parse an individual value from the input.

        :param str x: the value to parse.
        :return: the parsed value.
        """
        return list(json.loads(x))

    def serialize(self, x):
        """
        Opposite of :py:meth:`parse`.

        Converts the value ``x`` to a string.

        :param x: the value to serialize.
        """
        return json.dumps(x)


class TupleParameter(Parameter):
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
            return tuple(tuple(x) for x in json.loads(x))  # loop required to parse tuple of tuples
        except ValueError:
            return literal_eval(x)  # if this causes an error, let that error be raised.

    def serialize(self, x):
        """
        Opposite of :py:meth:`parse`.

        Converts the value ``x`` to a string.

        :param x: the value to serialize.
        """
        return json.dumps(x)
