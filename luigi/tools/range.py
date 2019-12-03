# -*- coding: utf-8 -*-
# Copyright (c) 2014 Spotify AB
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

"""
Produces contiguous completed ranges of recurring tasks.

See ``RangeDaily`` and ``RangeHourly`` for basic usage.

Caveat - if gaps accumulate, their causes (e.g. missing dependencies) going
unmonitored/unmitigated, then this will eventually keep retrying the same gaps
over and over and make no progress to more recent times. (See ``task_limit``
and ``reverse`` parameters.)
TODO foolproof against that kind of misuse?
"""
from collections import Counter
import itertools
import functools
import logging
import warnings
import re
import time
from datetime import datetime, timedelta, date

from dateutil.relativedelta import relativedelta

import luigi
from luigi.parameter import ParameterException
from luigi.target import FileSystemTarget
from luigi.task import Register, flatten_output


logger = logging.getLogger('luigi-interface')


class RangeEvent(luigi.Event):  # Not sure if subclassing currently serves a purpose. Stringly typed, events are.
    """
    Events communicating useful metrics.

    ``COMPLETE_COUNT`` would normally be nondecreasing, and its derivative
    would describe performance (how many instances complete
    invocation-over-invocation).

    ``COMPLETE_FRACTION`` reaching 1 would be a telling event in case of a
    backfill with defined start and stop. Would not be strikingly useful for a
    typical recurring task without stop defined, fluctuating close to 1.

    ``DELAY`` is measured from the first found missing datehour till (current
    time + hours_forward), or till stop if it is defined. In hours for Hourly.
    TBD different units for other frequencies?
    TODO any different for reverse mode? From first missing till last missing?
    From last gap till stop?
    """
    COMPLETE_COUNT = "event.tools.range.complete.count"
    COMPLETE_FRACTION = "event.tools.range.complete.fraction"
    DELAY = "event.tools.range.delay"


class RangeBase(luigi.WrapperTask):
    """
    Produces a contiguous completed range of a recurring task.

    Made for the common use case where a task is parameterized by e.g.
    ``DateParameter``, and assurance is needed that any gaps arising from
    downtime are eventually filled.

    Emits events that one can use to monitor gaps and delays.

    At least one of start and stop needs to be specified.

    (This is quite an abstract base class for subclasses with different
    datetime parameter classes, e.g. ``DateParameter``, ``DateHourParameter``,
    ..., and different parameter naming, e.g. days_back/forward,
    hours_back/forward, ..., as well as different documentation wording,
    to improve user experience.)

    Subclasses will need to use the ``of`` parameter when overriding methods.
    """
    # TODO lift the single parameter constraint by passing unknown parameters through WrapperTask?
    of = luigi.TaskParameter(
        description="task name to be completed. The task must take a single datetime parameter")
    of_params = luigi.DictParameter(default=dict(), description="Arguments to be provided to the 'of' class when instantiating")
    # The common parameters 'start' and 'stop' have type (e.g. DateParameter,
    # DateHourParameter) dependent on the concrete subclass, cumbersome to
    # define here generically without dark magic. Refer to the overrides.
    start = luigi.Parameter()
    stop = luigi.Parameter()
    reverse = luigi.BoolParameter(
        default=False,
        description="specifies the preferred order for catching up. False - work from the oldest missing outputs onward; True - from the newest backward")
    task_limit = luigi.IntParameter(
        default=50,
        description="how many of 'of' tasks to require. Guards against scheduling insane amounts of tasks in one go")
    # TODO overridable exclude_datetimes or something...
    now = luigi.IntParameter(
        default=None,
        description="set to override current time. In seconds since epoch")
    param_name = luigi.Parameter(
        default=None,
        description="parameter name used to pass in parameterized value. Defaults to None, meaning use first positional parameter",
        positional=False)

    @property
    def of_cls(self):
        """
        DONT USE. Will be deleted soon. Use ``self.of``!
        """
        if isinstance(self.of, str):
            warnings.warn('When using Range programatically, dont pass "of" param as string!')
            return Register.get_task_cls(self.of)
        return self.of

    # a bunch of datetime arithmetic building blocks that need to be provided in subclasses
    def datetime_to_parameter(self, dt):
        raise NotImplementedError

    def parameter_to_datetime(self, p):
        raise NotImplementedError

    def datetime_to_parameters(self, dt):
        """
        Given a date-time, will produce a dictionary of of-params combined with the ranged task parameter
        """
        raise NotImplementedError

    def parameters_to_datetime(self, p):
        """
        Given a dictionary of parameters, will extract the ranged task parameter value
        """
        raise NotImplementedError

    def moving_start(self, now):
        """
        Returns a datetime from which to ensure contiguousness in the case when
        start is None or unfeasibly far back.
        """
        raise NotImplementedError

    def moving_stop(self, now):
        """
        Returns a datetime till which to ensure contiguousness in the case when
        stop is None or unfeasibly far forward.
        """
        raise NotImplementedError

    def finite_datetimes(self, finite_start, finite_stop):
        """
        Returns the individual datetimes in interval [finite_start, finite_stop)
        for which task completeness should be required, as a sorted list.
        """
        raise NotImplementedError

    def _emit_metrics(self, missing_datetimes, finite_start, finite_stop):
        """
        For consistent metrics one should consider the entire range, but
        it is open (infinite) if stop or start is None.

        Hence make do with metrics respective to the finite simplification.
        """
        datetimes = self.finite_datetimes(
            finite_start if self.start is None else min(finite_start, self.parameter_to_datetime(self.start)),
            finite_stop if self.stop is None else max(finite_stop, self.parameter_to_datetime(self.stop)))

        delay_in_jobs = len(datetimes) - datetimes.index(missing_datetimes[0]) if datetimes and missing_datetimes else 0
        self.trigger_event(RangeEvent.DELAY, self.of_cls.task_family, delay_in_jobs)

        expected_count = len(datetimes)
        complete_count = expected_count - len(missing_datetimes)
        self.trigger_event(RangeEvent.COMPLETE_COUNT, self.of_cls.task_family, complete_count)
        self.trigger_event(RangeEvent.COMPLETE_FRACTION, self.of_cls.task_family, float(complete_count) / expected_count if expected_count else 1)

    def _format_datetime(self, dt):
        return self.datetime_to_parameter(dt)

    def _format_range(self, datetimes):
        param_first = self._format_datetime(datetimes[0])
        param_last = self._format_datetime(datetimes[-1])
        return '[%s, %s]' % (param_first, param_last)

    def _instantiate_task_cls(self, param):
        return self.of(**self._task_parameters(param))

    @property
    def _param_name(self):
        if self.param_name is None:
            return next(x[0] for x in self.of.get_params() if x[1].positional)
        else:
            return self.param_name

    def _task_parameters(self, param):
        kwargs = dict(**self.of_params)
        kwargs[self._param_name] = param
        return kwargs

    def requires(self):
        # cache because we anticipate a fair amount of computation
        if hasattr(self, '_cached_requires'):
            return self._cached_requires

        if not self.start and not self.stop:
            raise ParameterException("At least one of start and stop needs to be specified")
        if not self.start and not self.reverse:
            raise ParameterException("Either start needs to be specified or reverse needs to be True")
        if self.start and self.stop and self.start > self.stop:
            raise ParameterException("Can't have start > stop")
        # TODO check overridden complete() and exists()

        now = datetime.utcfromtimestamp(time.time() if self.now is None else self.now)

        moving_start = self.moving_start(now)
        finite_start = moving_start if self.start is None else max(self.parameter_to_datetime(self.start), moving_start)
        moving_stop = self.moving_stop(now)
        finite_stop = moving_stop if self.stop is None else min(self.parameter_to_datetime(self.stop), moving_stop)

        datetimes = self.finite_datetimes(finite_start, finite_stop) if finite_start <= finite_stop else []

        if datetimes:
            logger.debug('Actually checking if range %s of %s is complete',
                         self._format_range(datetimes), self.of_cls.task_family)
            missing_datetimes = sorted(self._missing_datetimes(datetimes))
            logger.debug('Range %s lacked %d of expected %d %s instances',
                         self._format_range(datetimes), len(missing_datetimes), len(datetimes), self.of_cls.task_family)
        else:
            missing_datetimes = []
            logger.debug('Empty range. No %s instances expected', self.of_cls.task_family)

        self._emit_metrics(missing_datetimes, finite_start, finite_stop)

        if self.reverse:
            required_datetimes = missing_datetimes[-self.task_limit:]
        else:
            required_datetimes = missing_datetimes[:self.task_limit]
        if required_datetimes:
            logger.debug('Requiring %d missing %s instances in range %s',
                         len(required_datetimes), self.of_cls.task_family, self._format_range(required_datetimes))
        if self.reverse:
            required_datetimes.reverse()  # TODO priorities, so that within the batch tasks are ordered too

        self._cached_requires = [self._instantiate_task_cls(self.datetime_to_parameter(d)) for d in required_datetimes]
        return self._cached_requires

    def missing_datetimes(self, finite_datetimes):
        """
        Override in subclasses to do bulk checks.

        Returns a sorted list.

        This is a conservative base implementation that brutally checks completeness, instance by instance.

        Inadvisable as it may be slow.
        """
        return [d for d in finite_datetimes if not self._instantiate_task_cls(self.datetime_to_parameter(d)).complete()]

    def _missing_datetimes(self, finite_datetimes):
        """
        Backward compatible wrapper. Will be deleted eventually (stated on Dec 2015)
        """
        try:
            return self.missing_datetimes(finite_datetimes)
        except TypeError as ex:
            if 'missing_datetimes()' in repr(ex):
                warnings.warn('In your Range* subclass, missing_datetimes() should only take 1 argument (see latest docs)')
                return self.missing_datetimes(self.of_cls, finite_datetimes)
            else:
                raise


class RangeDailyBase(RangeBase):
    """
    Produces a contiguous completed range of a daily recurring task.
    """
    start = luigi.DateParameter(
        default=None,
        description="beginning date, inclusive. Default: None - work backward forever (requires reverse=True)")
    stop = luigi.DateParameter(
        default=None,
        description="ending date, exclusive. Default: None - work forward forever")
    days_back = luigi.IntParameter(
        default=100,  # slightly more than three months
        description=("extent to which contiguousness is to be assured into "
                     "past, in days from current time. Prevents infinite loop "
                     "when start is none. If the dataset has limited retention"
                     " (i.e. old outputs get removed), this should be set "
                     "shorter to that, too, to prevent the oldest outputs "
                     "flapping. Increase freely if you intend to process old "
                     "dates - worker's memory is the limit"))
    days_forward = luigi.IntParameter(
        default=0,
        description="extent to which contiguousness is to be assured into future, in days from current time. Prevents infinite loop when stop is none")

    def datetime_to_parameter(self, dt):
        return dt.date()

    def parameter_to_datetime(self, p):
        return datetime(p.year, p.month, p.day)

    def datetime_to_parameters(self, dt):
        """
        Given a date-time, will produce a dictionary of of-params combined with the ranged task parameter
        """
        return self._task_parameters(dt.date())

    def parameters_to_datetime(self, p):
        """
        Given a dictionary of parameters, will extract the ranged task parameter value
        """
        dt = p[self._param_name]
        return datetime(dt.year, dt.month, dt.day)

    def moving_start(self, now):
        return now - timedelta(days=self.days_back)

    def moving_stop(self, now):
        return now + timedelta(days=self.days_forward)

    def finite_datetimes(self, finite_start, finite_stop):
        """
        Simply returns the points in time that correspond to turn of day.
        """
        date_start = datetime(finite_start.year, finite_start.month, finite_start.day)
        dates = []
        for i in itertools.count():
            t = date_start + timedelta(days=i)
            if t >= finite_stop:
                return dates
            if t >= finite_start:
                dates.append(t)


class RangeHourlyBase(RangeBase):
    """
    Produces a contiguous completed range of an hourly recurring task.
    """
    start = luigi.DateHourParameter(
        default=None,
        description="beginning datehour, inclusive. Default: None - work backward forever (requires reverse=True)")
    stop = luigi.DateHourParameter(
        default=None,
        description="ending datehour, exclusive. Default: None - work forward forever")
    hours_back = luigi.IntParameter(
        default=100 * 24,  # slightly more than three months
        description=("extent to which contiguousness is to be assured into "
                     "past, in hours from current time. Prevents infinite "
                     "loop when start is none. If the dataset has limited "
                     "retention (i.e. old outputs get removed), this should "
                     "be set shorter to that, too, to prevent the oldest "
                     "outputs flapping. Increase freely if you intend to "
                     "process old dates - worker's memory is the limit"))
    # TODO always entire interval for reprocessings (fixed start and stop)?
    hours_forward = luigi.IntParameter(
        default=0,
        description="extent to which contiguousness is to be assured into future, in hours from current time. Prevents infinite loop when stop is none")

    def datetime_to_parameter(self, dt):
        return dt

    def parameter_to_datetime(self, p):
        return p

    def datetime_to_parameters(self, dt):
        """
        Given a date-time, will produce a dictionary of of-params combined with the ranged task parameter
        """
        return self._task_parameters(dt)

    def parameters_to_datetime(self, p):
        """
        Given a dictionary of parameters, will extract the ranged task parameter value
        """
        return p[self._param_name]

    def moving_start(self, now):
        return now - timedelta(hours=self.hours_back)

    def moving_stop(self, now):
        return now + timedelta(hours=self.hours_forward)

    def finite_datetimes(self, finite_start, finite_stop):
        """
        Simply returns the points in time that correspond to whole hours.
        """
        datehour_start = datetime(finite_start.year, finite_start.month, finite_start.day, finite_start.hour)
        datehours = []
        for i in itertools.count():
            t = datehour_start + timedelta(hours=i)
            if t >= finite_stop:
                return datehours
            if t >= finite_start:
                datehours.append(t)

    def _format_datetime(self, dt):
        return luigi.DateHourParameter().serialize(dt)


class RangeByMinutesBase(RangeBase):
    """
    Produces a contiguous completed range of an recurring tasks separated a specified number of minutes.
    """
    start = luigi.DateMinuteParameter(
        default=None,
        description="beginning date-hour-minute, inclusive. Default: None - work backward forever (requires reverse=True)")
    stop = luigi.DateMinuteParameter(
        default=None,
        description="ending date-hour-minute, exclusive. Default: None - work forward forever")
    minutes_back = luigi.IntParameter(
        default=60*24,  # one day
        description=("extent to which contiguousness is to be assured into "
                     "past, in minutes from current time. Prevents infinite "
                     "loop when start is none. If the dataset has limited "
                     "retention (i.e. old outputs get removed), this should "
                     "be set shorter to that, too, to prevent the oldest "
                     "outputs flapping. Increase freely if you intend to "
                     "process old dates - worker's memory is the limit"))
    minutes_forward = luigi.IntParameter(
        default=0,
        description="extent to which contiguousness is to be assured into future, "
                    "in minutes from current time. Prevents infinite loop when stop is none")

    minutes_interval = luigi.IntParameter(
        default=1,
        description="separation between events in minutes. It must evenly divide 60"
    )

    def datetime_to_parameter(self, dt):
        return dt

    def parameter_to_datetime(self, p):
        return p

    def datetime_to_parameters(self, dt):
        """
        Given a date-time, will produce a dictionary of of-params combined with the ranged task parameter
        """
        return self._task_parameters(dt)

    def parameters_to_datetime(self, p):
        """
        Given a dictionary of parameters, will extract the ranged task parameter value
        """
        dt = p[self._param_name]
        return datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute)

    def moving_start(self, now):
        return now - timedelta(minutes=self.minutes_back)

    def moving_stop(self, now):
        return now + timedelta(minutes=self.minutes_forward)

    def finite_datetimes(self, finite_start, finite_stop):
        """
        Simply returns the points in time that correspond to a whole number of minutes intervals.
        """
        # Validate that the minutes_interval can divide 60 and it is greater than 0 and lesser than 60
        if not (0 < self.minutes_interval < 60):
            raise ParameterException('minutes-interval must be within 0..60')
        if 60 % self.minutes_interval != 0:
            raise ParameterException('minutes-interval does not evenly divide 60')
        # start of a complete interval, e.g. 20:13 and the interval is 5 -> 20:10
        start_minute = int(finite_start.minute/self.minutes_interval)*self.minutes_interval
        datehour_start = datetime(
            year=finite_start.year,
            month=finite_start.month,
            day=finite_start.day,
            hour=finite_start.hour,
            minute=start_minute)
        datehours = []
        for i in itertools.count():
            t = datehour_start + timedelta(minutes=i*self.minutes_interval)
            if t >= finite_stop:
                return datehours
            if t >= finite_start:
                datehours.append(t)

    def _format_datetime(self, dt):
        return luigi.DateMinuteParameter().serialize(dt)


def _constrain_glob(glob, paths, limit=5):
    """
    Tweaks glob into a list of more specific globs that together still cover paths and not too much extra.

    Saves us minutes long listings for long dataset histories.

    Specifically, in this implementation the leftmost occurrences of "[0-9]"
    give rise to a few separate globs that each specialize the expression to
    digits that actually occur in paths.
    """

    def digit_set_wildcard(chars):
        """
        Makes a wildcard expression for the set, a bit readable, e.g. [1-5].
        """
        chars = sorted(chars)
        if len(chars) > 1 and ord(chars[-1]) - ord(chars[0]) == len(chars) - 1:
            return '[%s-%s]' % (chars[0], chars[-1])
        else:
            return '[%s]' % ''.join(chars)

    current = {glob: paths}
    while True:
        pos = list(current.keys())[0].find('[0-9]')
        if pos == -1:
            # no wildcard expressions left to specialize in the glob
            return list(current.keys())
        char_sets = {}
        for g, p in current.items():
            char_sets[g] = sorted({path[pos] for path in p})
        if sum(len(s) for s in char_sets.values()) > limit:
            return [g.replace('[0-9]', digit_set_wildcard(char_sets[g]), 1) for g in current]
        for g, s in char_sets.items():
            for c in s:
                new_glob = g.replace('[0-9]', c, 1)
                new_paths = list(filter(lambda p: p[pos] == c, current[g]))
                current[new_glob] = new_paths
            del current[g]


def most_common(items):
    [(element, counter)] = Counter(items).most_common(1)
    return element, counter


def _get_per_location_glob(tasks, outputs, regexes):
    """
    Builds a glob listing existing output paths.

    Esoteric reverse engineering, but worth it given that (compared to an
    equivalent contiguousness guarantee by naive complete() checks)
    requests to the filesystem are cut by orders of magnitude, and users
    don't even have to retrofit existing tasks anyhow.
    """
    paths = [o.path for o in outputs]
    # naive, because some matches could be confused by numbers earlier
    # in path, e.g. /foo/fifa2000k/bar/2000-12-31/00
    matches = [r.search(p) for r, p in zip(regexes, paths)]

    for m, p, t in zip(matches, paths, tasks):
        if m is None:
            raise NotImplementedError("Couldn't deduce datehour representation in output path %r of task %s" % (p, t))

    n_groups = len(matches[0].groups())
    # the most common position of every group is likely
    # to be conclusive hit or miss
    positions = [most_common((m.start(i), m.end(i)) for m in matches)[0] for i in range(1, n_groups + 1)]

    glob = list(paths[0])  # FIXME sanity check that it's the same for all paths
    for start, end in positions:
        glob = glob[:start] + ['[0-9]'] * (end - start) + glob[end:]
    # chop off the last path item
    # (wouldn't need to if `hadoop fs -ls -d` equivalent were available)
    return ''.join(glob).rsplit('/', 1)[0]


def _get_filesystems_and_globs(datetime_to_task, datetime_to_re):
    """
    Yields a (filesystem, glob) tuple per every output location of task.

    The task can have one or several FileSystemTarget outputs.

    For convenience, the task can be a luigi.WrapperTask,
    in which case outputs of all its dependencies are considered.
    """
    # probe some scattered datetimes unlikely to all occur in paths, other than by being sincere datetime parameter's representations
    # TODO limit to [self.start, self.stop) so messages are less confusing? Done trivially it can kill correctness
    sample_datetimes = [datetime(y, m, d, h) for y in range(2000, 2050, 10) for m in range(1, 4) for d in range(5, 8) for h in range(21, 24)]
    regexes = [re.compile(datetime_to_re(d)) for d in sample_datetimes]
    sample_tasks = [datetime_to_task(d) for d in sample_datetimes]
    sample_outputs = [flatten_output(t) for t in sample_tasks]

    for o, t in zip(sample_outputs, sample_tasks):
        if len(o) != len(sample_outputs[0]):
            raise NotImplementedError("Outputs must be consistent over time, sorry; was %r for %r and %r for %r" % (o, t, sample_outputs[0], sample_tasks[0]))
            # TODO fall back on requiring last couple of days? to avoid astonishing blocking when changes like that are deployed
            # erm, actually it's not hard to test entire hours_back..hours_forward and split into consistent subranges FIXME?
        for target in o:
            if not isinstance(target, FileSystemTarget):
                raise NotImplementedError("Output targets must be instances of FileSystemTarget; was %r for %r" % (target, t))

    for o in zip(*sample_outputs):  # transposed, so here we're iterating over logical outputs, not datetimes
        glob = _get_per_location_glob(sample_tasks, o, regexes)
        yield o[0].fs, glob


def _list_existing(filesystem, glob, paths):
    """
    Get all the paths that do in fact exist. Returns a set of all existing paths.

    Takes a luigi.target.FileSystem object, a str which represents a glob and
    a list of strings representing paths.
    """
    globs = _constrain_glob(glob, paths)
    time_start = time.time()
    listing = []
    for g in sorted(globs):
        logger.debug('Listing %s', g)
        if filesystem.exists(g):
            listing.extend(filesystem.listdir(g))
    logger.debug('%d %s listings took %f s to return %d items',
                 len(globs), filesystem.__class__.__name__, time.time() - time_start, len(listing))
    return set(listing)


def infer_bulk_complete_from_fs(datetimes, datetime_to_task, datetime_to_re):
    """
    Efficiently determines missing datetimes by filesystem listing.

    The current implementation works for the common case of a task writing
    output to a ``FileSystemTarget`` whose path is built using strftime with
    format like '...%Y...%m...%d...%H...', without custom ``complete()`` or
    ``exists()``.

    (Eventually Luigi could have ranges of completion as first-class citizens.
    Then this listing business could be factored away/be provided for
    explicitly in target API or some kind of a history server.)
    """
    filesystems_and_globs_by_location = _get_filesystems_and_globs(datetime_to_task, datetime_to_re)
    paths_by_datetime = [[o.path for o in flatten_output(datetime_to_task(d))] for d in datetimes]
    listing = set()
    for (f, g), p in zip(filesystems_and_globs_by_location, zip(*paths_by_datetime)):  # transposed, so here we're iterating over logical outputs, not datetimes
        listing |= _list_existing(f, g, p)

    # quickly learn everything that's missing
    missing_datetimes = []
    for d, p in zip(datetimes, paths_by_datetime):
        if not set(p) <= listing:
            missing_datetimes.append(d)

    return missing_datetimes


class RangeMonthly(RangeBase):
    """
    Produces a contiguous completed range of a monthly recurring task.

    Unlike the Range* classes with shorter intervals, this class does not perform bulk optimisation.
    It is assumed that the number of months is low enough not to motivate the increased complexity.
    Hence, there is no class RangeMonthlyBase.
    """
    start = luigi.MonthParameter(
        default=None,
        description="beginning month, inclusive. Default: None - work backward forever (requires reverse=True)")
    stop = luigi.MonthParameter(
        default=None,
        description="ending month, exclusive. Default: None - work forward forever")
    months_back = luigi.IntParameter(
        default=13,  # Little over a year
        description=("extent to which contiguousness is to be assured into "
                     "past, in months from current time. Prevents infinite loop "
                     "when start is none. If the dataset has limited retention"
                     " (i.e. old outputs get removed), this should be set "
                     "shorter to that, too, to prevent the oldest outputs "
                     "flapping. Increase freely if you intend to process old "
                     "dates - worker's memory is the limit"))
    months_forward = luigi.IntParameter(
        default=0,
        description="extent to which contiguousness is to be assured into future, in months from current time. "
                    "Prevents infinite loop when stop is none")

    def datetime_to_parameter(self, dt):
        return date(dt.year, dt.month, 1)

    def parameter_to_datetime(self, p):
        return datetime(p.year, p.month, 1)

    def datetime_to_parameters(self, dt):
        """
        Given a date-time, will produce a dictionary of of-params combined with the ranged task parameter
        """
        return self._task_parameters(dt.date())

    def parameters_to_datetime(self, p):
        """
        Given a dictionary of parameters, will extract the ranged task parameter value
        """
        dt = p[self._param_name]
        return datetime(dt.year, dt.month, 1)

    def _format_datetime(self, dt):
        return dt.strftime('%Y-%m')

    def moving_start(self, now):
        return self._align(now) - relativedelta(months=self.months_back)

    def moving_stop(self, now):
        return self._align(now) + relativedelta(months=self.months_forward)

    def _align(self, dt):
        return datetime(dt.year, dt.month, 1)

    def finite_datetimes(self, finite_start, finite_stop):
        """
        Simply returns the points in time that correspond to turn of month.
        """
        start_date = self._align(finite_start)
        aligned_stop = self._align(finite_stop)
        dates = []
        for m in itertools.count():
            t = start_date + relativedelta(months=m)
            if t >= aligned_stop:
                return dates
            if t >= finite_start:
                dates.append(t)


class RangeDaily(RangeDailyBase):
    """Efficiently produces a contiguous completed range of a daily recurring
    task that takes a single ``DateParameter``.

    Falls back to infer it from output filesystem listing to facilitate the
    common case usage.

    Convenient to use even from command line, like:

    .. code-block:: console

        luigi --module your.module RangeDaily --of YourActualTask --start 2014-01-01
    """

    def missing_datetimes(self, finite_datetimes):
        try:
            cls_with_params = functools.partial(self.of, **self.of_params)
            complete_parameters = self.of.bulk_complete.__func__(cls_with_params, map(self.datetime_to_parameter, finite_datetimes))
            return set(finite_datetimes) - set(map(self.parameter_to_datetime, complete_parameters))
        except NotImplementedError:
            return infer_bulk_complete_from_fs(
                finite_datetimes,
                lambda d: self._instantiate_task_cls(self.datetime_to_parameter(d)),
                lambda d: d.strftime('(%Y).*(%m).*(%d)'))


class RangeHourly(RangeHourlyBase):
    """Efficiently produces a contiguous completed range of an hourly recurring
    task that takes a single ``DateHourParameter``.

    Benefits from ``bulk_complete`` information to efficiently cover gaps.

    Falls back to infer it from output filesystem listing to facilitate the
    common case usage.

    Convenient to use even from command line, like:

    .. code-block:: console

        luigi --module your.module RangeHourly --of YourActualTask --start 2014-01-01T00
    """

    def missing_datetimes(self, finite_datetimes):
        try:
            # TODO: Why is there a list() here but not for the RangeDaily??
            cls_with_params = functools.partial(self.of, **self.of_params)
            complete_parameters = self.of.bulk_complete.__func__(cls_with_params, list(map(self.datetime_to_parameter, finite_datetimes)))
            return set(finite_datetimes) - set(map(self.parameter_to_datetime, complete_parameters))
        except NotImplementedError:
            return infer_bulk_complete_from_fs(
                finite_datetimes,
                lambda d: self._instantiate_task_cls(self.datetime_to_parameter(d)),
                lambda d: d.strftime('(%Y).*(%m).*(%d).*(%H)'))


class RangeByMinutes(RangeByMinutesBase):
    """Efficiently produces a contiguous completed range of an recurring
    task every interval minutes that takes a single ``DateMinuteParameter``.

    Benefits from ``bulk_complete`` information to efficiently cover gaps.

    Falls back to infer it from output filesystem listing to facilitate the
    common case usage.

    Convenient to use even from command line, like:

    .. code-block:: console

        luigi --module your.module RangeByMinutes --of YourActualTask --start 2014-01-01T0123
    """

    def missing_datetimes(self, finite_datetimes):
        try:
            cls_with_params = functools.partial(self.of, **self.of_params)
            complete_parameters = self.of.bulk_complete.__func__(cls_with_params, map(self.datetime_to_parameter, finite_datetimes))
            return set(finite_datetimes) - set(map(self.parameter_to_datetime, complete_parameters))
        except NotImplementedError:
            return infer_bulk_complete_from_fs(
                finite_datetimes,
                lambda d: self._instantiate_task_cls(self.datetime_to_parameter(d)),
                lambda d: d.strftime('(%Y).*(%m).*(%d).*(%H).*(%M)'))
