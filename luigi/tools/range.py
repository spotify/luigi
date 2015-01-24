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

"""Produces contiguous completed ranges of recurring tasks.

Caveat - if gap causes (missing dependencies) aren't acted upon, then this will
eventually schedule the same gaps again and again and make no progress to other
datehours.
TODO foolproof against that kind of misuse?
"""

from datetime import datetime, timedelta
import logging
import luigi
from luigi.parameter import ParameterException
from luigi.target import FileSystemTarget
from luigi.task import Register, flatten_output
import re
import time

logger = logging.getLogger('luigi-interface')


class RangeEvent(luigi.Event):  # Not sure if subclassing currently serves a purpose. Stringly typed, events are.
    """Events communicating useful metrics.

    COMPLETE_COUNT would normally be nondecreasing, and its derivative would
    describe performance (how many instances complete
    invocation-over-invocation).

    COMPLETE_FRACTION reaching 1 would be a telling event in case of a backfill
    with defined start and stop. Would not be strikingly useful for a typical
    recurring task without stop defined, fluctuating close to 1.

    DELAY is measured from the first found missing datehour till (current time
    + hours_forward), or till stop if it is defined. In hours for Hourly.
    TBD different units for other frequencies?
    TODO any different for reverse mode? From first missing till last missing?
    """
    COMPLETE_COUNT = "event.tools.range.complete.count"
    COMPLETE_FRACTION = "event.tools.range.complete.fraction"
    DELAY = "event.tools.range.delay"


class RangeHourlyBase(luigi.WrapperTask):
    """Produces a contiguous completed range of a hourly recurring task.

    Made for the common use case where a task is parameterized by datehour and
    assurance is needed that any gaps arising from downtime are eventually
    filled.

    TODO Emits events that one can use to monitor gaps and delays.

    At least one of start and stop needs to be specified.
    """

    of = luigi.Parameter(
        description="task name to be completed. The task must take a single datehour parameter")
        # TODO lift the single parameter constraint by passing unknown parameters through WrapperTask?
    start = luigi.DateHourParameter(
        default=None,
        description="beginning datehour, inclusive. Default: None - work backward forever (requires reverse=True)")
    stop = luigi.DateHourParameter(
        default=None,
        description="ending datehour, exclusive. Default: None - work forward forever")
        # wanted to name them "from" and "to", but "from" is a reserved word :/ So named after https://docs.python.org/2/library/functions.html#range arguments
    reverse = luigi.BooleanParameter(
        default=False,
        description="specifies the preferred order for catching up. False - work from the oldest missing outputs onward; True - from the newest backward")
    task_limit = luigi.IntParameter(
        default=50,
        description="how many of 'of' tasks to require. Guards against scheduling insane amounts of tasks in one go")
        # TODO vary based on cluster load (time of day)? Resources feature suits that better though
    hours_back = luigi.IntParameter(
        default=100 * 24,  # slightly more than three months
        description="extent to which contiguousness is to be assured into past, in hours from current time. Prevents infinite loop when start is none. If the dataset has limited retention (i.e. old outputs get removed), this should be set shorter to that, too, to prevent the oldest outputs flapping. Increase freely if you intend to process old dates - worker's memory is the limit")
        # TODO always entire interval for reprocessings (fixed start and stop)?
    hours_forward = luigi.IntParameter(
        default=0,
        description="extent to which contiguousness is to be assured into future, in hours from current time. Prevents infinite loop when stop is none")
    # TODO when generalizing for RangeDaily/Weekly, "hours_" will need to become something else... strict_back, always in seconds?
    # TODO overridable exclude_datehours or something...
    now = luigi.IntParameter(
        default=None,
        description="set to override current time. In seconds since epoch")

    @staticmethod
    def total_seconds(td):
        """Total seconds in datetime.timedelta td. Python 2.7 backport."""
        return td.days * 24 * 60 * 60 + td.seconds + td.microseconds * 1E-6

    def missing_datehours(self, task_cls, finite_datehours):
        """Override in subclasses to do bulk checks.

        This is a conservative base implementation that brutally checks
        completeness, instance by instance. Inadvisable as it may be slow.
        """
        return [d for d in finite_datehours if not task_cls(d).complete()]

    def _emit_metrics(self, missing_datehours, now):
        """For consistent metrics one should consider the entire range, but it
        is open (infinite) if stop or start is None. In those cases the most
        meaningful thing I can think of is to use (now + hours_forward) resp.
        (now - hours_back) as a moving endpoint.
        """
        if self.start:
            finite_start = self.start
            finite_stop = max(self.stop or now + timedelta(hours=self.hours_forward + 1), finite_start)
        else:
            finite_stop = self.stop
            finite_start = min(self.start or now - timedelta(hours=self.hours_back), finite_stop)

        delay_in_hours = (self.total_seconds(finite_stop - missing_datehours[0]) if missing_datehours else 0) / 3600.
        self.trigger_event(RangeEvent.DELAY, self.of, delay_in_hours)

        expected_count = int(self.total_seconds(finite_stop - finite_start)) / 3600
        complete_count = expected_count - len(missing_datehours)
        self.trigger_event(RangeEvent.COMPLETE_COUNT, self.of, complete_count)
        self.trigger_event(RangeEvent.COMPLETE_FRACTION, self.of, float(complete_count) / expected_count if expected_count else 1)

    def requires(self):
        # cache because we anticipate a fair amount of computation
        if hasattr(self, '_cached_requires'):
            return self._cached_requires

        if not self.start and not self.stop:
            raise ParameterException("At least one of start and stop needs to be specified")
        if not self.start and not self.reverse:
            raise ParameterException("Either start needs to be specified or reverse needs to be True")
        # TODO check overridden complete() and exists()

        now = datetime.utcfromtimestamp(time.time() if self.now is None else self.now)
        now = datetime(now.year, now.month, now.day, now.hour)
        datehours = [now + timedelta(hours=h) for h in range(-self.hours_back, self.hours_forward + 1)]
        datehours = filter(lambda h: (not self.start or h >= self.start) and (not self.stop or h < self.stop), datehours)

        task_cls = Register.get_task_cls(self.of)
        if datehours:
            logger.debug('Actually checking if range [%s, %s] of %s is complete' % (datehours[0], datehours[-1], self.of))
            missing_datehours = sorted(self.missing_datehours(task_cls, datehours))
            logger.debug('Range [%s, %s] lacked %d of expected %d %s instances' % (datehours[0], datehours[-1], len(missing_datehours), len(datehours), self.of))
        else:
            missing_datehours = []

        self._emit_metrics(missing_datehours, now)

        if self.reverse:
            required_datehours = missing_datehours[-self.task_limit:]
        else:
            required_datehours = missing_datehours[:self.task_limit]
        if required_datehours:
            logger.debug('Requiring %d missing %s instances in range [%s, %s]' % (len(required_datehours), self.of, required_datehours[0], required_datehours[-1]))
        if self.reverse:
            required_datehours.reverse()  # I wish this determined the order tasks were scheduled or executed, but it doesn't. No priorities in Luigi yet

        self._cached_requires = [task_cls(d) for d in required_datehours]
        return self._cached_requires


def _constrain_glob(glob, paths, limit=5):
    """Tweaks glob into a list of more specific globs that together still cover
    paths and not too much extra.

    Saves us minutes long listings for long dataset histories.

    Specifically, in this implementation the leftmost occurrences of "[0-9]"
    give rise to a few separate globs that each specialize the expression to
    digits that actually occur in paths.
    """

    def digit_set_wildcard(chars):
        """Makes a wildcard expression for the set, a bit readable. E.g. [1-5]
        """
        chars = sorted(chars)
        if len(chars) > 1 and ord(chars[-1]) - ord(chars[0]) == len(chars) - 1:
            return '[%s-%s]' % (chars[0], chars[-1])
        else:
            return '[%s]' % ''.join(chars)

    current = {glob: paths}
    while True:
        pos = current.keys()[0].find('[0-9]')
        if pos == -1:
            # no wildcard expressions left to specialize in the glob
            return current.keys()
        char_sets = {}
        for g, p in current.iteritems():
            char_sets[g] = sorted(set(path[pos] for path in p))
        if sum(len(s) for s in char_sets.values()) > limit:
            return [g.replace('[0-9]', digit_set_wildcard(char_sets[g]), 1) for g in current]
        for g, s in char_sets.iteritems():
            for c in s:
                new_glob = g.replace('[0-9]', c, 1)
                new_paths = filter(lambda p: p[pos] == c, current[g])
                current[new_glob] = new_paths
            del current[g]


def most_common(items):
    """Wanted functionality from Counters (new in Python 2.7)
    """
    counts = {}
    for i in items:
        counts.setdefault(i, 0)
        counts[i] += 1
    return sorted(counts.items(), key=lambda x: x[1])[-1]


def _get_per_location_glob(tasks, outputs, regexes):
    """Builds a glob listing existing output paths.

    Esoteric reverse engineering, but worth it given that (compared to an
    equivalent contiguousness guarantee by naive complete() checks)
    requests to the filesystem are cut by orders of magnitude, and users
    don't even have to retrofit existing tasks anyhow.
    """
    paths = [o.path for o in outputs]
    matches = [r.search(p) for r, p in zip(regexes, paths)]  #  naive, because some matches could be confused by numbers earlier in path, e.g. /foo/fifa2000k/bar/2000-12-31/00

    for m, p, t in zip(matches, paths, tasks):
        if m is None:
            raise NotImplementedError("Couldn't deduce datehour representation in output path %r of task %s" % (p, t))

    positions = [most_common((m.start(i), m.end(i)) for m in matches)[0] for i in range(1, 5)]  # the most common position of every group is likely to be conclusive hit or miss

    glob = list(paths[0])  # TODO sanity check that it's the same for all paths?
    for start, end in positions:
        glob = glob[:start] + ['[0-9]'] * (end - start) + glob[end:]
    return ''.join(glob).rsplit('/', 1)[0]  # chop off the last path item (wouldn't need to if `hadoop fs -ls -d` equivalent were available)


def _get_filesystems_and_globs(task_cls):
    """Yields a (filesystem, glob) tuple per every output location of
    task_cls.

    task_cls can have one or several FileSystemTarget outputs. For
    convenience, task_cls can be a wrapper task, in which case outputs of
    all its dependencies are considered.
    """
    # probe some scattered datehours unlikely to all occur in paths, other than by being sincere datehour parameter's representations
    # TODO limit to [self.start, self.stop) so messages are less confusing? Done trivially it can kill correctness
    sample_datehours = [datetime(y, m, d, h) for y in range(2000, 2050, 10) for m in range(1, 4) for d in range(5, 8) for h in range(21, 24)]
    regexes = [re.compile('(%04d).*(%02d).*(%02d).*(%02d)' % (d.year, d.month, d.day, d.hour)) for d in sample_datehours]
    sample_tasks = [task_cls(d) for d in sample_datehours]
    sample_outputs = [flatten_output(t) for t in sample_tasks]

    for o, t in zip(sample_outputs, sample_tasks):
        if len(o) != len(sample_outputs[0]):
            raise NotImplementedError("Outputs must be consistent over time, sorry; was %r for %r and %r for %r" % (o, t, sample_outputs[0], sample_tasks[0]))
            # TODO fall back on requiring last couple of days? to avoid astonishing blocking when changes like that are deployed
            # erm, actually it's not hard to test entire hours_back..hours_forward and split into consistent subranges FIXME
        for target in o:
            if not isinstance(target, FileSystemTarget):
                raise NotImplementedError("Output targets must be instances of FileSystemTarget; was %r for %r" % (target, t))

    for o in zip(*sample_outputs):  # transposed, so here we're iterating over logical outputs, not datehours
        glob = _get_per_location_glob(sample_tasks, o, regexes)
        yield o[0].fs, glob


def _list_existing(filesystem, glob, paths):
    globs = _constrain_glob(glob, paths)
    time_start = time.time()
    listing = []
    for g in sorted(globs):
        logger.debug('Listing %s' % g)
        listing.extend(filesystem.listdir(g))
    logger.debug('%d %s listings took %f s to return %d items' % (len(globs), filesystem.__class__.__name__, time.time() - time_start, len(listing)))
    return set(listing)


def _infer_bulk_complete_from_fs(task_cls, finite_datehours):
    """Efficiently determines missing datehours by filesystem listing.

    The current implementation works for the common case of a task writing
    output to a FileSystemTarget whose path is built using strftime with format
    like '...%Y...%m...%d...%H...', without custom complete() or exists().

    (Eventually Luigi could have ranges of completion as first-class citizens.
    Then this listing business could be factored away/be provided for
    explicitly in target API or some kind of a history server.)

    TODO support RangeDaily
    """
    filesystems_and_globs_by_location = _get_filesystems_and_globs(task_cls)
    paths_by_datehour = [[o.path for o in flatten_output(task_cls(d))] for d in finite_datehours]
    listing = set()
    for (f, g), p in zip(filesystems_and_globs_by_location, zip(*paths_by_datehour)):  # transposed, so here we're iterating over logical outputs, not datehours
        listing |= _list_existing(f, g, p)

    # quickly learn everything that's missing
    missing_datehours = []
    for d, p in zip(finite_datehours, paths_by_datehour):
        if not set(p) <= listing:
            missing_datehours.append(d)

    return missing_datehours


class RangeHourly(RangeHourlyBase):
    """Benefits from bulk_complete information to efficiently cover gaps.

    Convenient to use even from command line, like:

        luigi --module your.module RangeHourly --of YourActualTask --start 2014-01-01T00
    """
    def missing_datehours(self, task_cls, finite_datehours):
        try:
            return set(finite_datehours) - set(task_cls.bulk_complete(finite_datehours))
        except NotImplementedError:
            return _infer_bulk_complete_from_fs(task_cls, finite_datehours)
