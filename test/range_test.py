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

import datetime
import fnmatch
from helpers import unittest

import luigi
import mock
from luigi.mock import MockTarget, MockFileSystem
from luigi.tools.range import (RangeDaily, RangeDailyBase, RangeEvent, RangeHourly, RangeHourlyBase, _constrain_glob,
                               _get_filesystems_and_globs)


class CommonDateHourTask(luigi.Task):
    dh = luigi.DateHourParameter()

    def output(self):
        return MockTarget(self.dh.strftime('/n2000y01a05n/%Y_%m-_-%daww/21mm%Hdara21/ooo'))


class CommonDateTask(luigi.Task):
    d = luigi.DateParameter()

    def output(self):
        return MockTarget(self.d.strftime('/n2000y01a05n/%Y_%m-_-%daww/21mm01dara21/ooo'))


task_a_paths = [
    'TaskA/2014-03-20/18',
    'TaskA/2014-03-20/21',
    'TaskA/2014-03-20/23',
    'TaskA/2014-03-21/00',
    'TaskA/2014-03-21/00.attempt.1',
    'TaskA/2014-03-21/00.attempt.2',
    'TaskA/2014-03-21/01',
    'TaskA/2014-03-21/02',
    'TaskA/2014-03-21/03.attempt-temp-2014-03-21T13-22-58.165969',
    'TaskA/2014-03-21/03.attempt.1',
    'TaskA/2014-03-21/03.attempt.2',
    'TaskA/2014-03-21/03.attempt.3',
    'TaskA/2014-03-21/03.attempt.latest',
    'TaskA/2014-03-21/04.attempt-temp-2014-03-21T13-23-09.078249',
    'TaskA/2014-03-21/12',
    'TaskA/2014-03-23/12',
]

task_b_paths = [
    'TaskB/no/worries2014-03-20/23',
    'TaskB/no/worries2014-03-21/01',
    'TaskB/no/worries2014-03-21/03',
    'TaskB/no/worries2014-03-21/04.attempt-yadayada',
    'TaskB/no/worries2014-03-21/05',
]

mock_contents = task_a_paths + task_b_paths


expected_a = [
    'TaskA(dh=2014-03-20T17)',
    'TaskA(dh=2014-03-20T19)',
    'TaskA(dh=2014-03-20T20)',
]

# expected_reverse = [
# ]

expected_wrapper = [
    'CommonWrapperTask(dh=2014-03-21T00)',
    'CommonWrapperTask(dh=2014-03-21T02)',
    'CommonWrapperTask(dh=2014-03-21T03)',
    'CommonWrapperTask(dh=2014-03-21T04)',
    'CommonWrapperTask(dh=2014-03-21T05)',
]


class TaskA(luigi.Task):
    dh = luigi.DateHourParameter()

    def output(self):
        return MockTarget(self.dh.strftime('TaskA/%Y-%m-%d/%H'))


class TaskB(luigi.Task):
    dh = luigi.DateHourParameter()
    complicator = luigi.Parameter()

    def output(self):
        return MockTarget(self.dh.strftime('TaskB/%%s%Y-%m-%d/%H') % self.complicator)


class TaskC(luigi.Task):
    dh = luigi.DateHourParameter()

    def output(self):
        return MockTarget(self.dh.strftime('not/a/real/path/%Y-%m-%d/%H'))


class CommonWrapperTask(luigi.WrapperTask):
    dh = luigi.DateHourParameter()

    def requires(self):
        yield TaskA(dh=self.dh)
        yield TaskB(dh=self.dh, complicator='no/worries')  # str(self.dh) would complicate beyond working


def mock_listdir(contents):
    def contents_listdir(_, glob):
        for path in fnmatch.filter(contents, glob + '*'):
            yield path

    return contents_listdir


def mock_exists_always_true(_, _2):
    yield True


def mock_exists_always_false(_, _2):
    yield False


class ConstrainGlobTest(unittest.TestCase):

    def test_limit(self):
        glob = '/[0-9][0-9][0-9][0-9]/[0-9][0-9]/[0-9][0-9]/[0-9][0-9]'
        paths = [(datetime.datetime(2013, 12, 31, 5) + datetime.timedelta(hours=h)).strftime('/%Y/%m/%d/%H') for h in range(40)]
        self.assertEqual(sorted(_constrain_glob(glob, paths)), [
            '/2013/12/31/[0-2][0-9]',
            '/2014/01/01/[0-2][0-9]',
        ])
        paths.pop(26)
        self.assertEqual(sorted(_constrain_glob(glob, paths, 6)), [
            '/2013/12/31/0[5-9]',
            '/2013/12/31/1[0-9]',
            '/2013/12/31/2[0-3]',
            '/2014/01/01/0[012345689]',
            '/2014/01/01/1[0-9]',
            '/2014/01/01/2[0]',
        ])
        self.assertEqual(sorted(_constrain_glob(glob, paths[:7], 10)), [
            '/2013/12/31/05',
            '/2013/12/31/06',
            '/2013/12/31/07',
            '/2013/12/31/08',
            '/2013/12/31/09',
            '/2013/12/31/10',
            '/2013/12/31/11',
        ])

    def test_no_wildcards(self):
        glob = '/2014/01'
        paths = '/2014/01'
        self.assertEqual(_constrain_glob(glob, paths), [
            '/2014/01',
        ])


def datetime_to_epoch(dt):
    td = dt - datetime.datetime(1970, 1, 1)
    return td.days * 86400 + td.seconds + td.microseconds / 1E6


class RangeDailyBaseTest(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        # yucky to create separate callbacks; would be nicer if the callback received an instance of a subclass of Event, so one callback could accumulate all types
        @RangeDailyBase.event_handler(RangeEvent.DELAY)
        def callback_delay(*args):
            self.events.setdefault(RangeEvent.DELAY, []).append(args)

        @RangeDailyBase.event_handler(RangeEvent.COMPLETE_COUNT)
        def callback_complete_count(*args):
            self.events.setdefault(RangeEvent.COMPLETE_COUNT, []).append(args)

        @RangeDailyBase.event_handler(RangeEvent.COMPLETE_FRACTION)
        def callback_complete_fraction(*args):
            self.events.setdefault(RangeEvent.COMPLETE_FRACTION, []).append(args)

        self.events = {}

    def test_consistent_formatting(self):
        task = RangeDailyBase(of='CommonDateTask',
                              start=datetime.date(2016, 1, 1))
        self.assertEqual(task._format_range([datetime.datetime(2016, 1, 2, 13), datetime.datetime(2016, 2, 29, 23)]), '[2016-01-02, 2016-02-29]')

    def _empty_subcase(self, kwargs, expected_events):
        calls = []

        class RangeDailyDerived(RangeDailyBase):
            def missing_datetimes(*args):
                calls.append(args)
                return args[-1][:5]

        task = RangeDailyDerived(of='CommonDateTask',
                                 **kwargs)
        self.assertEqual(task.requires(), [])
        self.assertEqual(calls, [])
        self.assertEqual(task.requires(), [])
        self.assertEqual(calls, [])  # subsequent requires() should return the cached result, never call missing_datetimes
        self.assertEqual(self.events, expected_events)
        self.assertTrue(task.complete())

    def test_stop_before_days_back(self):
        # nothing to do because stop is earlier
        self._empty_subcase(
            {
                'now': datetime_to_epoch(datetime.datetime(2015, 1, 1, 4)),
                'stop': datetime.date(2014, 3, 20),
                'days_back': 4,
                'days_forward': 20,
                'reverse': True,
            },
            {
                'event.tools.range.delay': [
                    ('CommonDateTask', 0),
                ],
                'event.tools.range.complete.count': [
                    ('CommonDateTask', 0),
                ],
                'event.tools.range.complete.fraction': [
                    ('CommonDateTask', 1.),
                ],
            }
        )

    def _nonempty_subcase(self, kwargs, expected_finite_datetimes_range, expected_requires, expected_events):
        calls = []

        class RangeDailyDerived(RangeDailyBase):
            def missing_datetimes(*args):
                calls.append(args)
                return args[-1][:7]

        task = RangeDailyDerived(of='CommonDateTask',
                                 **kwargs)
        self.assertEqual(list(map(str, task.requires())), expected_requires)
        self.assertEqual(calls[0][1], CommonDateTask)
        self.assertEqual((min(calls[0][2]), max(calls[0][2])), expected_finite_datetimes_range)
        self.assertEqual(list(map(str, task.requires())), expected_requires)
        self.assertEqual(len(calls), 1)  # subsequent requires() should return the cached result, not call missing_datetimes again
        self.assertEqual(self.events, expected_events)
        self.assertFalse(task.complete())

    def test_start_long_before_long_days_back_and_with_long_days_forward(self):
        self._nonempty_subcase(
            {
                'now': datetime_to_epoch(datetime.datetime(2017, 10, 22, 12, 4, 29)),
                'start': datetime.date(2011, 3, 20),
                'stop': datetime.date(2025, 1, 29),
                'task_limit': 4,
                'days_back': 3 * 365,
                'days_forward': 3 * 365,
            },
            (datetime.datetime(2014, 10, 24), datetime.datetime(2020, 10, 21)),
            [
                'CommonDateTask(d=2014-10-24)',
                'CommonDateTask(d=2014-10-25)',
                'CommonDateTask(d=2014-10-26)',
                'CommonDateTask(d=2014-10-27)',
            ],
            {
                'event.tools.range.delay': [
                    ('CommonDateTask', 3750),
                ],
                'event.tools.range.complete.count': [
                    ('CommonDateTask', 5057),
                ],
                'event.tools.range.complete.fraction': [
                    ('CommonDateTask', 5057. / (5057 + 7)),
                ],
            }
        )


class RangeHourlyBaseTest(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        # yucky to create separate callbacks; would be nicer if the callback received an instance of a subclass of Event, so one callback could accumulate all types
        @RangeHourlyBase.event_handler(RangeEvent.DELAY)
        def callback_delay(*args):
            self.events.setdefault(RangeEvent.DELAY, []).append(args)

        @RangeHourlyBase.event_handler(RangeEvent.COMPLETE_COUNT)
        def callback_complete_count(*args):
            self.events.setdefault(RangeEvent.COMPLETE_COUNT, []).append(args)

        @RangeHourlyBase.event_handler(RangeEvent.COMPLETE_FRACTION)
        def callback_complete_fraction(*args):
            self.events.setdefault(RangeEvent.COMPLETE_FRACTION, []).append(args)

        self.events = {}

    def test_consistent_formatting(self):
        task = RangeHourlyBase(of='CommonDateHourTask',
                               start=datetime.datetime(2016, 1, 1))
        self.assertEqual(task._format_range([datetime.datetime(2016, 1, 2, 13), datetime.datetime(2016, 2, 29, 23)]), '[2016-01-02T13, 2016-02-29T23]')

    def _empty_subcase(self, kwargs, expected_events):
        calls = []

        class RangeHourlyDerived(RangeHourlyBase):
            def missing_datetimes(*args):
                calls.append(args)
                return args[-1][:5]

        task = RangeHourlyDerived(of='CommonDateHourTask',
                                  **kwargs)
        self.assertEqual(task.requires(), [])
        self.assertEqual(calls, [])
        self.assertEqual(task.requires(), [])
        self.assertEqual(calls, [])  # subsequent requires() should return the cached result, never call missing_datetimes
        self.assertEqual(self.events, expected_events)
        self.assertTrue(task.complete())

    def test_start_after_hours_forward(self):
        # nothing to do because start is later
        self._empty_subcase(
            {
                'now': datetime_to_epoch(datetime.datetime(2000, 1, 1, 4)),
                'start': datetime.datetime(2014, 3, 20, 17),
                'hours_back': 4,
                'hours_forward': 20,
            },
            {
                'event.tools.range.delay': [
                    ('CommonDateHourTask', 0),
                ],
                'event.tools.range.complete.count': [
                    ('CommonDateHourTask', 0),
                ],
                'event.tools.range.complete.fraction': [
                    ('CommonDateHourTask', 1.),
                ],
            }
        )

    def _nonempty_subcase(self, kwargs, expected_finite_datetimes_range, expected_requires, expected_events):
        calls = []

        class RangeHourlyDerived(RangeHourlyBase):
            def missing_datetimes(*args):
                calls.append(args)
                return args[-1][:7]

        task = RangeHourlyDerived(of='CommonDateHourTask',
                                  **kwargs)
        self.assertEqual(list(map(str, task.requires())), expected_requires)
        self.assertEqual(calls[0][1], CommonDateHourTask)
        self.assertEqual((min(calls[0][2]), max(calls[0][2])), expected_finite_datetimes_range)
        self.assertEqual(list(map(str, task.requires())), expected_requires)
        self.assertEqual(len(calls), 1)  # subsequent requires() should return the cached result, not call missing_datetimes again
        self.assertEqual(self.events, expected_events)
        self.assertFalse(task.complete())

    def test_start_long_before_hours_back(self):
        self._nonempty_subcase(
            {
                'now': datetime_to_epoch(datetime.datetime(2000, 1, 1, 4)),
                'start': datetime.datetime(1960, 3, 2, 1),
                'hours_back': 5,
                'hours_forward': 20,
            },
            (datetime.datetime(1999, 12, 31, 23), datetime.datetime(2000, 1, 1, 23)),
            [
                'CommonDateHourTask(dh=1999-12-31T23)',
                'CommonDateHourTask(dh=2000-01-01T00)',
                'CommonDateHourTask(dh=2000-01-01T01)',
                'CommonDateHourTask(dh=2000-01-01T02)',
                'CommonDateHourTask(dh=2000-01-01T03)',
                'CommonDateHourTask(dh=2000-01-01T04)',
                'CommonDateHourTask(dh=2000-01-01T05)',
            ],
            {
                'event.tools.range.delay': [
                    ('CommonDateHourTask', 25),  # because of short hours_back we're oblivious to those 40 preceding years
                ],
                'event.tools.range.complete.count': [
                    ('CommonDateHourTask', 349192),
                ],
                'event.tools.range.complete.fraction': [
                    ('CommonDateHourTask', 349192. / (349192 + 7)),
                ],
            }
        )

    def test_start_after_long_hours_back(self):
        self._nonempty_subcase(
            {
                'now': datetime_to_epoch(datetime.datetime(2014, 10, 22, 12, 4, 29)),
                'start': datetime.datetime(2014, 3, 20, 17),
                'task_limit': 4,
                'hours_back': 365 * 24,
            },
            (datetime.datetime(2014, 3, 20, 17), datetime.datetime(2014, 10, 22, 12)),
            [
                'CommonDateHourTask(dh=2014-03-20T17)',
                'CommonDateHourTask(dh=2014-03-20T18)',
                'CommonDateHourTask(dh=2014-03-20T19)',
                'CommonDateHourTask(dh=2014-03-20T20)',
            ],
            {
                'event.tools.range.delay': [
                    ('CommonDateHourTask', 5180),
                ],
                'event.tools.range.complete.count': [
                    ('CommonDateHourTask', 5173),
                ],
                'event.tools.range.complete.fraction': [
                    ('CommonDateHourTask', 5173. / (5173 + 7)),
                ],
            }
        )

    def test_start_long_before_long_hours_back_and_with_long_hours_forward(self):
        self._nonempty_subcase(
            {
                'now': datetime_to_epoch(datetime.datetime(2017, 10, 22, 12, 4, 29)),
                'start': datetime.datetime(2011, 3, 20, 17),
                'task_limit': 4,
                'hours_back': 3 * 365 * 24,
                'hours_forward': 3 * 365 * 24,
            },
            (datetime.datetime(2014, 10, 23, 13), datetime.datetime(2020, 10, 21, 12)),
            [
                'CommonDateHourTask(dh=2014-10-23T13)',
                'CommonDateHourTask(dh=2014-10-23T14)',
                'CommonDateHourTask(dh=2014-10-23T15)',
                'CommonDateHourTask(dh=2014-10-23T16)',
            ],
            {
                'event.tools.range.delay': [
                    ('CommonDateHourTask', 52560),
                ],
                'event.tools.range.complete.count': [
                    ('CommonDateHourTask', 84061),
                ],
                'event.tools.range.complete.fraction': [
                    ('CommonDateHourTask', 84061. / (84061 + 7)),
                ],
            }
        )


class FilesystemInferenceTest(unittest.TestCase):

    def _test_filesystems_and_globs(self, datetime_to_task, datetime_to_re, expected):
        actual = list(_get_filesystems_and_globs(datetime_to_task, datetime_to_re))
        self.assertEqual(len(actual), len(expected))
        for (actual_filesystem, actual_glob), (expected_filesystem, expected_glob) in zip(actual, expected):
            self.assertTrue(isinstance(actual_filesystem, expected_filesystem))
            self.assertEqual(actual_glob, expected_glob)

    def test_date_glob_successfully_inferred(self):
        self._test_filesystems_and_globs(
            lambda d: CommonDateTask(d),
            lambda d: d.strftime('(%Y).*(%m).*(%d)'),
            [
                (MockFileSystem, '/n2000y01a05n/[0-9][0-9][0-9][0-9]_[0-9][0-9]-_-[0-9][0-9]aww/21mm01dara21'),
            ]
        )

    def test_datehour_glob_successfully_inferred(self):
        self._test_filesystems_and_globs(
            lambda d: CommonDateHourTask(d),
            lambda d: d.strftime('(%Y).*(%m).*(%d).*(%H)'),
            [
                (MockFileSystem, '/n2000y01a05n/[0-9][0-9][0-9][0-9]_[0-9][0-9]-_-[0-9][0-9]aww/21mm[0-9][0-9]dara21'),
            ]
        )

    def test_wrapped_datehour_globs_successfully_inferred(self):
        self._test_filesystems_and_globs(
            lambda d: CommonWrapperTask(d),
            lambda d: d.strftime('(%Y).*(%m).*(%d).*(%H)'),
            [
                (MockFileSystem, 'TaskA/[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
                (MockFileSystem, 'TaskB/no/worries[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
            ]
        )

    def test_inconsistent_output_datehour_glob_not_inferred(self):
        class InconsistentlyOutputtingDateHourTask(luigi.Task):
            dh = luigi.DateHourParameter()

            def output(self):
                base = self.dh.strftime('/even/%Y%m%d%H')
                if self.dh.hour % 2 == 0:
                    return MockTarget(base)
                else:
                    return {
                        'spi': MockTarget(base + '/something.spi'),
                        'spl': MockTarget(base + '/something.spl'),
                    }

        def test_raise_not_implemented():
            list(_get_filesystems_and_globs(
                lambda d: InconsistentlyOutputtingDateHourTask(d),
                lambda d: d.strftime('(%Y).*(%m).*(%d).*(%H)')))

        self.assertRaises(NotImplementedError, test_raise_not_implemented)

    def test_wrapped_inconsistent_datehour_globs_not_inferred(self):
        class InconsistentlyParameterizedWrapperTask(luigi.WrapperTask):
            dh = luigi.DateHourParameter()

            def requires(self):
                yield TaskA(dh=self.dh - datetime.timedelta(days=1))
                yield TaskB(dh=self.dh, complicator='no/worries')

        def test_raise_not_implemented():
            list(_get_filesystems_and_globs(
                lambda d: InconsistentlyParameterizedWrapperTask(d),
                lambda d: d.strftime('(%Y).*(%m).*(%d).*(%H)')))

        self.assertRaises(NotImplementedError, test_raise_not_implemented)


class RangeDailyTest(unittest.TestCase):

    def test_bulk_complete_correctly_interfaced(self):
        class BulkCompleteDailyTask(luigi.Task):
            d = luigi.DateParameter()

            @classmethod
            def bulk_complete(self, parameter_tuples):
                return list(parameter_tuples)[:-2]

            def output(self):
                raise RuntimeError("Shouldn't get called while resolving deps via bulk_complete")

        task = RangeDaily(now=datetime_to_epoch(datetime.datetime(2015, 12, 1)),
                          of='BulkCompleteDailyTask',
                          start=datetime.date(2015, 11, 1),
                          stop=datetime.date(2015, 12, 1))

        expected = [
            'BulkCompleteDailyTask(d=2015-11-29)',
            'BulkCompleteDailyTask(d=2015-11-30)',
        ]

        actual = [t.task_id for t in task.requires()]
        self.assertEqual(actual, expected)

    @mock.patch('luigi.mock.MockFileSystem.listdir',
                new=mock_listdir([
                    '/data/2014/p/v/z/2014_/_03-_-21octor/20/ZOOO',
                    '/data/2014/p/v/z/2014_/_03-_-23octor/20/ZOOO',
                    '/data/2014/p/v/z/2014_/_03-_-24octor/20/ZOOO',
                ]))
    @mock.patch('luigi.mock.MockFileSystem.exists',
                new=mock_exists_always_true)
    def test_missing_tasks_correctly_required(self):
        class SomeDailyTask(luigi.Task):
            d = luigi.DateParameter()

            def output(self):
                return MockTarget(self.d.strftime('/data/2014/p/v/z/%Y_/_%m-_-%doctor/20/ZOOO'))

        task = RangeDaily(now=datetime_to_epoch(datetime.datetime(2016, 4, 1)),
                          of='SomeDailyTask',
                          start=datetime.date(2014, 3, 20),
                          task_limit=3,
                          days_back=3 * 365)
        expected = [
            'SomeDailyTask(d=2014-03-20)',
            'SomeDailyTask(d=2014-03-22)',
            'SomeDailyTask(d=2014-03-25)',
        ]
        actual = [t.task_id for t in task.requires()]
        self.assertEqual(actual, expected)


class RangeHourlyTest(unittest.TestCase):

    @mock.patch('luigi.mock.MockFileSystem.listdir', new=mock_listdir(mock_contents))  # fishy to mock the mock, but MockFileSystem doesn't support globs yet
    @mock.patch('luigi.mock.MockFileSystem.exists',
                new=mock_exists_always_true)
    def test_missing_tasks_correctly_required(self):
        for task_path in task_a_paths:
            MockTarget(task_path)
        task = RangeHourly(now=datetime_to_epoch(datetime.datetime(2016, 4, 1)),
                           of='TaskA',
                           start=datetime.datetime(2014, 3, 20, 17),
                           task_limit=3,
                           hours_back=3 * 365 * 24)  # this test takes a few seconds. Since stop is not defined, finite_datetimes constitute many years to consider
        actual = [t.task_id for t in task.requires()]
        self.assertEqual(actual, expected_a)

    @mock.patch('luigi.mock.MockFileSystem.listdir', new=mock_listdir(mock_contents))
    @mock.patch('luigi.mock.MockFileSystem.exists',
                new=mock_exists_always_true)
    def test_missing_wrapper_tasks_correctly_required(self):
        task = RangeHourly(
            now=datetime_to_epoch(datetime.datetime(2040, 4, 1)),
            of='CommonWrapperTask',
            start=datetime.datetime(2014, 3, 20, 23),
            stop=datetime.datetime(2014, 3, 21, 6),
            hours_back=30 * 365 * 24)
        actual = [t.task_id for t in task.requires()]
        self.assertEqual(actual, expected_wrapper)

    def test_bulk_complete_correctly_interfaced(self):
        class BulkCompleteHourlyTask(luigi.Task):
            dh = luigi.DateHourParameter()

            @classmethod
            def bulk_complete(cls, parameter_tuples):
                return parameter_tuples[:-2]

            def output(self):
                raise RuntimeError("Shouldn't get called while resolving deps via bulk_complete")

        task = RangeHourly(now=datetime_to_epoch(datetime.datetime(2015, 12, 1)),
                           of='BulkCompleteHourlyTask',
                           start=datetime.datetime(2015, 11, 1),
                           stop=datetime.datetime(2015, 12, 1))

        expected = [
            'BulkCompleteHourlyTask(dh=2015-11-30T22)',
            'BulkCompleteHourlyTask(dh=2015-11-30T23)',
        ]

        actual = [t.task_id for t in task.requires()]
        self.assertEqual(actual, expected)

    @mock.patch('luigi.mock.MockFileSystem.exists',
                new=mock_exists_always_false)
    def test_missing_directory(self):
        task = RangeHourly(now=datetime_to_epoch(
                           datetime.datetime(2014, 4, 1)),
                           of='TaskC',
                           start=datetime.datetime(2014, 3, 20, 23),
                           stop=datetime.datetime(2014, 3, 21, 1))
        self.assertFalse(task.complete())
        expected = [
            'TaskC(dh=2014-03-20T23)',
            'TaskC(dh=2014-03-21T00)']
        self.assertEqual([t.task_id for t in task.requires()], expected)
