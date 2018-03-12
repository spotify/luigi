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
from helpers import unittest

import luigi
import luigi.date_interval
from luigi.util import get_previous_completed, previous


class DateTaskOk(luigi.Task):
    date = luigi.DateParameter()

    def complete(self):
        # test against 2000.03.01
        return self.date in [datetime.date(2000, 2, 25), datetime.date(2000, 3, 1), datetime.date(2000, 3, 2)]


class DateTaskOkTest(unittest.TestCase):

    def test_previous(self):
        task = DateTaskOk(datetime.date(2000, 3, 1))
        prev = previous(task)
        self.assertEqual(prev.date, datetime.date(2000, 2, 29))

    def test_get_previous_completed(self):
        task = DateTaskOk(datetime.date(2000, 3, 1))
        prev = get_previous_completed(task, 5)
        self.assertEqual(prev.date, datetime.date(2000, 2, 25))

    def test_get_previous_completed_not_found(self):
        task = DateTaskOk(datetime.date(2000, 3, 1))
        prev = get_previous_completed(task, 4)
        self.assertEqual(None, prev)


class DateHourTaskOk(luigi.Task):
    hour = luigi.DateHourParameter()

    def complete(self):
        # test against 2000.03.01T02
        return self.hour in [datetime.datetime(2000, 2, 29, 22), datetime.datetime(2000, 3, 1, 2), datetime.datetime(2000, 3, 1, 3)]


class DateHourTaskOkTest(unittest.TestCase):

    def test_previous(self):
        task = DateHourTaskOk(datetime.datetime(2000, 3, 1, 2))
        prev = previous(task)
        self.assertEqual(prev.hour, datetime.datetime(2000, 3, 1, 1))

    def test_get_previous_completed(self):
        task = DateHourTaskOk(datetime.datetime(2000, 3, 1, 2))
        prev = get_previous_completed(task, 4)
        self.assertEqual(prev.hour, datetime.datetime(2000, 2, 29, 22))

    def test_get_previous_completed_not_found(self):
        task = DateHourTaskOk(datetime.datetime(2000, 3, 1, 2))
        prev = get_previous_completed(task, 3)
        self.assertEqual(None, prev)


class DateMinuteTaskOk(luigi.Task):
    minute = luigi.DateMinuteParameter()

    def complete(self):
        # test against 2000.03.01T02H03
        return self.minute in [datetime.datetime(2000, 3, 1, 2, 0)]


class DateMinuteTaskOkTest(unittest.TestCase):

    def test_previous(self):
        task = DateMinuteTaskOk(datetime.datetime(2000, 3, 1, 2, 3))
        prev = previous(task)
        self.assertEqual(prev.minute, datetime.datetime(2000, 3, 1, 2, 2))

    def test_get_previous_completed(self):
        task = DateMinuteTaskOk(datetime.datetime(2000, 3, 1, 2, 3))
        prev = get_previous_completed(task, 3)
        self.assertEqual(prev.minute, datetime.datetime(2000, 3, 1, 2, 0))

    def test_get_previous_completed_not_found(self):
        task = DateMinuteTaskOk(datetime.datetime(2000, 3, 1, 2, 3))
        prev = get_previous_completed(task, 2)
        self.assertEqual(None, prev)


class DateSecondTaskOk(luigi.Task):
    second = luigi.DateSecondParameter()

    def complete(self):
        return self.second in [datetime.datetime(2000, 3, 1, 2, 3, 4)]


class DateSecondTaskOkTest(unittest.TestCase):

    def test_previous(self):
        task = DateSecondTaskOk(datetime.datetime(2000, 3, 1, 2, 3, 7))
        prev = previous(task)
        self.assertEqual(prev.second, datetime.datetime(2000, 3, 1, 2, 3, 6))

    def test_get_previous_completed(self):
        task = DateSecondTaskOk(datetime.datetime(2000, 3, 1, 2, 3, 7))
        prev = get_previous_completed(task, 3)
        self.assertEqual(prev.second, datetime.datetime(2000, 3, 1, 2, 3, 4))

    def test_get_previous_completed_not_found(self):
        task = DateSecondTaskOk(datetime.datetime(2000, 3, 1, 2, 3))
        prev = get_previous_completed(task, 2)
        self.assertEqual(None, prev)


class DateIntervalTaskOk(luigi.Task):
    interval = luigi.DateIntervalParameter()

    def complete(self):
        return self.interval in [luigi.date_interval.Week(1999, 48), luigi.date_interval.Week(2000, 1), luigi.date_interval.Week(2000, 2)]


class DateIntervalTaskOkTest(unittest.TestCase):

    def test_previous(self):
        task = DateIntervalTaskOk(luigi.date_interval.Week(2000, 1))
        prev = previous(task)
        self.assertEqual(prev.interval, luigi.date_interval.Week(1999, 52))

    def test_get_previous_completed(self):
        task = DateIntervalTaskOk(luigi.date_interval.Week(2000, 1))
        prev = get_previous_completed(task, 5)
        self.assertEqual(prev.interval, luigi.date_interval.Week(1999, 48))

    def test_get_previous_completed_not_found(self):
        task = DateIntervalTaskOk(luigi.date_interval.Week(2000, 1))
        prev = get_previous_completed(task, 4)
        self.assertEqual(None, prev)


class ExtendedDateTaskOk(DateTaskOk):
    param1 = luigi.Parameter()
    param2 = luigi.IntParameter(default=2)


class ExtendedDateTaskOkTest(unittest.TestCase):

    def test_previous(self):
        task = ExtendedDateTaskOk(datetime.date(2000, 3, 1), "some value")
        prev = previous(task)
        self.assertEqual(prev.date, datetime.date(2000, 2, 29))
        self.assertEqual(prev.param1, "some value")
        self.assertEqual(prev.param2, 2)


class MultiTemporalTaskNok(luigi.Task):
    date = luigi.DateParameter()
    hour = luigi.DateHourParameter()


class MultiTemporalTaskNokTest(unittest.TestCase):

    def test_previous(self):
        task = MultiTemporalTaskNok(datetime.date(2000, 1, 1), datetime.datetime(2000, 1, 1, 1))
        self.assertRaises(NotImplementedError, previous, task)
        self.assertRaises(NotImplementedError, get_previous_completed, task)


class NoTemporalTaskNok(luigi.Task):
    param = luigi.Parameter()


class NoTemporalTaskNokTest(unittest.TestCase):

    def test_previous(self):
        task = NoTemporalTaskNok("some value")
        self.assertRaises(NotImplementedError, previous, task)
        self.assertRaises(NotImplementedError, get_previous_completed, task)
