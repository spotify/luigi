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
from helpers import unittest, in_parse

import luigi
import luigi.interface


class DateTask(luigi.Task):
    day = luigi.DateParameter()


class DateHourTask(luigi.Task):
    dh = luigi.DateHourParameter()


class DateMinuteTask(luigi.Task):
    dm = luigi.DateMinuteParameter()


class DateSecondTask(luigi.Task):
    ds = luigi.DateSecondParameter()


class MonthTask(luigi.Task):
    month = luigi.MonthParameter()


class YearTask(luigi.Task):
    year = luigi.YearParameter()


class DateParameterTest(unittest.TestCase):
    def test_parse(self):
        d = luigi.DateParameter().parse('2015-04-03')
        self.assertEqual(d, datetime.date(2015, 4, 3))

    def test_serialize(self):
        d = luigi.DateParameter().serialize(datetime.date(2015, 4, 3))
        self.assertEqual(d, '2015-04-03')

    def test_parse_interface(self):
        in_parse(["DateTask", "--day", "2015-04-03"],
                 lambda task: self.assertEqual(task.day, datetime.date(2015, 4, 3)))

    def test_serialize_task(self):
        t = DateTask(datetime.date(2015, 4, 3))
        self.assertEqual(str(t), 'DateTask(day=2015-04-03)')


class DateHourParameterTest(unittest.TestCase):
    def test_parse(self):
        dh = luigi.DateHourParameter().parse('2013-02-01T18')
        self.assertEqual(dh, datetime.datetime(2013, 2, 1, 18, 0, 0))

    def test_date_to_dh(self):
        date = luigi.DateHourParameter().normalize(datetime.date(2000, 1, 1))
        self.assertEqual(date, datetime.datetime(2000, 1, 1, 0))

    def test_serialize(self):
        dh = luigi.DateHourParameter().serialize(datetime.datetime(2013, 2, 1, 18, 0, 0))
        self.assertEqual(dh, '2013-02-01T18')

    def test_parse_interface(self):
        in_parse(["DateHourTask", "--dh", "2013-02-01T18"],
                 lambda task: self.assertEqual(task.dh, datetime.datetime(2013, 2, 1, 18, 0, 0)))

    def test_serialize_task(self):
        t = DateHourTask(datetime.datetime(2013, 2, 1, 18, 0, 0))
        self.assertEqual(str(t), 'DateHourTask(dh=2013-02-01T18)')


class DateMinuteParameterTest(unittest.TestCase):
    def test_parse(self):
        dm = luigi.DateMinuteParameter().parse('2013-02-01T1842')
        self.assertEqual(dm, datetime.datetime(2013, 2, 1, 18, 42, 0))

    def test_parse_padding_zero(self):
        dm = luigi.DateMinuteParameter().parse('2013-02-01T1807')
        self.assertEqual(dm, datetime.datetime(2013, 2, 1, 18, 7, 0))

    def test_parse_deprecated(self):
        with self.assertWarnsRegex(DeprecationWarning,
                                   'Using "H" between hours and minutes is deprecated, omit it instead.'):
            dm = luigi.DateMinuteParameter().parse('2013-02-01T18H42')
        self.assertEqual(dm, datetime.datetime(2013, 2, 1, 18, 42, 0))

    def test_serialize(self):
        dm = luigi.DateMinuteParameter().serialize(datetime.datetime(2013, 2, 1, 18, 42, 0))
        self.assertEqual(dm, '2013-02-01T1842')

    def test_serialize_padding_zero(self):
        dm = luigi.DateMinuteParameter().serialize(datetime.datetime(2013, 2, 1, 18, 7, 0))
        self.assertEqual(dm, '2013-02-01T1807')

    def test_parse_interface(self):
        in_parse(["DateMinuteTask", "--dm", "2013-02-01T1842"],
                 lambda task: self.assertEqual(task.dm, datetime.datetime(2013, 2, 1, 18, 42, 0)))

    def test_serialize_task(self):
        t = DateMinuteTask(datetime.datetime(2013, 2, 1, 18, 42, 0))
        self.assertEqual(str(t), 'DateMinuteTask(dm=2013-02-01T1842)')


class DateSecondParameterTest(unittest.TestCase):
    def test_parse(self):
        ds = luigi.DateSecondParameter().parse('2013-02-01T184227')
        self.assertEqual(ds, datetime.datetime(2013, 2, 1, 18, 42, 27))

    def test_serialize(self):
        ds = luigi.DateSecondParameter().serialize(datetime.datetime(2013, 2, 1, 18, 42, 27))
        self.assertEqual(ds, '2013-02-01T184227')

    def test_parse_interface(self):
        in_parse(["DateSecondTask", "--ds", "2013-02-01T184227"],
                 lambda task: self.assertEqual(task.ds, datetime.datetime(2013, 2, 1, 18, 42, 27)))

    def test_serialize_task(self):
        t = DateSecondTask(datetime.datetime(2013, 2, 1, 18, 42, 27))
        self.assertEqual(str(t), 'DateSecondTask(ds=2013-02-01T184227)')


class MonthParameterTest(unittest.TestCase):
    def test_parse(self):
        m = luigi.MonthParameter().parse('2015-04')
        self.assertEqual(m, datetime.date(2015, 4, 1))

    def test_construct_month_interval(self):
        m = MonthTask(luigi.date_interval.Month(2015, 4))
        self.assertEqual(m.month, datetime.date(2015, 4, 1))

    def test_month_interval_default(self):
        class MonthDefaultTask(luigi.task.Task):
            month = luigi.MonthParameter(default=luigi.date_interval.Month(2015, 4))

        m = MonthDefaultTask()
        self.assertEqual(m.month, datetime.date(2015, 4, 1))

    def test_serialize(self):
        m = luigi.MonthParameter().serialize(datetime.date(2015, 4, 3))
        self.assertEqual(m, '2015-04')

    def test_parse_interface(self):
        in_parse(["MonthTask", "--month", "2015-04"],
                 lambda task: self.assertEqual(task.month, datetime.date(2015, 4, 1)))

    def test_serialize_task(self):
        task = MonthTask(datetime.date(2015, 4, 3))
        self.assertEqual(str(task), 'MonthTask(month=2015-04)')


class YearParameterTest(unittest.TestCase):
    def test_parse(self):
        year = luigi.YearParameter().parse('2015')
        self.assertEqual(year, datetime.date(2015, 1, 1))

    def test_construct_year_interval(self):
        y = YearTask(luigi.date_interval.Year(2015))
        self.assertEqual(y.year, datetime.date(2015, 1, 1))

    def test_year_interval_default(self):
        class YearDefaultTask(luigi.task.Task):
            year = luigi.YearParameter(default=luigi.date_interval.Year(2015))

        m = YearDefaultTask()
        self.assertEqual(m.year, datetime.date(2015, 1, 1))

    def test_serialize(self):
        year = luigi.YearParameter().serialize(datetime.date(2015, 4, 3))
        self.assertEqual(year, '2015')

    def test_parse_interface(self):
        in_parse(["YearTask", "--year", "2015"],
                 lambda task: self.assertEqual(task.year, datetime.date(2015, 1, 1)))

    def test_serialize_task(self):
        task = YearTask(datetime.date(2015, 4, 3))
        self.assertEqual(str(task), 'YearTask(year=2015)')
