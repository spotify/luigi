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
                 lambda: self.assertEqual(DateTask().day, datetime.date(2015, 4, 3)))

    def test_serialize_task(self):
        t = DateTask(datetime.date(2015, 4, 3))
        self.assertEqual(str(t), 'DateTask(day=2015-04-03)')


class DateHourParameterTest(unittest.TestCase):
    def test_parse(self):
        dh = luigi.DateHourParameter().parse('2013-02-01T18')
        self.assertEqual(dh, datetime.datetime(2013, 2, 1, 18, 0, 0))

    def test_serialize(self):
        dh = luigi.DateHourParameter().serialize(datetime.datetime(2013, 2, 1, 18, 0, 0))
        self.assertEqual(dh, '2013-02-01T18')

    def test_parse_interface(self):
        in_parse(["DateHourTask", "--dh", "2013-02-01T18"],
                 lambda: self.assertEqual(DateHourTask().dh, datetime.datetime(2013, 2, 1, 18, 0, 0)))

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
                 lambda: self.assertEqual(DateMinuteTask().dm, datetime.datetime(2013, 2, 1, 18, 42, 0)))

    def test_serialize_task(self):
        t = DateMinuteTask(datetime.datetime(2013, 2, 1, 18, 42, 0))
        self.assertEqual(str(t), 'DateMinuteTask(dm=2013-02-01T1842)')


class MonthParameterTest(unittest.TestCase):
    def test_parse(self):
        m = luigi.MonthParameter().parse('2015-04')
        self.assertEqual(m, datetime.date(2015, 4, 1))

    def test_serialize(self):
        m = luigi.MonthParameter().serialize(datetime.date(2015, 4, 3))
        self.assertEqual(m, '2015-04')

    def test_parse_interface(self):
        in_parse(["MonthTask", "--month", "2015-04"],
                 lambda: self.assertEqual(MonthTask().month, datetime.date(2015, 4, 1)))

    def test_serialize_task(self):
        task = MonthTask(datetime.date(2015, 4, 3))
        self.assertEqual(str(task), 'MonthTask(month=2015-04)')


class YearParameterTest(unittest.TestCase):
    def test_parse(self):
        year = luigi.YearParameter().parse('2015')
        self.assertEqual(year, datetime.date(2015, 1, 1))

    def test_serialize(self):
        year = luigi.YearParameter().serialize(datetime.date(2015, 4, 3))
        self.assertEqual(year, '2015')

    def test_parse_interface(self):
        in_parse(["YearTask", "--year", "2015"],
                 lambda: self.assertEqual(YearTask().year, datetime.date(2015, 1, 1)))

    def test_serialize_task(self):
        task = YearTask(datetime.date(2015, 4, 3))
        self.assertEqual(str(task), 'YearTask(year=2015)')
