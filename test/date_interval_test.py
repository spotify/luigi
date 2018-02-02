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
from helpers import LuigiTestCase, in_parse

import luigi
from luigi.parameter import DateIntervalParameter as DI


class DateIntervalTest(LuigiTestCase):

    def test_date(self):
        di = DI().parse('2012-01-01')
        self.assertEqual(di.dates(), [datetime.date(2012, 1, 1)])
        self.assertEqual(di.next().dates(), [datetime.date(2012, 1, 2)])
        self.assertEqual(di.prev().dates(), [datetime.date(2011, 12, 31)])
        self.assertEqual(str(di), '2012-01-01')

    def test_month(self):
        di = DI().parse('2012-01')
        self.assertEqual(di.dates(), [datetime.date(2012, 1, 1) + datetime.timedelta(i) for i in range(31)])
        self.assertEqual(di.next().dates(), [datetime.date(2012, 2, 1) + datetime.timedelta(i) for i in range(29)])
        self.assertEqual(di.prev().dates(), [datetime.date(2011, 12, 1) + datetime.timedelta(i) for i in range(31)])
        self.assertEqual(str(di), '2012-01')

    def test_year(self):
        di = DI().parse('2012')
        self.assertEqual(di.dates(), [datetime.date(2012, 1, 1) + datetime.timedelta(i) for i in range(366)])
        self.assertEqual(di.next().dates(), [datetime.date(2013, 1, 1) + datetime.timedelta(i) for i in range(365)])
        self.assertEqual(di.prev().dates(), [datetime.date(2011, 1, 1) + datetime.timedelta(i) for i in range(365)])
        self.assertEqual(str(di), '2012')

    def test_week(self):
        # >>> datetime.date(2012, 1, 1).isocalendar()
        # (2011, 52, 7)
        # >>> datetime.date(2012, 12, 31).isocalendar()
        # (2013, 1, 1)

        di = DI().parse('2011-W52')
        self.assertEqual(di.dates(), [datetime.date(2011, 12, 26) + datetime.timedelta(i) for i in range(7)])
        self.assertEqual(di.next().dates(), [datetime.date(2012, 1, 2) + datetime.timedelta(i) for i in range(7)])
        self.assertEqual(str(di), '2011-W52')

        di = DI().parse('2013-W01')
        self.assertEqual(di.dates(), [datetime.date(2012, 12, 31) + datetime.timedelta(i) for i in range(7)])
        self.assertEqual(di.prev().dates(), [datetime.date(2012, 12, 24) + datetime.timedelta(i) for i in range(7)])
        self.assertEqual(str(di), '2013-W01')

    def test_interval(self):
        di = DI().parse('2012-01-01-2012-02-01')
        self.assertEqual(di.dates(), [datetime.date(2012, 1, 1) + datetime.timedelta(i) for i in range(31)])
        self.assertRaises(NotImplementedError, di.next)
        self.assertRaises(NotImplementedError, di.prev)
        self.assertEqual(di.to_string(), '2012-01-01-2012-02-01')

    def test_exception(self):
        self.assertRaises(ValueError, DI().parse, 'xyz')

    def test_comparison(self):
        a = DI().parse('2011')
        b = DI().parse('2013')
        c = DI().parse('2012')
        self.assertTrue(a < b)
        self.assertTrue(a < c)
        self.assertTrue(b > c)
        d = DI().parse('2012')
        self.assertTrue(d == c)
        self.assertEqual(d, min(c, b))
        self.assertEqual(3, len({a, b, c, d}))

    def test_comparison_different_types(self):
        x = DI().parse('2012')
        y = DI().parse('2012-01-01-2013-01-01')
        self.assertRaises(TypeError, lambda: x == y)

    def test_parameter_parse_and_default(self):
        month = luigi.date_interval.Month(2012, 11)
        other = luigi.date_interval.Month(2012, 10)

        class MyTask(luigi.Task):
            di = DI(default=month)

        class MyTaskNoDefault(luigi.Task):
            di = DI()

        self.assertEqual(MyTask().di, month)
        in_parse(["MyTask", "--di", "2012-10"],
                 lambda task: self.assertEqual(task.di, other))
        task = MyTask(month)
        self.assertEqual(task.di, month)
        task = MyTask(di=month)
        self.assertEqual(task.di, month)
        task = MyTask(other)
        self.assertNotEqual(task.di, month)

        def fail1():
            return MyTaskNoDefault()
        self.assertRaises(luigi.parameter.MissingParameterException, fail1)

        in_parse(["MyTaskNoDefault", "--di", "2012-10"],
                 lambda task: self.assertEqual(task.di, other))

    def test_hours(self):
        d = DI().parse('2015')
        self.assertEqual(len(list(d.hours())), 24 * 365)

    def test_cmp(self):
        operators = [lambda x, y: x == y,
                     lambda x, y: x != y,
                     lambda x, y: x < y,
                     lambda x, y: x > y,
                     lambda x, y: x <= y,
                     lambda x, y: x >= y]

        dates = [(1, 30, DI().parse('2015-01-01-2015-01-30')),
                 (1, 15, DI().parse('2015-01-01-2015-01-15')),
                 (10, 20, DI().parse('2015-01-10-2015-01-20')),
                 (20, 30, DI().parse('2015-01-20-2015-01-30'))]

        for from_a, to_a, di_a in dates:
            for from_b, to_b, di_b in dates:
                for op in operators:
                    self.assertEqual(
                        op((from_a, to_a), (from_b, to_b)),
                        op(di_a, di_b))
