# -*- coding: utf-8 -*-
# Copyright (c) 2011 Spotify Ltd

from luigi.parameter import DateIntervalParameter as DI
import unittest, datetime

class DateIntervalTest(unittest.TestCase):
    def test_date(self):
        dates = DI().parse('2012-01-01').dates()
        self.assertEqual(dates, [datetime.date(2012, 1, 1)])

    def test_month(self):
        dates = DI().parse('2012-01').dates()
        self.assertEqual(dates, [datetime.date(2012, 1, 1) + datetime.timedelta(i) for i in xrange(31)])

    def test_year(self):
        dates = DI().parse('2012').dates()
        self.assertEqual(dates, [datetime.date(2012, 1, 1) + datetime.timedelta(i) for i in xrange(366)])

    def test_week(self):
        # >>> datetime.date(2012, 1, 1).isocalendar()
        # (2011, 52, 7)
        # >>> datetime.date(2012, 12, 31).isocalendar()
        # (2013, 1, 1)

        dates = DI().parse('2011-W52').dates()
        self.assertEqual(dates, [datetime.date(2011, 12, 26) + datetime.timedelta(i) for i in xrange(7)])

        dates = DI().parse('2013-W01').dates()
        self.assertEqual(dates, [datetime.date(2012, 12, 31) + datetime.timedelta(i) for i in xrange(7)])

    def test_interval(self):
        dates = DI().parse('2012-01-01-2012-02-01').dates()
        self.assertEqual(dates, [datetime.date(2012, 1, 1) + datetime.timedelta(i) for i in xrange(31)])

    def test_exception(self):
        self.assertRaises(ValueError, DI().parse, 'xyz')
