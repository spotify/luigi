# -*- coding: utf-8 -*-
# Copyright (c) 2011 Spotify Ltd

from luigi.parameter import DateIntervalParameter as DI
import unittest, datetime

class DateIntervalTest(unittest.TestCase):
    def test_date(self):
        di = DI().parse('2012-01-01')
        self.assertEqual(di.dates(), [datetime.date(2012, 1, 1)])
        self.assertEqual(di.next().dates(), [datetime.date(2012, 1, 2)])
        self.assertEqual(di.prev().dates(), [datetime.date(2011, 12, 31)])
        self.assertEqual(str(di), '2012-01-01')

    def test_month(self):
        di = DI().parse('2012-01')
        self.assertEqual(di.dates(), [datetime.date(2012, 1, 1) + datetime.timedelta(i) for i in xrange(31)])
        self.assertEqual(di.next().dates(), [datetime.date(2012, 2, 1) + datetime.timedelta(i) for i in xrange(29)])
        self.assertEqual(di.prev().dates(), [datetime.date(2011, 12, 1) + datetime.timedelta(i) for i in xrange(31)])
        self.assertEqual(str(di), '2012-01')

    def test_year(self):
        di = DI().parse('2012')
        self.assertEqual(di.dates(), [datetime.date(2012, 1, 1) + datetime.timedelta(i) for i in xrange(366)])
        self.assertEqual(di.next().dates(), [datetime.date(2013, 1, 1) + datetime.timedelta(i) for i in xrange(365)])
        self.assertEqual(di.prev().dates(), [datetime.date(2011, 1, 1) + datetime.timedelta(i) for i in xrange(365)])
        self.assertEqual(str(di), '2012')

    def test_week(self):
        # >>> datetime.date(2012, 1, 1).isocalendar()
        # (2011, 52, 7)
        # >>> datetime.date(2012, 12, 31).isocalendar()
        # (2013, 1, 1)

        di = DI().parse('2011-W52')
        self.assertEqual(di.dates(), [datetime.date(2011, 12, 26) + datetime.timedelta(i) for i in xrange(7)])
        self.assertEqual(di.next().dates(), [datetime.date(2012, 1, 2) + datetime.timedelta(i) for i in xrange(7)])
        self.assertEqual(str(di), '2011-W52')

        di = DI().parse('2013-W01')
        self.assertEqual(di.dates(), [datetime.date(2012, 12, 31) + datetime.timedelta(i) for i in xrange(7)])
        self.assertEqual(di.prev().dates(), [datetime.date(2012, 12, 24) + datetime.timedelta(i) for i in xrange(7)])
        self.assertEqual(str(di), '2013-W01')

    def test_interval(self):
        di = DI().parse('2012-01-01-2012-02-01')
        self.assertEqual(di.dates(), [datetime.date(2012, 1, 1) + datetime.timedelta(i) for i in xrange(31)])
        self.assertRaises(NotImplementedError, di.next)
        self.assertRaises(NotImplementedError, di.prev)

    def test_exception(self):
        self.assertRaises(ValueError, DI().parse, 'xyz')
