import unittest
import datetime

import luigi
import luigi.date_interval


class DateTaskOk(luigi.Task):
    date = luigi.DateParameter()

    def complete(self):
        # test against 2000.03.01
        return self.date in [datetime.date(2000, 2, 25), datetime.date(2000, 3, 1), datetime.date(2000, 3, 2)]


class DateTaskOkTest(unittest.TestCase):
    def test_previous(self):
        task = DateTaskOk(datetime.date(2000, 3, 1))
        previous = task.previous()
        self.assertEqual(previous.date, datetime.date(2000, 2, 29))

    def test_get_previous_completed(self):
        task = DateTaskOk(datetime.date(2000, 3, 1))
        previous = task.get_previous_completed(5)
        self.assertEqual(previous.date, datetime.date(2000, 2, 25))

    def test_get_previous_completed_not_found(self):
        task = DateTaskOk(datetime.date(2000, 3, 1))
        previous = task.get_previous_completed(4)
        self.assertIsNone(previous)


class DateHourTaskOk(luigi.Task):
    hour = luigi.DateHourParameter()

    def complete(self):
        # test against 2000.03.01T02
        return self.hour in [datetime.datetime(2000, 2, 29, 22), datetime.datetime(2000, 3, 1, 2), datetime.datetime(2000, 3, 1, 3)]


class DateHourTaskOkTest(unittest.TestCase):
    def test_previous(self):
        task = DateHourTaskOk(datetime.datetime(2000, 3, 1, 2))
        previous = task.previous()
        self.assertEqual(previous.hour, datetime.datetime(2000, 3, 1, 1))

    def test_get_previous_completed(self):
        task = DateHourTaskOk(datetime.datetime(2000, 3, 1, 2))
        previous = task.get_previous_completed(4)
        self.assertEqual(previous.hour, datetime.datetime(2000, 2, 29, 22))

    def test_get_previous_completed_not_found(self):
        task = DateHourTaskOk(datetime.datetime(2000, 3, 1, 2))
        previous = task.get_previous_completed(3)
        self.assertIsNone(previous)


class DateIntervalTaskOk(luigi.Task):
    interval = luigi.DateIntervalParameter()

    def complete(self):
        return self.interval in [luigi.date_interval.Week(1999, 48), luigi.date_interval.Week(2000, 1), luigi.date_interval.Week(2000, 2)]


class DateIntervalTaskOkTest(unittest.TestCase):
    def test_previous(self):
        task = DateIntervalTaskOk(luigi.date_interval.Week(2000, 1))
        previous = task.previous()
        self.assertEqual(previous.interval, luigi.date_interval.Week(1999, 52))

    def test_get_previous_completed(self):
        task = DateIntervalTaskOk(luigi.date_interval.Week(2000, 1))
        previous = task.get_previous_completed(5)
        self.assertEqual(previous.interval, luigi.date_interval.Week(1999, 48))

    def test_get_previous_completed_not_found(self):
        task = DateIntervalTaskOk(luigi.date_interval.Week(2000, 1))
        previous = task.get_previous_completed(4)
        self.assertIsNone(previous)


class ExtendedDateTaskOk(DateTaskOk):
    param1 = luigi.Parameter()
    param2 = luigi.IntParameter(default=2)


class ExtendedDateTaskOkTest(unittest.TestCase):
    def test_previous(self):
        task = ExtendedDateTaskOk(datetime.date(2000, 3, 1), "some value")
        previous = task.previous()
        self.assertEqual(previous.date, datetime.date(2000, 2, 29))
        self.assertEqual(previous.param1, "some value")
        self.assertEqual(previous.param2, 2)


class MultiTemporalTaskNok(luigi.Task):
    date = luigi.DateParameter()
    hour = luigi.DateHourParameter()


class MultiTemporalTaskNokTest(unittest.TestCase):
    def test_previous(self):
        task = MultiTemporalTaskNok(datetime.date(2000, 1, 1), datetime.datetime(2000, 1, 1, 1))
        self.assertRaises(NotImplementedError, task.previous)
        self.assertRaises(NotImplementedError, task.get_previous_completed)


class NoTemporalTaskNok(luigi.Task):
    param = luigi.Parameter()


class NoTemporalTaskNokTest(unittest.TestCase):
    def test_previous(self):
        task = NoTemporalTaskNok("some value")
        self.assertRaises(NotImplementedError, task.previous)
        self.assertRaises(NotImplementedError, task.get_previous_completed)
