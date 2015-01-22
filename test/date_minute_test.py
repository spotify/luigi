import luigi
import luigi.interface
import unittest
import datetime


class DateMinuteTask(luigi.Task):
    dh = luigi.DateMinuteParameter()


class DateMinuteTest(unittest.TestCase):
    def test_parse(self):
        dh = luigi.DateMinuteParameter().parse('2013-01-01T18H42')
        self.assertEqual(dh, datetime.datetime(2013, 1, 1, 18, 42, 0))

    def test_parse_padding_zero(self):
        dh = luigi.DateMinuteParameter().parse('2013-01-01T18H07')
        self.assertEqual(dh, datetime.datetime(2013, 1, 1, 18, 07, 0))

    def test_serialize(self):
        dh = luigi.DateMinuteParameter().serialize(datetime.datetime(2013, 1, 1, 18, 42, 0))
        self.assertEqual(dh, '2013-01-01T18H42')

    def test_serialize_padding_zero(self):
        dh = luigi.DateMinuteParameter().serialize(datetime.datetime(2013, 1, 1, 18, 07, 0))
        self.assertEqual(dh, '2013-01-01T18H07')

    def test_parse_interface(self):
        task = luigi.interface.ArgParseInterface().parse(["DateMinuteTask", "--dh", "2013-01-01T18H42"])[0]
        self.assertEqual(task.dh, datetime.datetime(2013, 1, 1, 18, 42, 0))

    def test_serialize_task(self):
        t = DateMinuteTask(datetime.datetime(2013, 1, 1, 18, 42, 0))
        self.assertEqual(str(t), 'DateMinuteTask(dh=2013-01-01T18H42)')
