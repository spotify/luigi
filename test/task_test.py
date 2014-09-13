import doctest
import unittest

import luigi.task
import luigi
from datetime import datetime, timedelta


class DummyTask(luigi.Task):

    param = luigi.Parameter()
    bool_param = luigi.BooleanParameter()
    int_param = luigi.IntParameter()
    float_param = luigi.FloatParameter()
    date_param = luigi.DateParameter()
    datehour_param = luigi.DateHourParameter()
    timedelta_param = luigi.TimeDeltaParameter()
    list_param = luigi.Parameter(is_list=True)


class TaskTest(unittest.TestCase):

    def test_tasks_doctest(self):
        doctest.testmod(luigi.task)

    def test_task_to_str_to_task(self):
        params = dict(
            param='test',
            bool_param=True,
            int_param=666,
            float_param=123.456,
            date_param=datetime(2014, 9, 13).date(),
            datehour_param=datetime(2014, 9, 13, 9),
            timedelta_param=timedelta(44),  # doesn't support seconds
            list_param=['in', 'flames'])

        original = DummyTask(**params)
        other = DummyTask.from_str_params(original.to_str_params(), {})
        self.assertEqual(original, other)


if __name__ == '__main__':
    unittest.main()
