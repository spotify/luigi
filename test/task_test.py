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

    def test_id_to_name_and_params(self):
        task_id = "InputText(date=2014-12-29)"
        (name, params) = luigi.task.id_to_name_and_params(task_id)
        self.assertEquals(name, "InputText")
        self.assertEquals(params, dict(date="2014-12-29"))

    def test_id_to_name_and_params_multiple_args(self):
        task_id = "InputText(date=2014-12-29,foo=bar)"
        (name, params) = luigi.task.id_to_name_and_params(task_id)
        self.assertEquals(name, "InputText")
        self.assertEquals(params, dict(date="2014-12-29", foo="bar"))

    def test_id_to_name_and_params_list_args(self):
        task_id = "InputText(date=2014-12-29,foo=[bar,baz-foo])"
        (name, params) = luigi.task.id_to_name_and_params(task_id)
        self.assertEquals(name, "InputText")
        self.assertEquals(params, dict(date="2014-12-29", foo=["bar","baz-foo"]))

if __name__ == '__main__':
    unittest.main()
