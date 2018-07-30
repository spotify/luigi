import luigi
from luigi.parameter import ParameterVisibility
from helpers import unittest
import json


class TestTask1(luigi.Task):
    param_one = luigi.Parameter(default='1', visibility=ParameterVisibility.HIDDEN, significant=True)
    param_two = luigi.Parameter(default='2', significant=True)
    param_three = luigi.Parameter(default='3', visibility=ParameterVisibility.PRIVATE, significant=True)


class TestTask2(luigi.Task):
    param_one = luigi.Parameter(default='1', visibility=ParameterVisibility.PRIVATE)
    param_two = luigi.Parameter(default='2', visibility=ParameterVisibility.PRIVATE)
    param_three = luigi.Parameter(default='3', visibility=ParameterVisibility.PRIVATE)


class TestTask3(luigi.Task):
    param_one = luigi.Parameter(default='1', visibility=ParameterVisibility.HIDDEN, significant=True)
    param_two = luigi.Parameter(default='2', visibility=ParameterVisibility.HIDDEN, significant=False)
    param_three = luigi.Parameter(default='3', visibility=ParameterVisibility.HIDDEN, significant=True)


class TestTask4(luigi.Task):
    param_one = luigi.Parameter(default='1', visibility=ParameterVisibility.PUBLIC, significant=True)
    param_two = luigi.Parameter(default='2', visibility=ParameterVisibility.PUBLIC, significant=False)
    param_three = luigi.Parameter(default='3', visibility=ParameterVisibility.PUBLIC, significant=True)


class Test(unittest.TestCase):
    def test_to_str_params(self):
        task = TestTask1()

        self.assertEqual(task.to_str_params(), {'param_one': '1', 'param_two': '2'})

        task = TestTask2()

        self.assertEqual(task.to_str_params(), {})

        task = TestTask3()

        self.assertEqual(task.to_str_params(), {'param_one': '1', 'param_two': '2', 'param_three': '3'})

    def test_all_public_equals_all_hidden(self):
        hidden = TestTask3()
        public = TestTask4()

        self.assertEqual(public.to_str_params(), hidden.to_str_params())

    def test_all_public_equals_all_hidden_using_significant(self):
        hidden = TestTask3()
        public = TestTask4()

        self.assertEqual(public.to_str_params(only_significant=True), hidden.to_str_params(only_significant=True))

    def test_private_params_and_significant(self):
        task = TestTask1()

        self.assertEqual(task.to_str_params(), task.to_str_params(only_significant=True))

    def test_param_visibilities(self):
        task = TestTask1()

        self.assertEqual(task._get_param_visibilities(), {'param_one': 1, 'param_two': 0})

    def test_incorrect_visibility_value(self):
        class Task(luigi.Task):
            a = luigi.Parameter(default='val', visibility=5)

        task = Task()

        self.assertEqual(task._get_param_visibilities(), {'a': 0})

    def test_task_id_exclude_hidden_and_private_params(self):
        task = TestTask1()

        self.assertEqual({'param_two': '2'}, task.to_str_params(only_public=True))

    def test_json_dumps(self):
        public = json.dumps(ParameterVisibility.PUBLIC.serialize())
        hidden = json.dumps(ParameterVisibility.HIDDEN.serialize())
        private = json.dumps(ParameterVisibility.PRIVATE.serialize())

        self.assertEqual('0', public)
        self.assertEqual('1', hidden)
        self.assertEqual('2', private)

        public = json.loads(public)
        hidden = json.loads(hidden)
        private = json.loads(private)

        self.assertEqual(0, public)
        self.assertEqual(1, hidden)
        self.assertEqual(2, private)
