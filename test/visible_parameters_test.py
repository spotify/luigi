import luigi
from helpers import unittest


class TestTask1(luigi.Task):
    param_one = luigi.Parameter(default='one', visible=False)
    param_two = luigi.Parameter(default='two')
    param_three = luigi.Parameter(default='three', visible=False)
    param_four = luigi.Parameter(default='four', significant=False)
    param_five = luigi.Parameter(default='five', visible=False, significant=False)


class TestTask2(luigi.Task):
    param_one = luigi.Parameter(default='1', visible=False)
    param_two = luigi.Parameter(default='2', visible=False)
    param_three = luigi.Parameter(default='3', visible=False)
    param_four = luigi.Parameter(default='4', visible=False)
    param_five = luigi.Parameter(default='5', visible=False)


class TestTask3(luigi.Task):
    param_one = luigi.Parameter(default='one')
    param_two = luigi.Parameter(default='two')
    param_three = luigi.Parameter(default='three')
    param_four = luigi.Parameter(default='four', significant=False)
    param_five = luigi.Parameter(default='five', significant=False)


class Test(unittest.TestCase):
    def test_task_visible_vs_invisible(self):
        task1 = TestTask1()
        task3 = TestTask3()

        self.assertEqual(task1.to_str_params(), task3.to_str_params())

    def test_task_visible_vs_invisible_using_only_significant(self):
        task1 = TestTask1()
        task3 = TestTask3()

        self.assertEqual(task1.to_str_params(only_significant=True), task3.to_str_params(only_significant=True))

    def test_task_params(self):
        task = TestTask1()

        self.assertEqual(str(task), 'TestTask1(param_one=one, param_two=two, param_three=three)')

    def test_similar_task_to_str_equality(self):
        task1 = TestTask1()
        task2 = TestTask1()

        self.assertEqual(task1.to_str_params(), task2.to_str_params())

    def test_only_visible(self):
        task = TestTask1()

        self.assertEqual(task.to_str_params(only_visible=True), {'param_two': 'two', 'param_four': 'four'})

    def test_to_str(self):
        task = TestTask2()

        self.assertEqual(task.to_str_params(), {'param_one': '1', 'param_two': '2', 'param_three': '3', 'param_four': '4', 'param_five': '5'})

    def test_to_str_all_params_invisible(self):
        task = TestTask2()

        self.assertEqual(task.to_str_params(only_visible=True), {})