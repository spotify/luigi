import luigi
from helpers import unittest


class TestTask1(luigi.Task):
    param_one = luigi.Parameter(default='one', visible=2)
    param_two = luigi.Parameter(default='two')
    param_three = luigi.Parameter(default='three', visible=2)
    param_four = luigi.Parameter(default='four', significant=False, visible=1)
    param_five = luigi.Parameter(default='five', visible=2, significant=False)


class TestTask2(luigi.Task):
    param_one = luigi.Parameter(default='1', visible=2)
    param_two = luigi.Parameter(default='2', visible=2)
    param_three = luigi.Parameter(default='3', visible=2)
    param_four = luigi.Parameter(default='4', visible=2)
    param_five = luigi.Parameter(default='5', visible=2)


class TestTask3(luigi.Task):
    param_one = luigi.Parameter(default='one', visible=2)
    param_two = luigi.Parameter(default='two')
    param_three = luigi.Parameter(default='three', visible=2)
    param_four = luigi.Parameter(default='four', significant=False, visible=1)
    param_five = luigi.Parameter(default='five', significant=False, visible=2)


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

        self.assertEqual(task.to_str_params(), {'param_two': 'two', 'param_four': 'four'})

    def test_to_str(self):
        task = TestTask2()

        self.assertEqual(task.to_str_params(), {})

    def test_to_str_all_params_invisible(self):
        task = TestTask2()

        self.assertEqual(task.to_str_params(), {})