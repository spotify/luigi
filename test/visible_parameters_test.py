import json

from helpers import unittest

import luigi
from luigi.parameter import ParameterVisibility


class TestTask1(luigi.Task):
    param_one = luigi.Parameter(default="1", visibility=ParameterVisibility.HIDDEN, significant=True)
    param_two = luigi.Parameter(default="2", significant=True)
    param_three = luigi.Parameter(default="3", visibility=ParameterVisibility.PRIVATE, significant=True)


class TestTask2(luigi.Task):
    param_one = luigi.Parameter(default="1", visibility=ParameterVisibility.PRIVATE)
    param_two = luigi.Parameter(default="2", visibility=ParameterVisibility.PRIVATE)
    param_three = luigi.Parameter(default="3", visibility=ParameterVisibility.PRIVATE)


class TestTask3(luigi.Task):
    param_one = luigi.Parameter(default="1", visibility=ParameterVisibility.HIDDEN, significant=True)
    param_two = luigi.Parameter(default="2", visibility=ParameterVisibility.HIDDEN, significant=False)
    param_three = luigi.Parameter(default="3", visibility=ParameterVisibility.HIDDEN, significant=True)


class TestTask4(luigi.Task):
    param_one = luigi.Parameter(default="1", visibility=ParameterVisibility.PUBLIC, significant=True)
    param_two = luigi.Parameter(default="2", visibility=ParameterVisibility.PUBLIC, significant=False)
    param_three = luigi.Parameter(default="3", visibility=ParameterVisibility.PUBLIC, significant=True)


class Test(unittest.TestCase):
    def test_to_str_params(self):
        task = TestTask1()

        self.assertEqual(task.to_str_params(), {"param_one": "1", "param_two": "2"})

        task = TestTask2()

        self.assertEqual(task.to_str_params(), {})

        task = TestTask3()

        self.assertEqual(task.to_str_params(), {"param_one": "1", "param_two": "2", "param_three": "3"})

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

        self.assertEqual(task._get_param_visibilities(), {"param_one": 1, "param_two": 0})

    def test_incorrect_visibility_value(self):
        class Task(luigi.Task):
            a = luigi.Parameter(default="val", visibility=5)

        task = Task()

        self.assertEqual(task._get_param_visibilities(), {"a": 0})

    def test_rerun_args_regular_params(self):
        class Task(luigi.Task):
            regular = luigi.Parameter(default="a")
            explicit_flag = luigi.BoolParameter(parsing=luigi.BoolParameter.EXPLICIT_PARSING)

        task = Task(regular="hello world", explicit_flag=True)

        self.assertEqual(task._get_rerun_args(), ["--regular 'hello world'", '--explicit-flag True'])

    def test_rerun_args_implicit_bool_default_false(self):
        class Task(luigi.Task):
            flag = luigi.BoolParameter()

        self.assertEqual(Task(flag=True)._get_rerun_args(), ["--flag"])
        self.assertEqual(Task(flag=False)._get_rerun_args(), [])

    def test_rerun_args_implicit_bool_default_true(self):
        class Task(luigi.Task):
            flag = luigi.BoolParameter(default=True)

        # the default already reproduces True, so the flag is omitted
        self.assertEqual(Task(flag=True)._get_rerun_args(), [])
        # no implicit-bool command line can express False: keep the legacy
        # (failing) output rather than silently re-running with True
        self.assertEqual(Task(flag=False)._get_rerun_args(), ['--flag False'])

    def test_rerun_args_custom_bool_subclass(self):
        class MyBool(luigi.BoolParameter):
            pass

        class Task(luigi.Task):
            flag = MyBool()

        self.assertEqual(Task(flag=True)._get_rerun_args(), ["--flag"])

    def test_rerun_args_optional_default_none(self):
        class Task(luigi.Task):
            optional = luigi.OptionalParameter(default=None)

        self.assertEqual(Task(optional=None)._get_rerun_args(), [])
        self.assertEqual(Task(optional="x")._get_rerun_args(), ['--optional x'])

    def test_rerun_args_optional_default_not_none(self):
        class Task(luigi.Task):
            optional = luigi.OptionalParameter(default="x")

        # parse("") is None, so the empty string still round-trips and must
        # not be dropped: omitting it would re-run with the default
        self.assertEqual(Task(optional=None)._get_rerun_args(), ["--optional ''"])

    def test_rerun_args_optional_bool(self):
        class Task(luigi.Task):
            optional_flag = luigi.OptionalBoolParameter()

        self.assertEqual(Task(optional_flag=True)._get_rerun_args(), ["--optional-flag"])
        # False cannot be expressed for an implicit bool defaulting to None
        self.assertEqual(Task(optional_flag=False)._get_rerun_args(), ["--optional-flag False"])

    def test_rerun_args_non_ascii_not_escaped(self):
        class Task(luigi.Task):
            name = luigi.Parameter(default="x")

        self.assertEqual(Task(name="café")._get_rerun_args(), ["--name 'café'"])

    def test_rerun_args_config_default(self):
        class ConfTask(luigi.Task):
            flag = luigi.BoolParameter(default=True)

        # without config, False cannot be expressed and keeps legacy output
        self.assertEqual(ConfTask(flag=False)._get_rerun_args(), ["--flag False"])

    def test_rerun_args_shell_special_chars(self):
        class Task(luigi.Task):
            name = luigi.Parameter(default="x")

        args = Task(name="a`b$c")._get_rerun_args()
        self.assertEqual(args, ["--name 'a`b$c'"])

    def test_rerun_args_private_params_excluded(self):
        class Task(luigi.Task):
            flag = luigi.BoolParameter(visibility=ParameterVisibility.PRIVATE)

        self.assertEqual(Task(flag=True)._get_rerun_args(), [])

    def test_task_id_exclude_hidden_and_private_params(self):
        task = TestTask1()

        self.assertEqual({"param_two": "2"}, task.to_str_params(only_public=True))

    def test_json_dumps(self):
        public = json.dumps(ParameterVisibility.PUBLIC.serialize())
        hidden = json.dumps(ParameterVisibility.HIDDEN.serialize())
        private = json.dumps(ParameterVisibility.PRIVATE.serialize())

        self.assertEqual("0", public)
        self.assertEqual("1", hidden)
        self.assertEqual("2", private)

        public = json.loads(public)
        hidden = json.loads(hidden)
        private = json.loads(private)

        self.assertEqual(0, public)
        self.assertEqual(1, hidden)
        self.assertEqual(2, private)
