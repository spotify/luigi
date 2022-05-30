import unittest

import luigi
from luigi.util import requires

from helpers import with_config


class A(luigi.Task):
    num = luigi.IntParameter()


class B(luigi.Task):
    num = luigi.IntParameter()


class ParameterCollisionDetectionTest(unittest.TestCase):
    @with_config({"worker": {"prevent_parameter_collision": "true"}})
    def test_parameter_collision_with_inherited_task(self):
        with self.assertRaises(ValueError):

            @requires(A)
            class T(luigi.Task):
                num = luigi.IntParameter()

    @with_config({"worker": {"prevent_parameter_collision": "true"}})
    def test_parameter_collision_in_inheriting_tasks(self):
        with self.assertRaises(ValueError):

            @requires(A, B)
            class T(luigi.Task):
                pass

    def test_no_parameter_collision_when_disabled_in_config(self):
        @requires(A, B)
        class T(luigi.Task):
            pass

    @with_config({"worker": {"prevent_parameter_collision": "true"}})
    def test_parameter_collision_with_inherited_task_ignored_by_allowlist(self):
        @requires(A, collisions_to_ignore=["num"])
        class T(luigi.Task):
            num = luigi.IntParameter()

    @with_config({"worker": {"prevent_parameter_collision": "true"}})
    def test_parameter_collision_in_inheriting_tasks_ignored_by_allowlist(self):
        @requires(A, B, collisions_to_ignore=["num"])
        class T(luigi.Task):
            pass
