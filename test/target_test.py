import unittest

import luigi.target


class TargetTest(unittest.TestCase):
    def test_cannot_instantiate(self):
        def instantiate_target():
            luigi.target.Target()

        self.assertRaises(TypeError, instantiate_target)

    def test_abstract_subclass(self):
        class ExistsLessTarget(luigi.target.Target):
            pass

        def instantiate_target():
            ExistsLessTarget()

        self.assertRaises(TypeError, instantiate_target)

    def test_instantiate_subclass(self):
        class GoodTarget(luigi.target.Target):
            def exists(self):
                return True

            def open(self, mode):
                return None

        GoodTarget()
