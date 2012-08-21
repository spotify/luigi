import unittest
import luigi, luigi.util

class A(luigi.Task):
    x = luigi.IntParameter(default=3)

class B(luigi.util.Derived(A)):
    y = luigi.IntParameter(default=4)

class UtilTest(unittest.TestCase):
    def test_derived_extended(self):
        b = B(1, 2)
        self.assertEquals(b.x, 1)
        self.assertEquals(b.y, 2)
        a = A(1)
        self.assertEquals(b.parent_obj, a)

    def test_derived_extended_default(self):
        b = B()
        self.assertEquals(b.x, 3)
        self.assertEquals(b.y, 4)
