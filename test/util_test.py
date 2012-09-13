import unittest
import luigi, luigi.util

class A(luigi.Task):
    x = luigi.IntParameter(default=3)

class B(luigi.util.Derived(A)):
    y = luigi.IntParameter(default=4)

class A2(luigi.Task):
    x = luigi.IntParameter(default=3)
    g = luigi.IntParameter(is_global=True, default=42)

class B2(luigi.util.Derived(A2)):
    pass

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

    def test_derived_global_param(self):
        # Had a bug with this
        b = B2()
        self.assertEquals(b.g, 42)
