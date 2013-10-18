import unittest
import luigi
import luigi.notifications
luigi.notifications.DEBUG = True
from luigi.util import inherits

class A(luigi.Task):
    param1 = luigi.Parameter("class A-specific default")

@inherits(A)
class B(luigi.Task):
    param2 = luigi.Parameter("class B-specific default")

@inherits(B)
class C(luigi.Task):
    param3 = luigi.Parameter("class C-specific default")

@inherits(B)
class D(luigi.Task):
    param1 = luigi.Parameter("class D overwriting class A's default")

@inherits(A)
@inherits(B)
class E(luigi.Task):
    param4 = luigi.Parameter("class E-specific default")


class InheritTest(unittest.TestCase):
    def setUp(self):
        self.a = A()
        self.a_changed = A(param1=34)
        self.b = B()
        self.c = C()
        self.d = D()
        self.e = E()

    def test_has_param(self):
        b_params = dict(self.b.get_params()).keys()
        self.assertIn("param1", b_params)

    def test_default_param(self):
        self.assertEqual(self.b.param1, self.a.param1)

    def test_change_of_defaults_not_equal(self):
        self.assertNotEqual(self.b.param1, self.a_changed.param1)

    def tested_chained_inheritance(self):
        self.assertEqual(self.c.param2, self.b.param2)
        self.assertEqual(self.c.param1, self.a.param1)
        self.assertEqual(self.c.param1, self.b.param1)

    def test_overwriting_defaults(self):
        self.assertEqual(self.d.param2, self.b.param2)
        self.assertNotEqual(self.d.param1, self.b.param1)
        self.assertNotEqual(self.d.param1, self.a.param1)
        self.assertEqual(self.d.param1, "class D overwriting class A's default")

    def test_stacked_inheritance(self):
        self.assertEqual(self.e.param1, self.a.param1)
        self.assertEqual(self.e.param1, self.b.param1)
        self.assertEqual(self.e.param2, self.b.param2)





if __name__ == '__main__':
    unittest.main()

