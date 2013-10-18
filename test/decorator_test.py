import unittest
import luigi
import luigi.notifications
luigi.notifications.DEBUG = True
from luigi.util import inherits, common_params

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


class F(luigi.Task):
    param1 = luigi.Parameter("A parameter on a base task, that will be required later.")

@inherits(F)
class G(luigi.Task):
    param2 = luigi.Parameter("A separate parameter that doesn't affect 'F'")
    
    def requires(self):
        return F(**common_params(self, F))

@inherits(G)
class H(luigi.Task):
    param2 = luigi.Parameter("OVERWRITING")
    def requires(self):
        return G(**common_params(self, G))

@inherits(G)
class I(luigi.Task):
    def requires(self):
        return F(**common_params(self, F))


class RequiresTest(unittest.TestCase):

    def setUp(self):
        self.f = F()
        self.g = G()
        self.g_changed = G(param1="changing the default")
        self.h = H()
        self.i = I()

    def test_inherits(self):
        self.assertEqual(self.f.param1, self.g.param1)
        self.assertEqual(self.f.param1, self.g.requires().param1)

    def test_change_of_defaults(self):
        self.assertNotEqual(self.f.param1, self.g_changed.param1)
        self.assertNotEqual(self.g.param1, self.g_changed.param1)
        self.assertNotEqual(self.f.param1, self.g_changed.requires().param1)

    def test_overwriting_parameter(self):
        self.h.requires()
        self.assertNotEqual(self.h.param2, self.g.param2)
        self.assertEqual(self.h.param2, self.h.requires().param2)
        self.assertEqual(self.h.param2, "OVERWRITING")

    def test_skipping_one_inheritance(self):
        self.assertEqual(self.i.requires().param1, self.f.param1)


if __name__ == '__main__':
    unittest.main()

