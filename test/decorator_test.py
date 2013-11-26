import unittest
import luigi
import luigi.notifications
import datetime
import pickle
from luigi.parameter import MissingParameterException
luigi.notifications.DEBUG = True
from luigi.util import inherits, common_params, requires, copies, delegates
from luigi.mock import MockFile

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

@inherits(B)
class D_null(luigi.Task):
    param1 = None

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
        self.d_null = D_null()
        self.e = E()

    def test_has_param(self):
        b_params = dict(self.b.get_params()).keys()
        self.assertTrue("param1" in b_params)

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

    def test_removing_parameter(self):
        self.assertFalse("param1" in dict(self.d_null.get_params()).keys())

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
class H_null(luigi.Task):
    param2 = None
    def requires(self):
        special_param2 = str(datetime.datetime.now())
        return G(param2=special_param2, **common_params(self, G))

@inherits(G)
class I(luigi.Task):
    def requires(self):
        return F(**common_params(self, F))

class J(luigi.Task):
    param1 = luigi.Parameter() # something required, with no default

@inherits(J)
class K_shouldnotinstantiate(luigi.Task):
    param2 = luigi.Parameter("A K-specific parameter")

@inherits(J)
class K_shouldfail(luigi.Task):
    param1 = None
    param2 = luigi.Parameter("A K-specific parameter")
    def requires(self):
        return J(**common_params(self, J))

@inherits(J)
class K_shouldsucceed(luigi.Task):
    param1 = None
    param2 = luigi.Parameter("A K-specific parameter")
    def requires(self):
        return J(param1="Required parameter", **common_params(self, J))

@inherits(J)
class K_wrongparamsorder(luigi.Task):
    param1 = None
    param2 = luigi.Parameter("A K-specific parameter")
    def requires(self):
        return J(param1="Required parameter", **common_params(J, self))


class RequiresTest(unittest.TestCase):

    def setUp(self):
        self.f = F()
        self.g = G()
        self.g_changed = G(param1="changing the default")
        self.h = H()
        self.h_null = H_null()
        self.i = I()
        self.k_shouldfail = K_shouldfail()
        self.k_shouldsucceed = K_shouldsucceed()
        self.k_wrongparamsorder = K_wrongparamsorder()

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

    def test_removing_parameter(self):
        self.assertNotEqual(self.h_null.requires().param2, self.g.param2)

    def test_not_setting_required_parameter(self):
        self.assertRaises(MissingParameterException, self.k_shouldfail.requires)

    def test_setting_required_parameters(self):
        self.k_shouldsucceed.requires()

    def test_should_not_instantiate(self):
        self.assertRaises(MissingParameterException, K_shouldnotinstantiate)

    def test_resuscitation(self):
        k = K_shouldnotinstantiate(param1='hello')
        k.requires()

    def test_wrong_common_params_order(self):
        self.assertRaises(AssertionError, self.k_wrongparamsorder.requires)


class X(luigi.Task):
    n = luigi.IntParameter(default=42)

@inherits(X)
class Y(luigi.Task):
    def requires(self):
        return self.clone_parent()

@requires(X)
class Y2(luigi.Task):
    pass

@inherits(X)
class Z(luigi.Task):
    n = None
    def requires(self):
        return self.clone_parent()

@requires(X)
class Y3(luigi.Task):
    n = luigi.IntParameter(default=43)

class CloneParentTest(unittest.TestCase):
    def test_clone_parent(self):
        y = Y()
        x = X()
        self.assertEqual(y.requires(), x)
        self.assertEqual(y.n, 42)

        z = Z()
        self.assertEqual(z.requires(), x)

    def test_requires(self):
        y2 = Y2()
        x = X()
        self.assertEqual(y2.requires(), x)
        self.assertEqual(y2.n, 42)

    def test_requires_override_default(self):
        y3 = Y3()
        x = X()
        self.assertNotEqual(y3.requires(), x)
        self.assertEqual(y3.n, 43)
        self.assertEqual(y3.requires().n, 43)

    def test_names(self):
        # Just make sure the decorators retain the original class names
        x = X()
        self.assertEqual(str(x), 'X(n=42)')
        self.assertEqual(x.__class__.__name__, 'X')


class P(luigi.Task):
    date = luigi.DateParameter()

    def output(self):
        return MockFile(self.date.strftime('/tmp/data-%Y-%m-%d.txt'))

    def run(self):
        f = self.output().open('w')
        print >>f, 'hello, world'
        f.close()


@copies(P)
class PCopy(luigi.Task):
    def output(self):
        return MockFile(self.date.strftime('/tmp/copy-data-%Y-%m-%d.txt'))

class CopyTest(unittest.TestCase):
    def test_copy(self):
        luigi.build([PCopy(date=datetime.date(2012, 1, 1))], local_scheduler=True)
        self.assertEqual(MockFile._file_contents['/tmp/data-2012-01-01.txt'], 'hello, world\n')
        self.assertEqual(MockFile._file_contents['/tmp/copy-data-2012-01-01.txt'], 'hello, world\n')


class PickleTest(unittest.TestCase):
    def test_pickle(self):
        # similar to CopyTest.test_copy
        p = PCopy(date=datetime.date(2013, 1, 1))
        p_pickled = pickle.dumps(p)
        p = pickle.loads(p_pickled)

        luigi.build([p], local_scheduler=True)
        self.assertEqual(MockFile._file_contents['/tmp/data-2013-01-01.txt'], 'hello, world\n')
        self.assertEqual(MockFile._file_contents['/tmp/copy-data-2013-01-01.txt'], 'hello, world\n')


class Subtask(luigi.Task):
    k = luigi.IntParameter()

    def f(self, x):
        return x ** self.k

@delegates
class SubtaskDelegator(luigi.Task):
    def subtasks(self):
        return [Subtask(1), Subtask(2)]

    def run(self):
        self.s = 0
        for t in self.subtasks():
            self.s += t.f(42)


class SubtaskTest(unittest.TestCase):
    def test_subtasks(self):
        sd = SubtaskDelegator()
        luigi.build([sd], local_scheduler=True)
        self.assertEqual(sd.s, 42 * (1 + 42))

    def test_forgot_subtasks(self):
        def trigger_failure():
            @delegates
            class SubtaskDelegatorBroken(luigi.Task):
                pass

        self.assertRaises(AttributeError, trigger_failure)


if __name__ == '__main__':
    unittest.main()

