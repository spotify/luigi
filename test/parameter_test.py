import unittest
import luigi


class Foo(luigi.Task):
    bar = luigi.Parameter()
    p2 = luigi.IntParameter()
    multi = luigi.Parameter(is_list=True)
    not_a_param = "lol"


class ParameterTest(unittest.TestCase):

    def test_parameter_registration(self):
        self.assertEquals(len(Foo.get_params()), 3)

    def test_task_creation(self):
        f = Foo("barval", p2=5, multi=('m1', 'm2'))
        self.assertEquals(len(f.get_params()), 3)
        self.assertEquals(f.bar, "barval")
        self.assertEquals(f.p2, 5)
        self.assertEquals(f.multi, ('m1', 'm2'))
        self.assertEquals(f.not_a_param, "lol")
