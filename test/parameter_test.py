import unittest
import luigi


class Foo(luigi.Task):
    bar = luigi.Parameter()
    p2 = luigi.IntParameter()
    multi = luigi.Parameter(is_list=True)
    not_a_param = "lol"


@luigi.expose
class Bar(luigi.Task):
    multibool = luigi.BooleanParameter(is_list=True)

    def run(self):
        Bar._val = self.multibool

@luigi.expose
class Baz(luigi.Task):
    bool = luigi.BooleanParameter()

    def run(self):
        Baz._val = self.bool

@luigi.expose
class ForgotParam(luigi.Task):
    param = luigi.Parameter()
    def run(self): pass

@luigi.expose
class ForgotParamDep(luigi.Task):
    def requires(self):
        return ForgotParam()
    def run(self): pass

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

    def test_multibool(self):
        luigi.run(['--local-scheduler', 'Bar', '--multibool', 'true', '--multibool', 'false'])
        self.assertEquals(Bar._val, (True, False))

    def test_multibool_empty(self):
        luigi.run(['--local-scheduler', 'Bar'])
        self.assertEquals(Bar._val, tuple())

    def test_bool_false(self):
        luigi.run(['--local-scheduler', 'Baz'])
        self.assertEquals(Baz._val, False)

    def test_bool_true(self):
        luigi.run(['--local-scheduler', 'Baz', '--bool'])
        self.assertEquals(Baz._val, True)

    def test_forgot_param(self):
        self.assertRaises(luigi.parameter.MissingParameterException, luigi.run, ['--local-scheduler', 'ForgotParam'],)

    def test_forgot_param_in_dep(self):
        self.assertRaises(luigi.parameter.MissingParameterException, luigi.run, ['--local-scheduler', 'ForgotParamDep'],)

if __name__ == '__main__':
    luigi.run()
