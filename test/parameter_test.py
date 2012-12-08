# Copyright (c) 2012 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import unittest
import luigi, luigi.interface


class A(luigi.Task):
    p = luigi.IntParameter()


# @luigi.expose
class WithDefault(luigi.Task):
    x = luigi.Parameter(default='xyz')


class Foo(luigi.Task):
    bar = luigi.Parameter()
    p2 = luigi.IntParameter()
    multi = luigi.Parameter(is_list=True)
    not_a_param = "lol"


# @luigi.expose
class Bar(luigi.Task):
    multibool = luigi.BooleanParameter(is_list=True)

    def run(self):
        Bar._val = self.multibool


# @luigi.expose
class Baz(luigi.Task):
    bool = luigi.BooleanParameter()

    def run(self):
        Baz._val = self.bool


# @luigi.expose
class ForgotParam(luigi.Task):
    param = luigi.Parameter()

    def run(self):
        pass


# @luigi.expose
class ForgotParamDep(luigi.Task):
    def requires(self):
        return ForgotParam()

    def run(self):
        pass


# @luigi.expose
class HasGlobalParam(luigi.Task):
    x = luigi.Parameter()
    global_param = luigi.IntParameter(is_global=True, default=123)  # global parameters need default values
    global_bool_param = luigi.BooleanParameter(is_global=True, default=False)

    def run(self):
        self.complete = lambda: True

    def complete(self):
        return False


# @luigi.expose
class HasGlobalParamDep(luigi.Task):
    x = luigi.Parameter()

    def requires(self):
        return HasGlobalParam(self.x)

_shared_global_param = luigi.Parameter(is_global=True, default='123')


# @luigi.expose
class SharedGlobalParamA(luigi.Task):
    shared_global_param = _shared_global_param


# @luigi.expose
class SharedGlobalParamB(luigi.Task):
    shared_global_param = _shared_global_param


class ParameterTest(unittest.TestCase):
    def setUp(self):
        # Reset global register
        luigi.interface.reset()
        for cls in [WithDefault, Bar, Baz, ForgotParam, ForgotParamDep, HasGlobalParam, HasGlobalParamDep, SharedGlobalParamA, SharedGlobalParamB]:
            luigi.expose(cls)
        # Need to restore some defaults for the global params since they are overriden
        HasGlobalParam.global_param.set_default(123)
        HasGlobalParam.global_bool_param.set_default(False)

    def test_default_param(self):
        self.assertEquals(WithDefault().x, 'xyz')

    def test_missing_param(self):
        def create_a():
            return A()
        self.assertRaises(luigi.parameter.MissingParameterException, create_a)

    def test_unknown_param(self):
        def create_a():
            return A(p=5, q=4)
        self.assertRaises(luigi.parameter.UnknownParameterException, create_a)

    def test_unknown_param_2(self):
        def create_a():
            return A(1, 2, 3)
        self.assertRaises(luigi.parameter.UnknownParameterException, create_a)

    def test_duplicated_param(self):
        def create_a():
            return A(5, p=7)
        self.assertRaises(luigi.parameter.DuplicateParameterException, create_a)

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
        # A programmatic missing parameter will cause a system exit
        self.assertRaises(SystemExit, luigi.run, ['--local-scheduler', 'ForgotParamDep'],)

    def test_default_param_cmdline(self):
        luigi.run(['--local-scheduler', 'WithDefault'])
        self.assertEquals(WithDefault().x, 'xyz')

    def test_global_param_defaults(self):
        h = HasGlobalParam(x='xyz')
        self.assertEquals(h.global_param, 123)
        self.assertEquals(h.global_bool_param, False)

    def test_global_param_cmdline(self):
        luigi.run(['--local-scheduler', 'HasGlobalParam', '--x', 'xyz', '--global-param', '124'])
        h = HasGlobalParam(x='xyz')
        self.assertEquals(h.global_param, 124)
        self.assertEquals(h.global_bool_param, False)

    def test_global_param_override(self):
        def f():
            return HasGlobalParam(x='xyz', global_param=124)
        self.assertRaises(luigi.parameter.ParameterException, f)  # can't override a global parameter

    def test_global_param_dep_cmdline(self):
        luigi.run(['--local-scheduler', 'HasGlobalParamDep', '--x', 'xyz', '--global-param', '124'])
        h = HasGlobalParam(x='xyz')
        self.assertEquals(h.global_param, 124)
        self.assertEquals(h.global_bool_param, False)

    def test_global_param_dep_cmdline_optparse(self):
        luigi.run(['--local-scheduler', '--task', 'HasGlobalParamDep', '--x', 'xyz', '--global-param', '124'], use_optparse=True)
        h = HasGlobalParam(x='xyz')
        self.assertEquals(h.global_param, 124)
        self.assertEquals(h.global_bool_param, False)

    def test_global_param_dep_cmdline_bool(self):
        luigi.run(['--local-scheduler', 'HasGlobalParamDep', '--x', 'xyz', '--global-bool-param'])
        h = HasGlobalParam(x='xyz')
        self.assertEquals(h.global_param, 123)
        self.assertEquals(h.global_bool_param, True)

    def test_global_param_shared(self):
        luigi.run(['--local-scheduler', 'SharedGlobalParamA', '--shared-global-param', 'abc'])
        b = SharedGlobalParamB()
        self.assertEquals(b.shared_global_param, 'abc')

    def test_insignificant_parameter(self):
        class InsignificantParameterTask(luigi.Task):
            foo = luigi.Parameter(significant=False)
            bar = luigi.Parameter()

        t = InsignificantParameterTask(foo='x', bar='y')
        self.assertEquals(t.task_id, 'InsignificantParameterTask(bar=y)')

if __name__ == '__main__':
    luigi.run(use_optparse=True)
