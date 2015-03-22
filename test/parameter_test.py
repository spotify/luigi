# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import datetime
from helpers import unittest
from datetime import timedelta

import luigi
import luigi.date_interval
import luigi.interface
import luigi.notifications
from helpers import with_config
from luigi.mock import MockTarget, MockFileSystem
from luigi.parameter import ParameterException
from worker_test import email_patch

luigi.notifications.DEBUG = True


class A(luigi.Task):
    p = luigi.IntParameter()


class WithDefault(luigi.Task):
    x = luigi.Parameter(default='xyz')


class Foo(luigi.Task):
    bar = luigi.Parameter()
    p2 = luigi.IntParameter()
    multi = luigi.Parameter(is_list=True)
    not_a_param = "lol"


class Bar(luigi.Task):
    multibool = luigi.BoolParameter(is_list=True)

    def run(self):
        Bar._val = self.multibool


class Baz(luigi.Task):
    bool = luigi.BoolParameter()

    def run(self):
        Baz._val = self.bool


class ForgotParam(luigi.Task):
    param = luigi.Parameter()

    def run(self):
        pass


class ForgotParamDep(luigi.Task):

    def requires(self):
        return ForgotParam()

    def run(self):
        pass


class HasGlobalParam(luigi.Task):
    x = luigi.Parameter()
    global_param = luigi.IntParameter(is_global=True, default=123)  # global parameters need default values
    global_bool_param = luigi.BoolParameter(is_global=True, default=False)

    def run(self):
        self.complete = lambda: True

    def complete(self):
        return False


class HasGlobalParamDep(luigi.Task):
    x = luigi.Parameter()

    def requires(self):
        return HasGlobalParam(self.x)

_shared_global_param = luigi.Parameter(is_global=True, default='123')


class SharedGlobalParamA(luigi.Task):
    shared_global_param = _shared_global_param


class SharedGlobalParamB(luigi.Task):
    shared_global_param = _shared_global_param


class BananaDep(luigi.Task):
    x = luigi.Parameter()
    y = luigi.Parameter(default='def')

    def output(self):
        return MockTarget('banana-dep-%s-%s' % (self.x, self.y))

    def run(self):
        self.output().open('w').close()


class Banana(luigi.Task):
    x = luigi.Parameter()
    y = luigi.Parameter()
    style = luigi.Parameter(default=None)

    def requires(self):
        if self.style is None:
            return BananaDep()  # will fail
        elif self.style == 'x-arg':
            return BananaDep(self.x)
        elif self.style == 'y-kwarg':
            return BananaDep(y=self.y)
        elif self.style == 'x-arg-y-arg':
            return BananaDep(self.x, self.y)
        else:
            raise Exception('unknown style')

    def output(self):
        return MockTarget('banana-%s-%s' % (self.x, self.y))

    def run(self):
        self.output().open('w').close()


class MyConfig(luigi.Config):
    mc_p = luigi.IntParameter()
    mc_q = luigi.IntParameter(default=73)


class MyConfigWithoutSection(luigi.Config):
    use_cmdline_section = False
    mc_r = luigi.IntParameter()
    mc_s = luigi.IntParameter(default=99)


class NoopTask(luigi.Task):
    pass


class ParameterTest(unittest.TestCase):

    def setUp(self):
        super(ParameterTest, self).setUp()
        # Need to restore some defaults for the global params since they are overriden
        HasGlobalParam.global_param.set_global(123)
        HasGlobalParam.global_bool_param.set_global(False)

    def test_default_param(self):
        self.assertEqual(WithDefault().x, 'xyz')

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
        self.assertEqual(len(Foo.get_params()), 3)

    def test_task_creation(self):
        f = Foo("barval", p2=5, multi=('m1', 'm2'))
        self.assertEqual(len(f.get_params()), 3)
        self.assertEqual(f.bar, "barval")
        self.assertEqual(f.p2, 5)
        self.assertEqual(f.multi, ('m1', 'm2'))
        self.assertEqual(f.not_a_param, "lol")

    def test_multibool(self):
        luigi.run(['--local-scheduler', '--no-lock', 'Bar', '--multibool', 'true', '--multibool', 'false'])
        self.assertEqual(Bar._val, (True, False))

    def test_multibool_empty(self):
        luigi.run(['--local-scheduler', '--no-lock', 'Bar'])
        self.assertEqual(Bar._val, tuple())

    def test_bool_false(self):
        luigi.run(['--local-scheduler', '--no-lock', 'Baz'])
        self.assertEqual(Baz._val, False)

    def test_bool_true(self):
        luigi.run(['--local-scheduler', '--no-lock', 'Baz', '--bool'])
        self.assertEqual(Baz._val, True)

    def test_forgot_param(self):
        self.assertRaises(luigi.parameter.MissingParameterException, luigi.run, ['--local-scheduler', '--no-lock', 'ForgotParam'],)

    @email_patch
    def test_forgot_param_in_dep(self, emails):
        # A programmatic missing parameter will cause an error email to be sent
        luigi.run(['--local-scheduler', '--no-lock', 'ForgotParamDep'])
        self.assertNotEquals(emails, [])

    def test_default_param_cmdline(self):
        luigi.run(['--local-scheduler', '--no-lock', 'WithDefault'])
        self.assertEqual(WithDefault().x, 'xyz')

    def test_global_param_defaults(self):
        h = HasGlobalParam(x='xyz')
        self.assertEqual(h.global_param, 123)
        self.assertEqual(h.global_bool_param, False)

    def test_global_param_cmdline(self):
        luigi.run(['--local-scheduler', '--no-lock', 'HasGlobalParam', '--x', 'xyz', '--global-param', '124'])
        h = HasGlobalParam(x='xyz')
        self.assertEqual(h.global_param, 124)
        self.assertEqual(h.global_bool_param, False)

    def test_global_param_cmdline_flipped(self):
        luigi.run(['--local-scheduler', '--no-lock', '--global-param', '125', 'HasGlobalParam', '--x', 'xyz'])
        h = HasGlobalParam(x='xyz')
        self.assertEqual(h.global_param, 125)
        self.assertEqual(h.global_bool_param, False)

    def test_global_param_override(self):
        h1 = HasGlobalParam(x='xyz', global_param=124)
        h2 = HasGlobalParam(x='xyz')
        self.assertEquals(h1.global_param, 124)
        self.assertEquals(h2.global_param, 123)

    def test_global_param_dep_cmdline(self):
        luigi.run(['--local-scheduler', '--no-lock', 'HasGlobalParamDep', '--x', 'xyz', '--global-param', '124'])
        h = HasGlobalParam(x='xyz')
        self.assertEqual(h.global_param, 124)
        self.assertEqual(h.global_bool_param, False)

    def test_global_param_dep_cmdline_optparse(self):
        luigi.run(['--local-scheduler', '--no-lock', '--task', 'HasGlobalParamDep', '--x', 'xyz', '--global-param', '124'], use_optparse=True)
        h = HasGlobalParam(x='xyz')
        self.assertEqual(h.global_param, 124)
        self.assertEqual(h.global_bool_param, False)

    def test_global_param_dep_cmdline_bool(self):
        luigi.run(['--local-scheduler', '--no-lock', 'HasGlobalParamDep', '--x', 'xyz', '--global-bool-param'])
        h = HasGlobalParam(x='xyz')
        self.assertEqual(h.global_param, 123)
        self.assertEqual(h.global_bool_param, True)

    def test_global_param_shared(self):
        luigi.run(['--local-scheduler', '--no-lock', 'SharedGlobalParamA', '--shared-global-param', 'abc'])
        b = SharedGlobalParamB()
        self.assertEqual(b.shared_global_param, 'abc')

    def test_insignificant_parameter(self):
        class InsignificantParameterTask(luigi.Task):
            foo = luigi.Parameter(significant=False, default='foo_default')
            bar = luigi.Parameter()

        t1 = InsignificantParameterTask(foo='x', bar='y')
        self.assertEqual(t1.task_id, 'InsignificantParameterTask(bar=y)')

        t2 = InsignificantParameterTask('u', 'z')
        self.assertEqual(t2.foo, 'u')
        self.assertEqual(t2.bar, 'z')
        self.assertEqual(t2.task_id, 'InsignificantParameterTask(bar=z)')

    def test_local_significant_param(self):
        """ Obviously, if anything should be positional, so should local
        significant parameters """
        class MyTask(luigi.Task):
            # This could typically be "--label-company=disney"
            x = luigi.Parameter(significant=True)

        MyTask('arg')
        self.assertRaises(luigi.parameter.MissingParameterException,
                          lambda: MyTask())

    def test_local_insignificant_param(self):
        """ Ensure we have the same behavior as in before a78338c  """
        class MyTask(luigi.Task):
            # This could typically be "--num-threads=True"
            x = luigi.Parameter(significant=False)

        MyTask('arg')
        self.assertRaises(luigi.parameter.MissingParameterException,
                          lambda: MyTask())


class TestNewStyleGlobalParameters(unittest.TestCase):

    def setUp(self):
        super(TestNewStyleGlobalParameters, self).setUp()
        MockTarget.fs.clear()
        BananaDep.y.reset_global()

    def expect_keys(self, expected):
        self.assertEquals(set(MockTarget.fs.get_all_data().keys()), set(expected))

    def test_x_arg(self):
        luigi.run(['--local-scheduler', '--no-lock', 'Banana', '--x', 'foo', '--y', 'bar', '--style', 'x-arg'])
        self.expect_keys(['banana-foo-bar', 'banana-dep-foo-def'])

    def test_x_arg_override(self):
        luigi.run(['--local-scheduler', '--no-lock', 'Banana', '--x', 'foo', '--y', 'bar', '--style', 'x-arg', '--BananaDep-y', 'xyz'])
        self.expect_keys(['banana-foo-bar', 'banana-dep-foo-xyz'])

    def test_x_arg_override_stupid(self):
        luigi.run(['--local-scheduler', '--no-lock', 'Banana', '--x', 'foo', '--y', 'bar', '--style', 'x-arg', '--BananaDep-x', 'blabla'])
        self.expect_keys(['banana-foo-bar', 'banana-dep-foo-def'])

    def test_x_arg_y_arg(self):
        luigi.run(['--local-scheduler', '--no-lock', 'Banana', '--x', 'foo', '--y', 'bar', '--style', 'x-arg-y-arg'])
        self.expect_keys(['banana-foo-bar', 'banana-dep-foo-bar'])

    def test_x_arg_y_arg_override(self):
        luigi.run(['--local-scheduler', '--no-lock', 'Banana', '--x', 'foo', '--y', 'bar', '--style', 'x-arg-y-arg', '--BananaDep-y', 'xyz'])
        self.expect_keys(['banana-foo-bar', 'banana-dep-foo-bar'])

    def test_x_arg_y_arg_override_all(self):
        luigi.run(['--local-scheduler', '--no-lock', 'Banana', '--x', 'foo', '--y', 'bar', '--style', 'x-arg-y-arg', '--BananaDep-y', 'xyz', '--BananaDep-x', 'blabla'])
        self.expect_keys(['banana-foo-bar', 'banana-dep-foo-bar'])

    def test_y_arg_override(self):
        luigi.run(['--local-scheduler', '--no-lock', 'Banana', '--x', 'foo', '--y', 'bar', '--style', 'y-kwarg', '--BananaDep-x', 'xyz'])
        self.expect_keys(['banana-foo-bar', 'banana-dep-xyz-bar'])

    def test_y_arg_override_both(self):
        luigi.run(['--local-scheduler', '--no-lock', 'Banana', '--x', 'foo', '--y', 'bar', '--style', 'y-kwarg', '--BananaDep-x', 'xyz', '--BananaDep-y', 'blah'])
        self.expect_keys(['banana-foo-bar', 'banana-dep-xyz-bar'])

    def test_y_arg_override_banana(self):
        luigi.run(['--local-scheduler', '--no-lock', 'Banana', '--y', 'bar', '--style', 'y-kwarg', '--BananaDep-x', 'xyz', '--Banana-x', 'baz'])
        self.expect_keys(['banana-baz-bar', 'banana-dep-xyz-bar'])


class TestRemoveGlobalParameters(unittest.TestCase):

    def setUp(self):
        super(TestRemoveGlobalParameters, self).setUp()
        MyConfig.mc_p.reset_global()
        MyConfig.mc_q.reset_global()
        MyConfigWithoutSection.mc_r.reset_global()
        MyConfigWithoutSection.mc_s.reset_global()

    def run_and_check(self, args):
        run_exit_status = luigi.run(['--local-scheduler', '--no-lock'] + args)
        self.assertTrue(run_exit_status)
        return run_exit_status

    def test_use_config_class_1(self):
        self.run_and_check(['--MyConfig-mc-p', '99', '--mc-r', '55', 'NoopTask'])
        self.assertEqual(MyConfig().mc_p, 99)
        self.assertEqual(MyConfig().mc_q, 73)
        self.assertEqual(MyConfigWithoutSection().mc_r, 55)
        self.assertEqual(MyConfigWithoutSection().mc_s, 99)

    def test_use_config_class_2(self):
        self.run_and_check(['NoopTask', '--MyConfig-mc-p', '99', '--mc-r', '55'])
        self.assertEqual(MyConfig().mc_p, 99)
        self.assertEqual(MyConfig().mc_q, 73)
        self.assertEqual(MyConfigWithoutSection().mc_r, 55)
        self.assertEqual(MyConfigWithoutSection().mc_s, 99)

    def test_use_config_class_more_args(self):
        self.run_and_check(['--MyConfig-mc-p', '99', '--mc-r', '55', 'NoopTask', '--mc-s', '123', '--MyConfig-mc-q', '42'])
        self.assertEqual(MyConfig().mc_p, 99)
        self.assertEqual(MyConfig().mc_q, 42)
        self.assertEqual(MyConfigWithoutSection().mc_r, 55)
        self.assertEqual(MyConfigWithoutSection().mc_s, 123)

    @with_config({"MyConfig": {"mc_p": "666", "mc_q": "777"}})
    def test_use_config_class_with_configuration(self):
        self.run_and_check(['--mc-r', '555', 'NoopTask'])
        self.assertEqual(MyConfig().mc_p, 666)
        self.assertEqual(MyConfig().mc_q, 777)
        self.assertEqual(MyConfigWithoutSection().mc_r, 555)
        self.assertEqual(MyConfigWithoutSection().mc_s, 99)

    @with_config({"MyConfigWithoutSection": {"mc_r": "999", "mc_s": "888"}})
    def test_use_config_class_with_configuration_2(self):
        self.run_and_check(['NoopTask', '--MyConfig-mc-p', '222', '--mc-r', '555'])
        self.assertEqual(MyConfig().mc_p, 222)
        self.assertEqual(MyConfig().mc_q, 73)
        self.assertEqual(MyConfigWithoutSection().mc_r, 555)
        self.assertEqual(MyConfigWithoutSection().mc_s, 888)

    def test_misc_1(self):
        class Dogs(luigi.Config):
            n_dogs = luigi.IntParameter()

        class CatsWithoutSection(luigi.Config):
            use_cmdline_section = False
            n_cats = luigi.IntParameter()

        self.run_and_check(['--n-cats', '123', '--Dogs-n-dogs', '456', 'WithDefault'])
        self.assertEqual(Dogs().n_dogs, 456)
        self.assertEqual(CatsWithoutSection().n_cats, 123)

        self.run_and_check(['WithDefault', '--n-cats', '321', '--Dogs-n-dogs', '654'])
        self.assertEqual(Dogs().n_dogs, 654)
        self.assertEqual(CatsWithoutSection().n_cats, 321)

    def test_global_significant_param(self):
        """ We don't want any kind of global param to be positional """
        class MyTask(luigi.Task):
            # This could typically be called "--test-dry-run"
            x_g1 = luigi.Parameter(default='y', is_global=True, significant=True)

        self.assertRaises(luigi.parameter.UnknownParameterException,
                          lambda: MyTask('arg'))

    def test_global_insignificant_param(self):
        """ We don't want any kind of global param to be positional """
        class MyTask(luigi.Task):
            # This could typically be "--yarn-pool=development"
            x_g2 = luigi.Parameter(default='y', is_global=True, significant=False)

        self.assertRaises(luigi.parameter.UnknownParameterException,
                          lambda: MyTask('arg'))

    def test_mixed_params(self):
        """ Essentially for what broke in a78338c and was reported in #738 """
        class MyTask(luigi.Task):
            # This could typically be "--num-threads=True"
            x_g3 = luigi.Parameter(default='y', is_global=True)
            local_param = luigi.Parameter()

        MyTask('setting_local_param')

    def test_mixed_params_inheritence(self):
        """ A slightly more real-world like test case """
        class TaskWithOneGlobalParam(luigi.Task):
            non_positional_param = luigi.Parameter(default='y', is_global=True)

        class TaskWithOnePositionalParam(TaskWithOneGlobalParam):
            """ Try to mess with positional parameters by subclassing """
            only_positional_param = luigi.Parameter()

            def complete(self):
                return True

        class PositionalParamsRequirer(luigi.Task):

            def requires(self):
                return TaskWithOnePositionalParam('only_positional_value')

            def run(self):
                pass

        self.run_and_check(['PositionalParamsRequirer'])
        self.run_and_check(['PositionalParamsRequirer', '--non-positional-param', 'z'])


class TestParamWithDefaultFromConfig(unittest.TestCase):

    def testNoSection(self):
        self.assertRaises(ParameterException, lambda: luigi.Parameter(config_path=dict(section="foo", name="bar")).value)

    @with_config({"foo": {}})
    def testNoValue(self):
        self.assertRaises(ParameterException, lambda: luigi.Parameter(config_path=dict(section="foo", name="bar")).value)

    @with_config({"foo": {"bar": "baz"}})
    def testDefault(self):
        class A(luigi.Task):
            p = luigi.Parameter(config_path=dict(section="foo", name="bar"))

        self.assertEqual("baz", A().p)
        self.assertEqual("boo", A(p="boo").p)

    @with_config({"foo": {"bar": "2001-02-03T04"}})
    def testDateHour(self):
        p = luigi.DateHourParameter(config_path=dict(section="foo", name="bar"))
        self.assertEqual(datetime.datetime(2001, 2, 3, 4, 0, 0), p.value)

    @with_config({"foo": {"bar": "2001-02-03"}})
    def testDate(self):
        p = luigi.DateParameter(config_path=dict(section="foo", name="bar"))
        self.assertEqual(datetime.date(2001, 2, 3), p.value)

    @with_config({"foo": {"bar": "123"}})
    def testInt(self):
        p = luigi.IntParameter(config_path=dict(section="foo", name="bar"))
        self.assertEqual(123, p.value)

    @with_config({"foo": {"bar": "true"}})
    def testBool(self):
        p = luigi.BoolParameter(config_path=dict(section="foo", name="bar"))
        self.assertEqual(True, p.value)

    @with_config({"foo": {"bar": "2001-02-03-2001-02-28"}})
    def testDateInterval(self):
        p = luigi.DateIntervalParameter(config_path=dict(section="foo", name="bar"))
        expected = luigi.date_interval.Custom.parse("2001-02-03-2001-02-28")
        self.assertEqual(expected, p.value)

    @with_config({"foo": {"bar": "1 day"}})
    def testTimeDelta(self):
        p = luigi.TimeDeltaParameter(config_path=dict(section="foo", name="bar"))
        self.assertEqual(timedelta(days=1), p.value)

    @with_config({"foo": {"bar": "2 seconds"}})
    def testTimeDeltaPlural(self):
        p = luigi.TimeDeltaParameter(config_path=dict(section="foo", name="bar"))
        self.assertEqual(timedelta(seconds=2), p.value)

    @with_config({"foo": {"bar": "3w 4h 5m"}})
    def testTimeDeltaMultiple(self):
        p = luigi.TimeDeltaParameter(config_path=dict(section="foo", name="bar"))
        self.assertEqual(timedelta(weeks=3, hours=4, minutes=5), p.value)

    @with_config({"foo": {"bar": "P4DT12H30M5S"}})
    def testTimeDelta8601(self):
        p = luigi.TimeDeltaParameter(config_path=dict(section="foo", name="bar"))
        self.assertEqual(timedelta(days=4, hours=12, minutes=30, seconds=5), p.value)

    @with_config({"foo": {"bar": "P5D"}})
    def testTimeDelta8601NoTimeComponent(self):
        p = luigi.TimeDeltaParameter(config_path=dict(section="foo", name="bar"))
        self.assertEqual(timedelta(days=5), p.value)

    @with_config({"foo": {"bar": "P5W"}})
    def testTimeDelta8601Weeks(self):
        p = luigi.TimeDeltaParameter(config_path=dict(section="foo", name="bar"))
        self.assertEqual(timedelta(weeks=5), p.value)

    @with_config({"foo": {"bar": "P3Y6M4DT12H30M5S"}})
    def testTimeDelta8601YearMonthNotSupported(self):
        def f():
            return luigi.TimeDeltaParameter(config_path=dict(section="foo", name="bar")).value
        self.assertRaises(luigi.parameter.ParameterException, f)  # ISO 8601 durations with years or months are not supported

    @with_config({"foo": {"bar": "PT6M"}})
    def testTimeDelta8601MAfterT(self):
        p = luigi.TimeDeltaParameter(config_path=dict(section="foo", name="bar"))
        self.assertEqual(timedelta(minutes=6), p.value)

    @with_config({"foo": {"bar": "P6M"}})
    def testTimeDelta8601MBeforeT(self):
        def f():
            return luigi.TimeDeltaParameter(config_path=dict(section="foo", name="bar")).value
        self.assertRaises(luigi.parameter.ParameterException, f)  # ISO 8601 durations with months are not supported

    def testHasDefaultNoSection(self):
        luigi.Parameter(config_path=dict(section="foo", name="bar")).has_value
        self.assertFalse(luigi.Parameter(config_path=dict(section="foo", name="bar")).has_value)

    @with_config({"foo": {}})
    def testHasDefaultNoValue(self):
        self.assertFalse(luigi.Parameter(config_path=dict(section="foo", name="bar")).has_value)

    @with_config({"foo": {"bar": "baz"}})
    def testHasDefaultWithBoth(self):
        self.assertTrue(luigi.Parameter(config_path=dict(section="foo", name="bar")).has_value)

    @with_config({"foo": {"bar": "one\n\ttwo\n\tthree\n"}})
    def testDefaultList(self):
        p = luigi.Parameter(is_list=True, config_path=dict(section="foo", name="bar"))
        self.assertEqual(('one', 'two', 'three'), p.value)

    @with_config({"foo": {"bar": "1\n2\n3"}})
    def testDefaultIntList(self):
        p = luigi.IntParameter(is_list=True, config_path=dict(section="foo", name="bar"))
        self.assertEqual((1, 2, 3), p.value)

    @with_config({"foo": {"bar": "baz"}})
    def testWithDefault(self):
        p = luigi.Parameter(config_path=dict(section="foo", name="bar"), default='blah')
        self.assertEqual('baz', p.value)  # config overrides default

    def testWithDefaultAndMissing(self):
        p = luigi.Parameter(config_path=dict(section="foo", name="bar"), default='blah')
        self.assertEqual('blah', p.value)

    @with_config({"foo": {"bar": "baz"}})
    def testGlobal(self):
        p = luigi.Parameter(config_path=dict(section="foo", name="bar"), is_global=True, default='blah')
        self.assertEqual('baz', p.value)
        p.set_global('meh')
        self.assertEqual('meh', p.value)

    def testGlobalAndMissing(self):
        p = luigi.Parameter(config_path=dict(section="foo", name="bar"), is_global=True, default='blah')
        self.assertEqual('blah', p.value)
        p.set_global('meh')
        self.assertEqual('meh', p.value)

    @with_config({"A": {"p": "p_default"}})
    def testDefaultFromTaskName(self):
        class A(luigi.Task):
            p = luigi.Parameter()

        self.assertEqual("p_default", A().p)
        self.assertEqual("boo", A(p="boo").p)

    @with_config({"A": {"p": "999"}})
    def testDefaultFromTaskNameInt(self):
        class A(luigi.Task):
            p = luigi.IntParameter()

        self.assertEqual(999, A().p)
        self.assertEqual(777, A(p=777).p)

    @with_config({"A": {"p": "p_default"}, "foo": {"bar": "baz"}})
    def testDefaultFromConfigWithTaskNameToo(self):
        class A(luigi.Task):
            p = luigi.Parameter(config_path=dict(section="foo", name="bar"))

        self.assertEqual("p_default", A().p)
        self.assertEqual("boo", A(p="boo").p)

    @with_config({"A": {"p": "p_default_2"}})
    def testDefaultFromTaskNameWithDefault(self):
        class A(luigi.Task):
            p = luigi.Parameter(default="banana")

        self.assertEqual("p_default_2", A().p)
        self.assertEqual("boo_2", A(p="boo_2").p)


class OverrideEnvStuff(unittest.TestCase):

    def setUp(self):
        env_params_cls = luigi.interface.core
        env_params_cls.scheduler_port.reset_global()

    @with_config({"core": {"default-scheduler-port": '6543'}})
    def testOverrideSchedulerPort(self):
        env_params = luigi.interface.core()
        self.assertEqual(env_params.scheduler_port, 6543)

    @with_config({"core": {"scheduler-port": '6544'}})
    def testOverrideSchedulerPort2(self):
        env_params = luigi.interface.core()
        self.assertEqual(env_params.scheduler_port, 6544)

    @with_config({"core": {"scheduler_port": '6545'}})
    def testOverrideSchedulerPort3(self):
        env_params = luigi.interface.core()
        self.assertEqual(env_params.scheduler_port, 6545)


if __name__ == '__main__':
    luigi.run(use_optparse=True)
