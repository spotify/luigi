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

import datetime
from datetime import timedelta
import luigi.date_interval
import luigi
import luigi.interface
from worker_test import EmailTest
import luigi.notifications
from luigi.parameter import UnknownConfigException
luigi.notifications.DEBUG = True
import unittest
from helpers import with_config


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
    multibool = luigi.BooleanParameter(is_list=True)

    def run(self):
        Bar._val = self.multibool


class Baz(luigi.Task):
    bool = luigi.BooleanParameter()

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
    global_bool_param = luigi.BooleanParameter(is_global=True, default=False)

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


class ParameterTest(EmailTest):
    def setUp(self):
        super(ParameterTest, self).setUp()
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
        # A programmatic missing parameter will cause an error email to be sent
        luigi.run(['--local-scheduler', 'ForgotParamDep'])
        self.assertNotEquals(self.last_email, None)

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


class TestParamWithDefaultFromConfig(unittest.TestCase):

    def testNoSection(self):
        self.assertRaises(UnknownConfigException, lambda: luigi.Parameter(default_from_config=dict(section="foo", name="bar")).default)

    @with_config({"foo": {}})
    def testNoValue(self):
        self.assertRaises(UnknownConfigException, lambda: luigi.Parameter(default_from_config=dict(section="foo", name="bar")).default)

    @with_config({"foo": {"bar": "baz"}})
    def testDefault(self):
        class A(luigi.Task):
            p = luigi.Parameter(default_from_config=dict(section="foo", name="bar"))

        self.assertEquals("baz", A().p)
        self.assertEquals("boo", A(p="boo").p)

    @with_config({"foo": {"bar": "2001-02-03T04"}})
    def testDateHour(self):
        p = luigi.DateHourParameter(default_from_config=dict(section="foo", name="bar"))
        self.assertEquals(datetime.datetime(2001, 2, 3, 4, 0, 0), p.default)

    @with_config({"foo": {"bar": "2001-02-03"}})
    def testDate(self):
        p = luigi.DateParameter(default_from_config=dict(section="foo", name="bar"))
        self.assertEquals(datetime.date(2001, 2, 3), p.default)

    @with_config({"foo": {"bar": "123"}})
    def testInt(self):
        p = luigi.IntParameter(default_from_config=dict(section="foo", name="bar"))
        self.assertEquals(123, p.default)

    @with_config({"foo": {"bar": "true"}})
    def testBool(self):
        p = luigi.BooleanParameter(default_from_config=dict(section="foo", name="bar"))
        self.assertEquals(True, p.default)

    @with_config({"foo": {"bar": "2001-02-03-2001-02-28"}})
    def testDateInterval(self):
        p = luigi.DateIntervalParameter(default_from_config=dict(section="foo", name="bar"))
        expected = luigi.date_interval.Custom.parse("2001-02-03-2001-02-28")
        self.assertEquals(expected, p.default)

    @with_config({"foo": {"bar": "1 day"}})
    def testTimeDelta(self):
        p = luigi.TimeDeltaParameter(default_from_config=dict(section="foo", name="bar"))
        self.assertEquals(timedelta(days = 1), p.default)

    @with_config({"foo": {"bar": "2 seconds"}})
    def testTimeDeltaPlural(self):
        p = luigi.TimeDeltaParameter(default_from_config=dict(section="foo", name="bar"))
        self.assertEquals(timedelta(seconds = 2), p.default)

    @with_config({"foo": {"bar": "3w 4h 5m"}})
    def testTimeDeltaMultiple(self):
        p = luigi.TimeDeltaParameter(default_from_config=dict(section="foo", name="bar"))
        self.assertEquals(timedelta(weeks = 3, hours = 4, minutes = 5), p.default)

    @with_config({"foo": {"bar": "P4DT12H30M5S"}})
    def testTimeDelta8601(self):
        p = luigi.TimeDeltaParameter(default_from_config=dict(section="foo", name="bar"))
        self.assertEquals(timedelta(days = 4, hours = 12, minutes = 30, seconds = 5), p.default)

    @with_config({"foo": {"bar": "P5D"}})
    def testTimeDelta8601NoTimeComponent(self):
        p = luigi.TimeDeltaParameter(default_from_config=dict(section="foo", name="bar"))
        self.assertEquals(timedelta(days = 5), p.default)

    @with_config({"foo": {"bar": "P5W"}})
    def testTimeDelta8601Weeks(self):
        p = luigi.TimeDeltaParameter(default_from_config=dict(section="foo", name="bar"))
        self.assertEquals(timedelta(weeks = 5), p.default)

    @with_config({"foo": {"bar": "P3Y6M4DT12H30M5S"}})
    def testTimeDelta8601YearMonthNotSupported(self):
        def f():
            return luigi.TimeDeltaParameter(default_from_config=dict(section="foo", name="bar")).default
        self.assertRaises(luigi.parameter.ParameterException, f)  # ISO 8601 durations with years or months are not supported

    @with_config({"foo": {"bar": "PT6M"}})
    def testTimeDelta8601MAfterT(self):
        p = luigi.TimeDeltaParameter(default_from_config=dict(section="foo", name="bar"))
        self.assertEquals(timedelta(minutes = 6), p.default)

    @with_config({"foo": {"bar": "P6M"}})
    def testTimeDelta8601MBeforeT(self):
        def f():
            return luigi.TimeDeltaParameter(default_from_config=dict(section="foo", name="bar")).default
        self.assertRaises(luigi.parameter.ParameterException, f)  # ISO 8601 durations with months are not supported

    def testTwoDefaults(self):
        self.assertRaises(luigi.parameter.ParameterException, lambda: luigi.Parameter(default="baz", default_from_config=dict(section="foo", name="bar")))

    def testHasDefaultNoSection(self):
        luigi.Parameter(default_from_config=dict(section="foo", name="bar")).has_default
        self.assertFalse(luigi.Parameter(default_from_config=dict(section="foo", name="bar")).has_default)

    @with_config({"foo": {}})
    def testHasDefaultNoValue(self):
        self.assertFalse(luigi.Parameter(default_from_config=dict(section="foo", name="bar")).has_default)

    @with_config({"foo": {"bar": "baz"}})
    def testHasDefaultWithBoth(self):
        self.assertTrue(luigi.Parameter(default_from_config=dict(section="foo", name="bar")).has_default)

if __name__ == '__main__':
    luigi.run(use_optparse=True)
