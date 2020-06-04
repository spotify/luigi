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

from helpers import with_config, LuigiTestCase, parsing, in_parse, RunOnceTask
from datetime import timedelta
import enum
import mock

import luigi
import luigi.date_interval
import luigi.interface
import luigi.notifications
from luigi.mock import MockTarget
from luigi.parameter import ParameterException
from worker_test import email_patch

luigi.notifications.DEBUG = True


class A(luigi.Task):
    p = luigi.IntParameter()


class WithDefault(luigi.Task):
    x = luigi.Parameter(default='xyz')


class WithDefaultTrue(luigi.Task):
    x = luigi.BoolParameter(default=True)


class WithDefaultFalse(luigi.Task):
    x = luigi.BoolParameter(default=False)


class Foo(luigi.Task):
    bar = luigi.Parameter()
    p2 = luigi.IntParameter()
    not_a_param = "lol"


class Baz(luigi.Task):
    bool = luigi.BoolParameter()
    bool_true = luigi.BoolParameter(default=True)
    bool_explicit = luigi.BoolParameter(parsing=luigi.BoolParameter.EXPLICIT_PARSING)

    def run(self):
        Baz._val = self.bool
        Baz._val_true = self.bool_true
        Baz._val_explicit = self.bool_explicit


class ListFoo(luigi.Task):
    my_list = luigi.ListParameter()

    def run(self):
        ListFoo._val = self.my_list


class TupleFoo(luigi.Task):
    my_tuple = luigi.TupleParameter()

    def run(self):
        TupleFoo._val = self.my_tuple


class ForgotParam(luigi.Task):
    param = luigi.Parameter()

    def run(self):
        pass


class ForgotParamDep(luigi.Task):

    def requires(self):
        return ForgotParam()

    def run(self):
        pass


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


class MyEnum(enum.Enum):
    A = 1
    C = 3


def _value(parameter):
    """
    A hackish way to get the "value" of a parameter.

    Previously Parameter exposed ``param_obj._value``. This is replacement for
    that so I don't need to rewrite all test cases.
    """
    class DummyLuigiTask(luigi.Task):
        param = parameter

    return DummyLuigiTask().param


class ParameterTest(LuigiTestCase):

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
        self.assertEqual(len(Foo.get_params()), 2)

    def test_task_creation(self):
        f = Foo("barval", p2=5)
        self.assertEqual(len(f.get_params()), 2)
        self.assertEqual(f.bar, "barval")
        self.assertEqual(f.p2, 5)
        self.assertEqual(f.not_a_param, "lol")

    def test_bool_parsing(self):
        self.run_locally(['Baz'])
        self.assertFalse(Baz._val)
        self.assertTrue(Baz._val_true)
        self.assertFalse(Baz._val_explicit)

        self.run_locally(['Baz', '--bool', '--bool-true'])
        self.assertTrue(Baz._val)
        self.assertTrue(Baz._val_true)

        self.run_locally(['Baz', '--bool-explicit', 'true'])
        self.assertTrue(Baz._val_explicit)

        self.run_locally(['Baz', '--bool-explicit', 'false'])
        self.assertFalse(Baz._val_explicit)

    def test_bool_default(self):
        self.assertTrue(WithDefaultTrue().x)
        self.assertFalse(WithDefaultFalse().x)

    def test_bool_coerce(self):
        self.assertTrue(WithDefaultTrue(x='true').x)
        self.assertFalse(WithDefaultTrue(x='false').x)

    def test_bool_no_coerce_none(self):
        self.assertIsNone(WithDefaultTrue(x=None).x)

    def test_forgot_param(self):
        self.assertRaises(luigi.parameter.MissingParameterException, self.run_locally, ['ForgotParam'],)

    @email_patch
    def test_forgot_param_in_dep(self, emails):
        # A programmatic missing parameter will cause an error email to be sent
        self.run_locally(['ForgotParamDep'])
        self.assertNotEqual(emails, [])

    def test_default_param_cmdline(self):
        self.assertEqual(WithDefault().x, 'xyz')

    def test_default_param_cmdline_2(self):
        self.assertEqual(WithDefault().x, 'xyz')

    def test_insignificant_parameter(self):
        class InsignificantParameterTask(luigi.Task):
            foo = luigi.Parameter(significant=False, default='foo_default')
            bar = luigi.Parameter()

        t1 = InsignificantParameterTask(foo='x', bar='y')
        self.assertEqual(str(t1), 'InsignificantParameterTask(bar=y)')

        t2 = InsignificantParameterTask('u', 'z')
        self.assertEqual(t2.foo, 'u')
        self.assertEqual(t2.bar, 'z')
        self.assertEqual(str(t2), 'InsignificantParameterTask(bar=z)')

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

    def test_nonpositional_param(self):
        """ Ensure we have the same behavior as in before a78338c  """
        class MyTask(luigi.Task):
            # This could typically be "--num-threads=10"
            x = luigi.Parameter(significant=False, positional=False)

        MyTask(x='arg')
        self.assertRaises(luigi.parameter.UnknownParameterException,
                          lambda: MyTask('arg'))

    def test_enum_param_valid(self):
        p = luigi.parameter.EnumParameter(enum=MyEnum)
        self.assertEqual(MyEnum.A, p.parse('A'))

    def test_enum_param_invalid(self):
        p = luigi.parameter.EnumParameter(enum=MyEnum)
        self.assertRaises(ValueError, lambda: p.parse('B'))

    def test_enum_param_missing(self):
        self.assertRaises(ParameterException, lambda: luigi.parameter.EnumParameter())

    def test_enum_list_param_valid(self):
        p = luigi.parameter.EnumListParameter(enum=MyEnum)
        self.assertEqual((), p.parse(''))
        self.assertEqual((MyEnum.A,), p.parse('A'))
        self.assertEqual((MyEnum.A, MyEnum.C), p.parse('A,C'))

    def test_enum_list_param_invalid(self):
        p = luigi.parameter.EnumListParameter(enum=MyEnum)
        self.assertRaises(ValueError, lambda: p.parse('A,B'))

    def test_enum_list_param_missing(self):
        self.assertRaises(ParameterException, lambda: luigi.parameter.EnumListParameter())

    def test_list_serialize_parse(self):
        a = luigi.ListParameter()
        b_list = [1, 2, 3]
        self.assertEqual(b_list, a.parse(a.serialize(b_list)))

    def test_tuple_serialize_parse(self):
        a = luigi.TupleParameter()
        b_tuple = ((1, 2), (3, 4))
        self.assertEqual(b_tuple, a.parse(a.serialize(b_tuple)))

    def test_parse_list_without_batch_method(self):
        param = luigi.Parameter()
        for xs in [], ['x'], ['x', 'y']:
            self.assertRaises(NotImplementedError, param._parse_list, xs)

    def test_parse_empty_list_raises_value_error(self):
        for batch_method in (max, min, tuple, ','.join):
            param = luigi.Parameter(batch_method=batch_method)
            self.assertRaises(ValueError, param._parse_list, [])

    def test_parse_int_list_max(self):
        param = luigi.IntParameter(batch_method=max)
        self.assertEqual(17, param._parse_list(['7', '17', '5']))

    def test_parse_string_list_max(self):
        param = luigi.Parameter(batch_method=max)
        self.assertEqual('7', param._parse_list(['7', '17', '5']))

    def test_parse_list_as_tuple(self):
        param = luigi.IntParameter(batch_method=tuple)
        self.assertEqual((7, 17, 5), param._parse_list(['7', '17', '5']))

    @mock.patch('luigi.parameter.warnings')
    def test_warn_on_default_none(self, warnings):
        class TestConfig(luigi.Config):
            param = luigi.Parameter(default=None)

        TestConfig()
        warnings.warn.assert_called_once_with('Parameter "param" with value "None" is not of type string.')

    @mock.patch('luigi.parameter.warnings')
    def test_no_warn_on_string(self, warnings):
        class TestConfig(luigi.Config):
            param = luigi.Parameter(default=None)

        TestConfig(param="str")
        warnings.warn.assert_not_called()

    @mock.patch('luigi.parameter.warnings')
    def test_no_warn_on_none_in_optional(self, warnings):
        class TestConfig(luigi.Config):
            param = luigi.OptionalParameter(default=None)

        TestConfig()
        warnings.warn.assert_not_called()

    @mock.patch('luigi.parameter.warnings')
    def test_no_warn_on_string_in_optional(self, warnings):
        class TestConfig(luigi.Config):
            param = luigi.OptionalParameter(default=None)

        TestConfig(param='value')
        warnings.warn.assert_not_called()

    @mock.patch('luigi.parameter.warnings')
    def test_warn_on_bad_type_in_optional(self, warnings):
        class TestConfig(luigi.Config):
            param = luigi.OptionalParameter()

        TestConfig(param=1)
        warnings.warn.assert_called_once_with('OptionalParameter "param" with value "1" is not of type string or None.')

    def test_optional_parameter_parse_none(self):
        self.assertIsNone(luigi.OptionalParameter().parse(''))

    def test_optional_parameter_parse_string(self):
        self.assertEqual('test', luigi.OptionalParameter().parse('test'))

    def test_optional_parameter_serialize_none(self):
        self.assertEqual('', luigi.OptionalParameter().serialize(None))

    def test_optional_parameter_serialize_string(self):
        self.assertEqual('test', luigi.OptionalParameter().serialize('test'))


class TestParametersHashability(LuigiTestCase):
    def test_date(self):
        class Foo(luigi.Task):
            args = luigi.parameter.DateParameter()
        p = luigi.parameter.DateParameter()
        self.assertEqual(hash(Foo(args=datetime.date(2000, 1, 1)).args), hash(p.parse('2000-1-1')))

    def test_dateminute(self):
        class Foo(luigi.Task):
            args = luigi.parameter.DateMinuteParameter()
        p = luigi.parameter.DateMinuteParameter()
        self.assertEqual(hash(Foo(args=datetime.datetime(2000, 1, 1, 12, 0)).args), hash(p.parse('2000-1-1T1200')))

    def test_dateinterval(self):
        class Foo(luigi.Task):
            args = luigi.parameter.DateIntervalParameter()
        p = luigi.parameter.DateIntervalParameter()
        di = luigi.date_interval.Custom(datetime.date(2000, 1, 1), datetime.date(2000, 2, 12))
        self.assertEqual(hash(Foo(args=di).args), hash(p.parse('2000-01-01-2000-02-12')))

    def test_timedelta(self):
        class Foo(luigi.Task):
            args = luigi.parameter.TimeDeltaParameter()
        p = luigi.parameter.TimeDeltaParameter()
        self.assertEqual(hash(Foo(args=datetime.timedelta(days=2, hours=3, minutes=2)).args), hash(p.parse('P2DT3H2M')))

    def test_boolean(self):
        class Foo(luigi.Task):
            args = luigi.parameter.BoolParameter()

        p = luigi.parameter.BoolParameter()

        self.assertEqual(hash(Foo(args=True).args), hash(p.parse('true')))
        self.assertEqual(hash(Foo(args=False).args), hash(p.parse('false')))

    def test_int(self):
        class Foo(luigi.Task):
            args = luigi.parameter.IntParameter()

        p = luigi.parameter.IntParameter()
        self.assertEqual(hash(Foo(args=1).args), hash(p.parse('1')))

    def test_float(self):
        class Foo(luigi.Task):
            args = luigi.parameter.FloatParameter()

        p = luigi.parameter.FloatParameter()
        self.assertEqual(hash(Foo(args=1.0).args), hash(p.parse('1')))

    def test_enum(self):
        class Foo(luigi.Task):
            args = luigi.parameter.EnumParameter(enum=MyEnum)

        p = luigi.parameter.EnumParameter(enum=MyEnum)
        self.assertEqual(hash(Foo(args=MyEnum.A).args), hash(p.parse('A')))

    def test_enum_list(self):
        class Foo(luigi.Task):
            args = luigi.parameter.EnumListParameter(enum=MyEnum)

        p = luigi.parameter.EnumListParameter(enum=MyEnum)
        self.assertEqual(hash(Foo(args=(MyEnum.A, MyEnum.C)).args), hash(p.parse('A,C')))

        class FooWithDefault(luigi.Task):
            args = luigi.parameter.EnumListParameter(enum=MyEnum, default=[MyEnum.C])

        self.assertEqual(FooWithDefault().args, p.parse('C'))

    def test_dict(self):
        class Foo(luigi.Task):
            args = luigi.parameter.DictParameter()

        p = luigi.parameter.DictParameter()
        self.assertEqual(hash(Foo(args=dict(foo=1, bar="hello")).args), hash(p.parse('{"foo":1,"bar":"hello"}')))

    def test_list(self):
        class Foo(luigi.Task):
            args = luigi.parameter.ListParameter()

        p = luigi.parameter.ListParameter()
        self.assertEqual(hash(Foo(args=[1, "hello"]).args), hash(p.normalize(p.parse('[1,"hello"]'))))

    def test_list_dict(self):
        class Foo(luigi.Task):
            args = luigi.parameter.ListParameter()

        p = luigi.parameter.ListParameter()
        self.assertEqual(hash(Foo(args=[{'foo': 'bar'}, {'doge': 'wow'}]).args),
                         hash(p.normalize(p.parse('[{"foo": "bar"}, {"doge": "wow"}]'))))

    def test_list_nested(self):
        class Foo(luigi.Task):
            args = luigi.parameter.ListParameter()

        p = luigi.parameter.ListParameter()
        self.assertEqual(hash(Foo(args=[['foo', 'bar'], ['doge', 'wow']]).args),
                         hash(p.normalize(p.parse('[["foo", "bar"], ["doge", "wow"]]'))))

    def test_tuple(self):
        class Foo(luigi.Task):
            args = luigi.parameter.TupleParameter()

        p = luigi.parameter.TupleParameter()
        self.assertEqual(hash(Foo(args=(1, "hello")).args), hash(p.parse('(1,"hello")')))

    def test_tuple_dict(self):
        class Foo(luigi.Task):
            args = luigi.parameter.TupleParameter()

        p = luigi.parameter.TupleParameter()
        self.assertEqual(hash(Foo(args=({'foo': 'bar'}, {'doge': 'wow'})).args),
                         hash(p.normalize(p.parse('({"foo": "bar"}, {"doge": "wow"})'))))

    def test_tuple_nested(self):
        class Foo(luigi.Task):
            args = luigi.parameter.TupleParameter()

        p = luigi.parameter.TupleParameter()
        self.assertEqual(hash(Foo(args=(('foo', 'bar'), ('doge', 'wow'))).args),
                         hash(p.normalize(p.parse('(("foo", "bar"), ("doge", "wow"))'))))

    def test_task(self):
        class Bar(luigi.Task):
            pass

        class Foo(luigi.Task):
            args = luigi.parameter.TaskParameter()

        p = luigi.parameter.TaskParameter()
        self.assertEqual(hash(Foo(args=Bar).args), hash(p.parse('Bar')))


class TestNewStyleGlobalParameters(LuigiTestCase):

    def setUp(self):
        super(TestNewStyleGlobalParameters, self).setUp()
        MockTarget.fs.clear()

    def expect_keys(self, expected):
        self.assertEqual(set(MockTarget.fs.get_all_data().keys()), set(expected))

    def test_x_arg(self):
        self.run_locally(['Banana', '--x', 'foo', '--y', 'bar', '--style', 'x-arg'])
        self.expect_keys(['banana-foo-bar', 'banana-dep-foo-def'])

    def test_x_arg_override(self):
        self.run_locally(['Banana', '--x', 'foo', '--y', 'bar', '--style', 'x-arg', '--BananaDep-y', 'xyz'])
        self.expect_keys(['banana-foo-bar', 'banana-dep-foo-xyz'])

    def test_x_arg_override_stupid(self):
        self.run_locally(['Banana', '--x', 'foo', '--y', 'bar', '--style', 'x-arg', '--BananaDep-x', 'blabla'])
        self.expect_keys(['banana-foo-bar', 'banana-dep-foo-def'])

    def test_x_arg_y_arg(self):
        self.run_locally(['Banana', '--x', 'foo', '--y', 'bar', '--style', 'x-arg-y-arg'])
        self.expect_keys(['banana-foo-bar', 'banana-dep-foo-bar'])

    def test_x_arg_y_arg_override(self):
        self.run_locally(['Banana', '--x', 'foo', '--y', 'bar', '--style', 'x-arg-y-arg', '--BananaDep-y', 'xyz'])
        self.expect_keys(['banana-foo-bar', 'banana-dep-foo-bar'])

    def test_x_arg_y_arg_override_all(self):
        self.run_locally(['Banana', '--x', 'foo',
                          '--y', 'bar', '--style', 'x-arg-y-arg', '--BananaDep-y',
                          'xyz', '--BananaDep-x', 'blabla'])
        self.expect_keys(['banana-foo-bar', 'banana-dep-foo-bar'])

    def test_y_arg_override(self):
        self.run_locally(['Banana', '--x', 'foo', '--y', 'bar', '--style', 'y-kwarg', '--BananaDep-x', 'xyz'])
        self.expect_keys(['banana-foo-bar', 'banana-dep-xyz-bar'])

    def test_y_arg_override_both(self):
        self.run_locally(['Banana', '--x', 'foo',
                          '--y', 'bar', '--style', 'y-kwarg', '--BananaDep-x', 'xyz',
                          '--BananaDep-y', 'blah'])
        self.expect_keys(['banana-foo-bar', 'banana-dep-xyz-bar'])

    def test_y_arg_override_banana(self):
        self.run_locally(['Banana', '--y', 'bar', '--style', 'y-kwarg', '--BananaDep-x', 'xyz', '--Banana-x', 'baz'])
        self.expect_keys(['banana-baz-bar', 'banana-dep-xyz-bar'])


class TestRemoveGlobalParameters(LuigiTestCase):

    def run_and_check(self, args):
        run_exit_status = self.run_locally(args)
        self.assertTrue(run_exit_status)
        return run_exit_status

    @parsing(['--MyConfig-mc-p', '99', '--mc-r', '55', 'NoopTask'])
    def test_use_config_class_1(self):
        self.assertEqual(MyConfig().mc_p, 99)
        self.assertEqual(MyConfig().mc_q, 73)
        self.assertEqual(MyConfigWithoutSection().mc_r, 55)
        self.assertEqual(MyConfigWithoutSection().mc_s, 99)

    @parsing(['NoopTask', '--MyConfig-mc-p', '99', '--mc-r', '55'])
    def test_use_config_class_2(self):
        self.assertEqual(MyConfig().mc_p, 99)
        self.assertEqual(MyConfig().mc_q, 73)
        self.assertEqual(MyConfigWithoutSection().mc_r, 55)
        self.assertEqual(MyConfigWithoutSection().mc_s, 99)

    @parsing(['--MyConfig-mc-p', '99', '--mc-r', '55', 'NoopTask', '--mc-s', '123', '--MyConfig-mc-q', '42'])
    def test_use_config_class_more_args(self):
        self.assertEqual(MyConfig().mc_p, 99)
        self.assertEqual(MyConfig().mc_q, 42)
        self.assertEqual(MyConfigWithoutSection().mc_r, 55)
        self.assertEqual(MyConfigWithoutSection().mc_s, 123)

    @with_config({"MyConfig": {"mc_p": "666", "mc_q": "777"}})
    @parsing(['--mc-r', '555', 'NoopTask'])
    def test_use_config_class_with_configuration(self):
        self.assertEqual(MyConfig().mc_p, 666)
        self.assertEqual(MyConfig().mc_q, 777)
        self.assertEqual(MyConfigWithoutSection().mc_r, 555)
        self.assertEqual(MyConfigWithoutSection().mc_s, 99)

    @with_config({"MyConfigWithoutSection": {"mc_r": "999", "mc_s": "888"}})
    @parsing(['NoopTask', '--MyConfig-mc-p', '222', '--mc-r', '555'])
    def test_use_config_class_with_configuration_2(self):
        self.assertEqual(MyConfig().mc_p, 222)
        self.assertEqual(MyConfig().mc_q, 73)
        self.assertEqual(MyConfigWithoutSection().mc_r, 555)
        self.assertEqual(MyConfigWithoutSection().mc_s, 888)

    @with_config({"MyConfig": {"mc_p": "555", "mc-p": "666", "mc-q": "777"}})
    def test_configuration_style(self):
        self.assertEqual(MyConfig().mc_p, 555)
        self.assertEqual(MyConfig().mc_q, 777)

    def test_misc_1(self):
        class Dogs(luigi.Config):
            n_dogs = luigi.IntParameter()

        class CatsWithoutSection(luigi.Config):
            use_cmdline_section = False
            n_cats = luigi.IntParameter()

        with luigi.cmdline_parser.CmdlineParser.global_instance(['--n-cats', '123', '--Dogs-n-dogs', '456', 'WithDefault'], allow_override=True):
            self.assertEqual(Dogs().n_dogs, 456)
            self.assertEqual(CatsWithoutSection().n_cats, 123)

        with luigi.cmdline_parser.CmdlineParser.global_instance(['WithDefault', '--n-cats', '321', '--Dogs-n-dogs', '654'], allow_override=True):
            self.assertEqual(Dogs().n_dogs, 654)
            self.assertEqual(CatsWithoutSection().n_cats, 321)

    def test_global_significant_param_warning(self):
        """ We don't want any kind of global param to be positional """
        with self.assertWarnsRegex(DeprecationWarning, 'is_global support is removed. Assuming positional=False'):
            class MyTask(luigi.Task):
                # This could typically be called "--test-dry-run"
                x_g1 = luigi.Parameter(default='y', is_global=True, significant=True)

        self.assertRaises(luigi.parameter.UnknownParameterException,
                          lambda: MyTask('arg'))

        def test_global_insignificant_param_warning(self):
            """ We don't want any kind of global param to be positional """
            with self.assertWarnsRegex(DeprecationWarning, 'is_global support is removed. Assuming positional=False'):
                class MyTask(luigi.Task):
                    # This could typically be "--yarn-pool=development"
                    x_g2 = luigi.Parameter(default='y', is_global=True, significant=False)

            self.assertRaises(luigi.parameter.UnknownParameterException,
                              lambda: MyTask('arg'))


class TestParamWithDefaultFromConfig(LuigiTestCase):

    def testNoSection(self):
        self.assertRaises(ParameterException, lambda: _value(luigi.Parameter(config_path=dict(section="foo", name="bar"))))

    @with_config({"foo": {}})
    def testNoValue(self):
        self.assertRaises(ParameterException, lambda: _value(luigi.Parameter(config_path=dict(section="foo", name="bar"))))

    @with_config({"foo": {"bar": "baz"}})
    def testDefault(self):
        class LocalA(luigi.Task):
            p = luigi.Parameter(config_path=dict(section="foo", name="bar"))

        self.assertEqual("baz", LocalA().p)
        self.assertEqual("boo", LocalA(p="boo").p)

    @with_config({"foo": {"bar": "2001-02-03T04"}})
    def testDateHour(self):
        p = luigi.DateHourParameter(config_path=dict(section="foo", name="bar"))
        self.assertEqual(datetime.datetime(2001, 2, 3, 4, 0, 0), _value(p))

    @with_config({"foo": {"bar": "2001-02-03T05"}})
    def testDateHourWithInterval(self):
        p = luigi.DateHourParameter(config_path=dict(section="foo", name="bar"), interval=2)
        self.assertEqual(datetime.datetime(2001, 2, 3, 4, 0, 0), _value(p))

    @with_config({"foo": {"bar": "2001-02-03T0430"}})
    def testDateMinute(self):
        p = luigi.DateMinuteParameter(config_path=dict(section="foo", name="bar"))
        self.assertEqual(datetime.datetime(2001, 2, 3, 4, 30, 0), _value(p))

    @with_config({"foo": {"bar": "2001-02-03T0431"}})
    def testDateWithMinuteInterval(self):
        p = luigi.DateMinuteParameter(config_path=dict(section="foo", name="bar"), interval=2)
        self.assertEqual(datetime.datetime(2001, 2, 3, 4, 30, 0), _value(p))

    @with_config({"foo": {"bar": "2001-02-03T04H30"}})
    def testDateMinuteDeprecated(self):
        p = luigi.DateMinuteParameter(config_path=dict(section="foo", name="bar"))
        with self.assertWarnsRegex(DeprecationWarning,
                                   'Using "H" between hours and minutes is deprecated, omit it instead.'):
            self.assertEqual(datetime.datetime(2001, 2, 3, 4, 30, 0), _value(p))

    @with_config({"foo": {"bar": "2001-02-03T040506"}})
    def testDateSecond(self):
        p = luigi.DateSecondParameter(config_path=dict(section="foo", name="bar"))
        self.assertEqual(datetime.datetime(2001, 2, 3, 4, 5, 6), _value(p))

    @with_config({"foo": {"bar": "2001-02-03T040507"}})
    def testDateSecondWithInterval(self):
        p = luigi.DateSecondParameter(config_path=dict(section="foo", name="bar"), interval=2)
        self.assertEqual(datetime.datetime(2001, 2, 3, 4, 5, 6), _value(p))

    @with_config({"foo": {"bar": "2001-02-03"}})
    def testDate(self):
        p = luigi.DateParameter(config_path=dict(section="foo", name="bar"))
        self.assertEqual(datetime.date(2001, 2, 3), _value(p))

    @with_config({"foo": {"bar": "2001-02-03"}})
    def testDateWithInterval(self):
        p = luigi.DateParameter(config_path=dict(section="foo", name="bar"),
                                interval=3, start=datetime.date(2001, 2, 1))
        self.assertEqual(datetime.date(2001, 2, 1), _value(p))

    @with_config({"foo": {"bar": "2015-07"}})
    def testMonthParameter(self):
        p = luigi.MonthParameter(config_path=dict(section="foo", name="bar"))
        self.assertEqual(datetime.date(2015, 7, 1), _value(p))

    @with_config({"foo": {"bar": "2015-07"}})
    def testMonthWithIntervalParameter(self):
        p = luigi.MonthParameter(config_path=dict(section="foo", name="bar"),
                                 interval=13, start=datetime.date(2014, 1, 1))
        self.assertEqual(datetime.date(2015, 2, 1), _value(p))

    @with_config({"foo": {"bar": "2015"}})
    def testYearParameter(self):
        p = luigi.YearParameter(config_path=dict(section="foo", name="bar"))
        self.assertEqual(datetime.date(2015, 1, 1), _value(p))

    @with_config({"foo": {"bar": "2015"}})
    def testYearWithIntervalParameter(self):
        p = luigi.YearParameter(config_path=dict(section="foo", name="bar"),
                                start=datetime.date(2011, 1, 1), interval=5)
        self.assertEqual(datetime.date(2011, 1, 1), _value(p))

    @with_config({"foo": {"bar": "123"}})
    def testInt(self):
        p = luigi.IntParameter(config_path=dict(section="foo", name="bar"))
        self.assertEqual(123, _value(p))

    @with_config({"foo": {"bar": "true"}})
    def testBool(self):
        p = luigi.BoolParameter(config_path=dict(section="foo", name="bar"))
        self.assertEqual(True, _value(p))

    @with_config({"foo": {"bar": "false"}})
    def testBoolConfigOutranksDefault(self):
        p = luigi.BoolParameter(default=True, config_path=dict(section="foo", name="bar"))
        self.assertEqual(False, _value(p))

    @with_config({"foo": {"bar": "2001-02-03-2001-02-28"}})
    def testDateInterval(self):
        p = luigi.DateIntervalParameter(config_path=dict(section="foo", name="bar"))
        expected = luigi.date_interval.Custom.parse("2001-02-03-2001-02-28")
        self.assertEqual(expected, _value(p))

    @with_config({"foo": {"bar": "0 seconds"}})
    def testTimeDeltaNoSeconds(self):
        p = luigi.TimeDeltaParameter(config_path=dict(section="foo", name="bar"))
        self.assertEqual(timedelta(seconds=0), _value(p))

    @with_config({"foo": {"bar": "0 d"}})
    def testTimeDeltaNoDays(self):
        p = luigi.TimeDeltaParameter(config_path=dict(section="foo", name="bar"))
        self.assertEqual(timedelta(days=0), _value(p))

    @with_config({"foo": {"bar": "1 day"}})
    def testTimeDelta(self):
        p = luigi.TimeDeltaParameter(config_path=dict(section="foo", name="bar"))
        self.assertEqual(timedelta(days=1), _value(p))

    @with_config({"foo": {"bar": "2 seconds"}})
    def testTimeDeltaPlural(self):
        p = luigi.TimeDeltaParameter(config_path=dict(section="foo", name="bar"))
        self.assertEqual(timedelta(seconds=2), _value(p))

    @with_config({"foo": {"bar": "3w 4h 5m"}})
    def testTimeDeltaMultiple(self):
        p = luigi.TimeDeltaParameter(config_path=dict(section="foo", name="bar"))
        self.assertEqual(timedelta(weeks=3, hours=4, minutes=5), _value(p))

    @with_config({"foo": {"bar": "P4DT12H30M5S"}})
    def testTimeDelta8601(self):
        p = luigi.TimeDeltaParameter(config_path=dict(section="foo", name="bar"))
        self.assertEqual(timedelta(days=4, hours=12, minutes=30, seconds=5), _value(p))

    @with_config({"foo": {"bar": "P5D"}})
    def testTimeDelta8601NoTimeComponent(self):
        p = luigi.TimeDeltaParameter(config_path=dict(section="foo", name="bar"))
        self.assertEqual(timedelta(days=5), _value(p))

    @with_config({"foo": {"bar": "P5W"}})
    def testTimeDelta8601Weeks(self):
        p = luigi.TimeDeltaParameter(config_path=dict(section="foo", name="bar"))
        self.assertEqual(timedelta(weeks=5), _value(p))

    @with_config({"foo": {"bar": "P3Y6M4DT12H30M5S"}})
    def testTimeDelta8601YearMonthNotSupported(self):
        def f():
            return _value(luigi.TimeDeltaParameter(config_path=dict(section="foo", name="bar")))
        self.assertRaises(luigi.parameter.ParameterException, f)  # ISO 8601 durations with years or months are not supported

    @with_config({"foo": {"bar": "PT6M"}})
    def testTimeDelta8601MAfterT(self):
        p = luigi.TimeDeltaParameter(config_path=dict(section="foo", name="bar"))
        self.assertEqual(timedelta(minutes=6), _value(p))

    @with_config({"foo": {"bar": "P6M"}})
    def testTimeDelta8601MBeforeT(self):
        def f():
            return _value(luigi.TimeDeltaParameter(config_path=dict(section="foo", name="bar")))
        self.assertRaises(luigi.parameter.ParameterException, f)  # ISO 8601 durations with months are not supported

    def testHasDefaultNoSection(self):
        self.assertRaises(luigi.parameter.MissingParameterException,
                          lambda: _value(luigi.Parameter(config_path=dict(section="foo", name="bar"))))

    @with_config({"foo": {}})
    def testHasDefaultNoValue(self):
        self.assertRaises(luigi.parameter.MissingParameterException,
                          lambda: _value(luigi.Parameter(config_path=dict(section="foo", name="bar"))))

    @with_config({"foo": {"bar": "baz"}})
    def testHasDefaultWithBoth(self):
        self.assertTrue(_value(luigi.Parameter(config_path=dict(section="foo", name="bar"))))

    @with_config({"foo": {"bar": "baz"}})
    def testWithDefault(self):
        p = luigi.Parameter(config_path=dict(section="foo", name="bar"), default='blah')
        self.assertEqual('baz', _value(p))  # config overrides default

    def testWithDefaultAndMissing(self):
        p = luigi.Parameter(config_path=dict(section="foo", name="bar"), default='blah')
        self.assertEqual('blah', _value(p))

    @with_config({"LocalA": {"p": "p_default"}})
    def testDefaultFromTaskName(self):
        class LocalA(luigi.Task):
            p = luigi.Parameter()

        self.assertEqual("p_default", LocalA().p)
        self.assertEqual("boo", LocalA(p="boo").p)

    @with_config({"LocalA": {"p": "999"}})
    def testDefaultFromTaskNameInt(self):
        class LocalA(luigi.Task):
            p = luigi.IntParameter()

        self.assertEqual(999, LocalA().p)
        self.assertEqual(777, LocalA(p=777).p)

    @with_config({"LocalA": {"p": "p_default"}, "foo": {"bar": "baz"}})
    def testDefaultFromConfigWithTaskNameToo(self):
        class LocalA(luigi.Task):
            p = luigi.Parameter(config_path=dict(section="foo", name="bar"))

        self.assertEqual("p_default", LocalA().p)
        self.assertEqual("boo", LocalA(p="boo").p)

    @with_config({"LocalA": {"p": "p_default_2"}})
    def testDefaultFromTaskNameWithDefault(self):
        class LocalA(luigi.Task):
            p = luigi.Parameter(default="banana")

        self.assertEqual("p_default_2", LocalA().p)
        self.assertEqual("boo_2", LocalA(p="boo_2").p)

    @with_config({"MyClass": {"p_wohoo": "p_default_3"}})
    def testWithLongParameterName(self):
        class MyClass(luigi.Task):
            p_wohoo = luigi.Parameter(default="banana")

        self.assertEqual("p_default_3", MyClass().p_wohoo)
        self.assertEqual("boo_2", MyClass(p_wohoo="boo_2").p_wohoo)

    @with_config({"RangeDaily": {"days_back": "123"}})
    def testSettingOtherMember(self):
        class LocalA(luigi.Task):
            pass

        self.assertEqual(123, luigi.tools.range.RangeDaily(of=LocalA).days_back)
        self.assertEqual(70, luigi.tools.range.RangeDaily(of=LocalA, days_back=70).days_back)

    @with_config({"MyClass": {"p_not_global": "123"}})
    def testCommandLineWithDefault(self):
        """
        Verify that we also read from the config when we build tasks from the
        command line parsers.
        """
        class MyClass(luigi.Task):
            p_not_global = luigi.Parameter(default='banana')

            def complete(self):
                import sys
                luigi.configuration.get_config().write(sys.stdout)
                if self.p_not_global != "123":
                    raise ValueError("The parameter didn't get set!!")
                return True

            def run(self):
                pass

        self.assertTrue(self.run_locally(['MyClass']))
        self.assertFalse(self.run_locally(['MyClass', '--p-not-global', '124']))
        self.assertFalse(self.run_locally(['MyClass', '--MyClass-p-not-global', '124']))

    @with_config({"MyClass2": {"p_not_global_no_default": "123"}})
    def testCommandLineNoDefault(self):
        """
        Verify that we also read from the config when we build tasks from the
        command line parsers.
        """
        class MyClass2(luigi.Task):
            """ TODO: Make luigi clean it's register for tests. Hate this 2 dance. """
            p_not_global_no_default = luigi.Parameter()

            def complete(self):
                import sys
                luigi.configuration.get_config().write(sys.stdout)
                luigi.configuration.get_config().write(sys.stdout)
                if self.p_not_global_no_default != "123":
                    raise ValueError("The parameter didn't get set!!")
                return True

            def run(self):
                pass

        self.assertTrue(self.run_locally(['MyClass2']))
        self.assertFalse(self.run_locally(['MyClass2', '--p-not-global-no-default', '124']))
        self.assertFalse(self.run_locally(['MyClass2', '--MyClass2-p-not-global-no-default', '124']))

    @with_config({"mynamespace.A": {"p": "999"}})
    def testWithNamespaceConfig(self):
        class A(luigi.Task):
            task_namespace = 'mynamespace'
            p = luigi.IntParameter()

        self.assertEqual(999, A().p)
        self.assertEqual(777, A(p=777).p)

    def testWithNamespaceCli(self):
        class A(luigi.Task):
            task_namespace = 'mynamespace'
            p = luigi.IntParameter(default=100)
            expected = luigi.IntParameter()

            def complete(self):
                if self.p != self.expected:
                    raise ValueError
                return True

        self.assertTrue(self.run_locally_split('mynamespace.A --expected 100'))
        # TODO(arash): Why is `--p 200` hanging with multiprocessing stuff?
        # self.assertTrue(self.run_locally_split('mynamespace.A --p 200 --expected 200'))
        self.assertTrue(self.run_locally_split('mynamespace.A --mynamespace.A-p 200 --expected 200'))
        self.assertFalse(self.run_locally_split('mynamespace.A --A-p 200 --expected 200'))

    def testListWithNamespaceCli(self):
        class A(luigi.Task):
            task_namespace = 'mynamespace'
            l_param = luigi.ListParameter(default=[1, 2, 3])
            expected = luigi.ListParameter()

            def complete(self):
                if self.l_param != self.expected:
                    raise ValueError
                return True

        self.assertTrue(self.run_locally_split('mynamespace.A --expected [1,2,3]'))
        self.assertTrue(self.run_locally_split('mynamespace.A --mynamespace.A-l [1,2,3] --expected [1,2,3]'))

    def testTupleWithNamespaceCli(self):
        class A(luigi.Task):
            task_namespace = 'mynamespace'
            t = luigi.TupleParameter(default=((1, 2), (3, 4)))
            expected = luigi.TupleParameter()

            def complete(self):
                if self.t != self.expected:
                    raise ValueError
                return True

        self.assertTrue(self.run_locally_split('mynamespace.A --expected ((1,2),(3,4))'))
        self.assertTrue(self.run_locally_split('mynamespace.A --mynamespace.A-t ((1,2),(3,4)) --expected ((1,2),(3,4))'))

    @with_config({"foo": {"bar": "[1,2,3]"}})
    def testListConfig(self):
        self.assertTrue(_value(luigi.ListParameter(config_path=dict(section="foo", name="bar"))))

    @with_config({"foo": {"bar": "((1,2),(3,4))"}})
    def testTupleConfig(self):
        self.assertTrue(_value(luigi.TupleParameter(config_path=dict(section="foo", name="bar"))))

    @with_config({"foo": {"bar": "-3"}})
    def testNumericalParameter(self):
        p = luigi.NumericalParameter(min_value=-3, max_value=7, var_type=int, config_path=dict(section="foo", name="bar"))
        self.assertEqual(-3, _value(p))

    @with_config({"foo": {"bar": "3"}})
    def testChoiceParameter(self):
        p = luigi.ChoiceParameter(var_type=int, choices=[1, 2, 3], config_path=dict(section="foo", name="bar"))
        self.assertEqual(3, _value(p))


class OverrideEnvStuff(LuigiTestCase):

    @with_config({"core": {"default-scheduler-port": '6543'}})
    def testOverrideSchedulerPort(self):
        with self.assertWarnsRegex(DeprecationWarning, r'default-scheduler-port is deprecated'):
            env_params = luigi.interface.core()
            self.assertEqual(env_params.scheduler_port, 6543)

    @with_config({"core": {"scheduler-port": '6544'}})
    def testOverrideSchedulerPort2(self):
        with self.assertWarnsRegex(DeprecationWarning, r'scheduler-port \(with dashes\) should be avoided'):
            env_params = luigi.interface.core()
        self.assertEqual(env_params.scheduler_port, 6544)

    @with_config({"core": {"scheduler_port": '6545'}})
    def testOverrideSchedulerPort3(self):
        env_params = luigi.interface.core()
        self.assertEqual(env_params.scheduler_port, 6545)


class TestSerializeDateParameters(LuigiTestCase):

    def testSerialize(self):
        date = datetime.date(2013, 2, 3)
        self.assertEqual(luigi.DateParameter().serialize(date), '2013-02-03')
        self.assertEqual(luigi.YearParameter().serialize(date), '2013')
        self.assertEqual(luigi.MonthParameter().serialize(date), '2013-02')
        dt = datetime.datetime(2013, 2, 3, 4, 5)
        self.assertEqual(luigi.DateHourParameter().serialize(dt), '2013-02-03T04')


class TestSerializeTimeDeltaParameters(LuigiTestCase):

    def testSerialize(self):
        tdelta = timedelta(weeks=5, days=4, hours=3, minutes=2, seconds=1)
        self.assertEqual(luigi.TimeDeltaParameter().serialize(tdelta), '5 w 4 d 3 h 2 m 1 s')
        tdelta = timedelta(seconds=0)
        self.assertEqual(luigi.TimeDeltaParameter().serialize(tdelta), '0 w 0 d 0 h 0 m 0 s')


class TestTaskParameter(LuigiTestCase):

    def testUsage(self):

        class MetaTask(luigi.Task):
            task_namespace = "mynamespace"
            a = luigi.TaskParameter()

            def run(self):
                self.__class__.saved_value = self.a

        class OtherTask(luigi.Task):
            task_namespace = "other_namespace"

        self.assertEqual(MetaTask(a=MetaTask).a, MetaTask)
        self.assertEqual(MetaTask(a=OtherTask).a, OtherTask)

        # So I first thought this "should" work, but actually it should not,
        # because it should not need to parse values known at run-time
        self.assertRaises(AttributeError,
                          lambda: MetaTask(a="mynamespace.MetaTask"))

        # But is should be able to parse command line arguments
        self.assertRaises(luigi.task_register.TaskClassNotFoundException,
                          lambda: (self.run_locally_split('mynamespace.MetaTask --a blah')))
        self.assertRaises(luigi.task_register.TaskClassNotFoundException,
                          lambda: (self.run_locally_split('mynamespace.MetaTask --a Taskk')))
        self.assertTrue(self.run_locally_split('mynamespace.MetaTask --a mynamespace.MetaTask'))
        self.assertEqual(MetaTask.saved_value, MetaTask)
        self.assertTrue(self.run_locally_split('mynamespace.MetaTask --a other_namespace.OtherTask'))
        self.assertEqual(MetaTask.saved_value, OtherTask)

    def testSerialize(self):

        class OtherTask(luigi.Task):

            def complete(self):
                return True

        class DepTask(luigi.Task):

            dep = luigi.TaskParameter()
            ran = False

            def complete(self):
                return self.__class__.ran

            def requires(self):
                return self.dep()

            def run(self):
                self.__class__.ran = True

        class MainTask(luigi.Task):

            def run(self):
                yield DepTask(dep=OtherTask)

        # OtherTask is serialized because it is used as an argument for DepTask.
        self.assertTrue(self.run_locally(['MainTask']))


class TestSerializeTupleParameter(LuigiTestCase):
    def testSerialize(self):
        the_tuple = (1, 2, 3)

        self.assertEqual(luigi.TupleParameter().parse(luigi.TupleParameter().serialize(the_tuple)), the_tuple)


class NewStyleParameters822Test(LuigiTestCase):
    """
    I bet these tests created at 2015-03-08 are reduntant by now (Oct 2015).
    But maintaining them anyway, just in case I have overlooked something.
    """
    # See https://github.com/spotify/luigi/issues/822

    def test_subclasses(self):
        class BarBaseClass(luigi.Task):
            x = luigi.Parameter(default='bar_base_default')

        class BarSubClass(BarBaseClass):
            pass

        in_parse(['BarSubClass', '--x', 'xyz', '--BarBaseClass-x', 'xyz'],
                 lambda task: self.assertEqual(task.x, 'xyz'))

        # https://github.com/spotify/luigi/issues/822#issuecomment-77782714
        in_parse(['BarBaseClass', '--BarBaseClass-x', 'xyz'],
                 lambda task: self.assertEqual(task.x, 'xyz'))


class LocalParameters1304Test(LuigiTestCase):
    """
    It was discussed and decided that local parameters (--x) should be
    semantically different from global parameters (--MyTask-x).

    The former sets only the parsed root task, and the later sets the parameter
    for all the tasks.

    https://github.com/spotify/luigi/issues/1304#issuecomment-148402284
    """
    def test_local_params(self):

        class MyTask(RunOnceTask):
            param1 = luigi.IntParameter()
            param2 = luigi.BoolParameter(default=False)

            def requires(self):
                if self.param1 > 0:
                    yield MyTask(param1=(self.param1 - 1))

            def run(self):
                assert self.param1 == 1 or not self.param2
                self.comp = True

        self.assertTrue(self.run_locally_split('MyTask --param1 1 --param2'))

    def test_local_takes_precedence(self):

        class MyTask(luigi.Task):
            param = luigi.IntParameter()

            def complete(self):
                return False

            def run(self):
                assert self.param == 5

        self.assertTrue(self.run_locally_split('MyTask --param 5 --MyTask-param 6'))

    def test_local_only_affects_root(self):

        class MyTask(RunOnceTask):
            param = luigi.IntParameter(default=3)

            def requires(self):
                assert self.param != 3
                if self.param == 5:
                    yield MyTask()

        # It would be a cyclic dependency if local took precedence
        self.assertTrue(self.run_locally_split('MyTask --param 5 --MyTask-param 6'))

    def test_range_doesnt_propagate_args(self):
        """
        Ensure that ``--task Range --of Blah --blah-arg 123`` doesn't work.

        This will of course not work unless support is explicitly added for it.
        But being a bit paranoid here and adding this test case so that if
        somebody decides to add it in the future, they'll be redircted to the
        dicussion in #1304
        """

        class Blah(RunOnceTask):
            date = luigi.DateParameter()
            blah_arg = luigi.IntParameter()

        # The SystemExit is assumed to be thrown by argparse
        self.assertRaises(SystemExit, self.run_locally_split, 'RangeDailyBase --of Blah --start 2015-01-01 --task-limit 1 --blah-arg 123')
        self.assertTrue(self.run_locally_split('RangeDailyBase --of Blah --start 2015-01-01 --task-limit 1 --Blah-blah-arg 123'))


class TaskAsParameterName1335Test(LuigiTestCase):
    def test_parameter_can_be_named_task(self):

        class MyTask(luigi.Task):
            # Indeed, this is not the most realistic example, but still ...
            task = luigi.IntParameter()

        self.assertTrue(self.run_locally_split('MyTask --task 5'))
