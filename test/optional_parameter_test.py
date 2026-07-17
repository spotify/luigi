import datetime
import warnings

import mock
from helpers import LuigiTestCase, with_config

import luigi


class OptionalParameterTest(LuigiTestCase):
    def actual_test(self, cls, default, expected_value, expected_type, bad_data, **kwargs):

        class TestConfig(luigi.Config):
            param = cls(default=default, **kwargs)
            empty_param = cls(default=default, **kwargs)

            def run(self):
                assert self.param == expected_value
                assert self.empty_param is None

        # Test parsing empty string (should be None)
        self.assertIsNone(cls(**kwargs).parse(""))

        # Test next_in_enumeration always returns None for summary
        self.assertIsNone(TestConfig.param.next_in_enumeration(expected_value))
        self.assertIsNone(TestConfig.param.next_in_enumeration(None))

        # Test that warning is raised only with bad type
        with mock.patch("luigi.parameter.warnings") as warnings:
            TestConfig()
            warnings.warn.assert_not_called()

        if cls != luigi.OptionalChoiceParameter:
            with mock.patch("luigi.parameter.warnings") as warnings:
                TestConfig(param=None)
                warnings.warn.assert_not_called()

            with mock.patch("luigi.parameter.warnings") as warnings:
                TestConfig(param=bad_data)
                if cls == luigi.OptionalBoolParameter:
                    warnings.warn.assert_not_called()
                else:
                    warnings.warn.assert_called_with(
                        '{} "param" with value "{}" is not of type "{}" or None.'.format(cls.__name__, bad_data, expected_type),
                        luigi.parameter.OptionalParameterTypeWarning,
                    )

        # Test with value from config
        self.assertTrue(luigi.build([TestConfig()], local_scheduler=True))

    @with_config({"TestConfig": {"param": "expected value", "empty_param": ""}})
    def test_optional_parameter(self):
        self.actual_test(luigi.OptionalParameter, None, "expected value", "str", 0)
        self.actual_test(luigi.OptionalParameter, "default value", "expected value", "str", 0)

    @with_config({"TestConfig": {"param": "10", "empty_param": ""}})
    def test_optional_int_parameter(self):
        self.actual_test(luigi.OptionalIntParameter, None, 10, "int", "bad data")
        self.actual_test(luigi.OptionalIntParameter, 1, 10, "int", "bad data")

    @with_config({"TestConfig": {"param": "true", "empty_param": ""}})
    def test_optional_bool_parameter(self):
        self.actual_test(luigi.OptionalBoolParameter, None, True, "bool", "bad data")
        self.actual_test(luigi.OptionalBoolParameter, False, True, "bool", "bad data")

    @with_config({"TestConfig": {"param": "10.5", "empty_param": ""}})
    def test_optional_float_parameter(self):
        self.actual_test(luigi.OptionalFloatParameter, None, 10.5, "float", "bad data")
        self.actual_test(luigi.OptionalFloatParameter, 1.5, 10.5, "float", "bad data")

    @with_config({"TestConfig": {"param": '{"a": 10}', "empty_param": ""}})
    def test_optional_dict_parameter(self):
        self.actual_test(luigi.OptionalDictParameter, None, {"a": 10}, "FrozenOrderedDict", "bad data")
        self.actual_test(luigi.OptionalDictParameter, {"a": 1}, {"a": 10}, "FrozenOrderedDict", "bad data")

    @with_config({"TestConfig": {"param": "[10.5]", "empty_param": ""}})
    def test_optional_list_parameter(self):
        self.actual_test(luigi.OptionalListParameter, None, (10.5,), "tuple", "bad data")
        self.actual_test(luigi.OptionalListParameter, (1.5,), (10.5,), "tuple", "bad data")

    @with_config({"TestConfig": {"param": "[10.5]", "empty_param": ""}})
    def test_optional_tuple_parameter(self):
        self.actual_test(luigi.OptionalTupleParameter, None, (10.5,), "tuple", "bad data")
        self.actual_test(luigi.OptionalTupleParameter, (1.5,), (10.5,), "tuple", "bad data")

    @with_config({"TestConfig": {"param": "10.5", "empty_param": ""}})
    def test_optional_numerical_parameter_float(self):
        self.actual_test(luigi.OptionalNumericalParameter, None, 10.5, "float", "bad data", var_type=float, min_value=0, max_value=100)
        self.actual_test(luigi.OptionalNumericalParameter, 1.5, 10.5, "float", "bad data", var_type=float, min_value=0, max_value=100)

    @with_config({"TestConfig": {"param": "10", "empty_param": ""}})
    def test_optional_numerical_parameter_int(self):
        self.actual_test(luigi.OptionalNumericalParameter, None, 10, "int", "bad data", var_type=int, min_value=0, max_value=100)
        self.actual_test(luigi.OptionalNumericalParameter, 1, 10, "int", "bad data", var_type=int, min_value=0, max_value=100)

    @with_config({"TestConfig": {"param": "expected value", "empty_param": ""}})
    def test_optional_choice_parameter(self):
        choices = ["default value", "expected value"]
        self.actual_test(luigi.OptionalChoiceParameter, None, "expected value", "str", "bad data", choices=choices)
        self.actual_test(luigi.OptionalChoiceParameter, "default value", "expected value", "str", "bad data", choices=choices)

    def test_optional_date_parameter(self):
        """Unlike actual_test, this skips the bad-data warning check because
        DateParameter.normalize() crashes on non-date types."""
        expected_value = datetime.date(2013, 7, 10)
        config_value = "2013-07-10"
        param = luigi.OptionalDateParameter()

        # parse("") → None
        self.assertIsNone(param.parse(""))

        # parse(valid_string) → expected
        self.assertEqual(param.parse(config_value), expected_value)

        # serialize(None) → ""
        self.assertEqual(param.serialize(None), "")

        # serialize(value) → string
        self.assertEqual(param.serialize(expected_value), config_value)

        # next_in_enumeration → None
        self.assertIsNone(param.next_in_enumeration(expected_value))
        self.assertIsNone(param.next_in_enumeration(None))

        # Setting None should not trigger a warning
        with mock.patch("luigi.parameter.warnings") as mocked:

            @with_config({"Cfg": {"param": config_value, "empty": ""}})
            def inner(self_inner):
                class Cfg(luigi.Config):
                    param = luigi.OptionalDateParameter()
                    empty = luigi.OptionalDateParameter()

                    def run(inner_self):
                        self_inner.assertEqual(inner_self.param, expected_value)
                        self_inner.assertIsNone(inner_self.empty)

                Cfg(param=None)
                mocked.warn.assert_not_called()
                self_inner.assertTrue(luigi.build([Cfg()], local_scheduler=True))

            inner(self)

        # Should also work with an explicit default value
        @with_config({"Cfg2": {"param": config_value, "empty": ""}})
        def inner2(self_inner):
            class Cfg2(luigi.Config):
                param = luigi.OptionalDateParameter(default=datetime.date(2000, 1, 1))
                empty = luigi.OptionalDateParameter(default=datetime.date(2000, 1, 1))

                def run(inner_self):
                    self_inner.assertEqual(inner_self.param, expected_value)
                    self_inner.assertIsNone(inner_self.empty)

            self_inner.assertTrue(luigi.build([Cfg2()], local_scheduler=True))

        inner2(self)

    @with_config({"TestConfig": {"param": "1", "empty_param": ""}})
    def test_optional_choice_parameter_int(self):
        choices = [0, 1, 2]
        self.actual_test(luigi.OptionalChoiceParameter, None, 1, "int", "bad data", var_type=int, choices=choices)
        self.actual_test(luigi.OptionalChoiceParameter, "default value", 1, "int", "bad data", var_type=int, choices=choices)

    def test_warning(self):
        class TestOptionalFloatParameterSingleType(luigi.parameter.OptionalParameter, luigi.FloatParameter):
            expected_type = float

        class TestOptionalFloatParameterMultiTypes(luigi.parameter.OptionalParameter, luigi.FloatParameter):
            expected_type = (int, float)

        class TestConfig(luigi.Config):
            param_single = TestOptionalFloatParameterSingleType()
            param_multi = TestOptionalFloatParameterMultiTypes()

        with warnings.catch_warnings(record=True) as record:
            TestConfig(param_single=0.0, param_multi=1.0)

        assert len(record) == 0

        with warnings.catch_warnings(record=True) as record:
            warnings.filterwarnings(
                action="ignore",
                category=Warning,
            )
            warnings.simplefilter(
                action="always",
                category=luigi.parameter.OptionalParameterTypeWarning,
            )
            assert luigi.build([TestConfig(param_single="0", param_multi="1")], local_scheduler=True)

        assert len(record) == 2
        assert issubclass(record[0].category, luigi.parameter.OptionalParameterTypeWarning)
        assert issubclass(record[1].category, luigi.parameter.OptionalParameterTypeWarning)
        assert str(record[0].message) == ('TestOptionalFloatParameterSingleType "param_single" with value "0" is not of type "float" or None.')
        assert str(record[1].message) == ('TestOptionalFloatParameterMultiTypes "param_multi" with value "1" is not of any type in ["int", "float"] or None.')
