import datetime
import warnings

import mock
from helpers import LuigiTestCase, with_config

import luigi
from luigi import date_interval


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

    def _test_optional_date_like(self, cls, config_key, config_value, expected_value, default_value):
        """Dedicated helper for date/datetime optional parameters.

        Unlike actual_test, this skips the bad-data warning check because
        date/datetime normalize() crashes on non-date types."""
        param = cls()

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

            @with_config({"Cfg": {config_key: config_value, "empty": ""}})
            def inner(self_inner):
                class Cfg(luigi.Config):
                    param = cls()
                    empty = cls()

                    def run(inner_self):
                        self_inner.assertEqual(inner_self.param, expected_value)
                        self_inner.assertIsNone(inner_self.empty)

                Cfg(param=None)
                mocked.warn.assert_not_called()
                self_inner.assertTrue(luigi.build([Cfg()], local_scheduler=True))

            inner(self)

        # Should also work with an explicit default value
        @with_config({"Cfg2": {config_key: config_value, "empty": ""}})
        def inner2(self_inner):
            class Cfg2(luigi.Config):
                param = cls(default=default_value)
                empty = cls(default=default_value)

                def run(inner_self):
                    self_inner.assertEqual(inner_self.param, expected_value)
                    self_inner.assertIsNone(inner_self.empty)

            self_inner.assertTrue(luigi.build([Cfg2()], local_scheduler=True))

        inner2(self)

    def test_optional_date_parameter(self):
        self._test_optional_date_like(
            luigi.OptionalDateParameter,
            "param",
            "2013-07-10",
            datetime.date(2013, 7, 10),
            datetime.date(2000, 1, 1),
        )

    def test_optional_month_parameter(self):
        self._test_optional_date_like(
            luigi.OptionalMonthParameter,
            "param",
            "2013-07",
            datetime.date(2013, 7, 1),
            datetime.date(2000, 1, 1),
        )

    def test_optional_year_parameter(self):
        self._test_optional_date_like(
            luigi.OptionalYearParameter,
            "param",
            "2013",
            datetime.date(2013, 1, 1),
            datetime.date(2000, 1, 1),
        )

    def test_optional_date_hour_parameter(self):
        self._test_optional_date_like(
            luigi.OptionalDateHourParameter,
            "param",
            "2013-07-10T19",
            datetime.datetime(2013, 7, 10, 19),
            datetime.datetime(2000, 1, 1, 0),
        )

    def test_optional_date_minute_parameter(self):
        self._test_optional_date_like(
            luigi.OptionalDateMinuteParameter,
            "param",
            "2013-07-10T1907",
            datetime.datetime(2013, 7, 10, 19, 7),
            datetime.datetime(2000, 1, 1, 0, 0),
        )

    def test_optional_date_second_parameter(self):
        self._test_optional_date_like(
            luigi.OptionalDateSecondParameter,
            "param",
            "2013-07-10T190738",
            datetime.datetime(2013, 7, 10, 19, 7, 38),
            datetime.datetime(2000, 1, 1, 0, 0, 0),
        )

    def test_optional_date_interval_parameter(self):
        expected = date_interval.Date.parse("2015-11-04")
        default_val = date_interval.Date.parse("2000-01-01")
        self._test_optional_date_like(
            luigi.OptionalDateIntervalParameter,
            "param",
            "2015-11-04",
            expected,
            default_val,
        )

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
