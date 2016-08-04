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

from helpers import unittest

import luigi


class ChoiceParameterTest(unittest.TestCase):

    _value = 2

    def test_parse(self):
        v = luigi.ChoiceParameter(choices=[1, 2, 5], var_type=int)
        self.assertEqual(v.parse(2), ChoiceParameterTest._value)

    def test_parse_convert(self):
        v = luigi.ChoiceParameter(choices=[1, 2, 5], var_type=int)
        self.assertEqual(v.parse("2"), ChoiceParameterTest._value)

    def test_parse_convert2(self):
        v = luigi.ChoiceParameter(choices=[1, 2, 5], var_type=int)
        self.assertEqual(v.parse(2.0), ChoiceParameterTest._value)

    def test_parse_invalid_choice(self):
        self.assertRaises(ValueError, lambda: luigi.ChoiceParameter(choices=[1, 2, 5], var_type=int).parse("-2"))

    def test_parse_invalid_input(self):
        self.assertRaises(ValueError, lambda: luigi.ChoiceParameter(choices=[1, 2, 5], var_type=int).parse("a"))

    def test_choices_param_missing(self):
        self.assertRaises(luigi.parameter.ParameterException, lambda: luigi.ChoiceParameter())

    def test_invalid_choice_type(self):
        self.assertRaises(AssertionError, lambda: luigi.ChoiceParameter(choices=[1, 2, "5"], var_type=int))
