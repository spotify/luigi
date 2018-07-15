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
    def test_parse_str(self):
        d = luigi.ChoiceParameter(choices=["1", "2", "3"])
        self.assertEqual("3", d.parse("3"))

    def test_parse_int(self):
        d = luigi.ChoiceParameter(var_type=int, choices=[1, 2, 3])
        self.assertEqual(3, d.parse(3))

    def test_parse_int_conv(self):
        d = luigi.ChoiceParameter(var_type=int, choices=[1, 2, 3])
        self.assertEqual(3, d.parse("3"))

    def test_invalid_choice(self):
        d = luigi.ChoiceParameter(choices=["1", "2", "3"])
        self.assertRaises(ValueError, lambda: d.parse("xyz"))

    def test_invalid_choice_type(self):
        self.assertRaises(AssertionError, lambda: luigi.ChoiceParameter(var_type=int, choices=[1, 2, "3"]))

    def test_choices_parameter_exception(self):
        self.assertRaises(luigi.parameter.ParameterException, lambda: luigi.ChoiceParameter(var_type=int))

    def test_hash_str(self):
        class Foo(luigi.Task):
            args = luigi.ChoiceParameter(var_type=str, choices=["1", "2", "3"])
        p = luigi.ChoiceParameter(var_type=str, choices=["3", "2", "1"])
        self.assertEqual(hash(Foo(args="3").args), hash(p.parse("3")))

    def test_serialize_parse(self):
        a = luigi.ChoiceParameter(var_type=str, choices=["1", "2", "3"])
        b = "3"
        self.assertEqual(b, a.parse(a.serialize(b)))

    def test_invalid_choice_task(self):
        class Foo(luigi.Task):
            args = luigi.ChoiceParameter(var_type=str, choices=["1", "2", "3"])
        self.assertRaises(ValueError, lambda: Foo(args="4"))
