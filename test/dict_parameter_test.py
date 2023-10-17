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

from jsonschema import Draft4Validator
from jsonschema.exceptions import ValidationError
from helpers import unittest, in_parse

import luigi
import luigi.interface
import json
import mock
import collections
import pytest


class DictParameterTask(luigi.Task):
    param = luigi.DictParameter()


class DictParameterTest(unittest.TestCase):

    _dict = collections.OrderedDict([('username', 'me'), ('password', 'secret')])

    def test_parse(self):
        d = luigi.DictParameter().parse(json.dumps(DictParameterTest._dict))
        self.assertEqual(d, DictParameterTest._dict)

    def test_serialize(self):
        d = luigi.DictParameter().serialize(DictParameterTest._dict)
        self.assertEqual(d, '{"username": "me", "password": "secret"}')

    def test_parse_and_serialize(self):
        inputs = ['{"username": "me", "password": "secret"}', '{"password": "secret", "username": "me"}']
        for json_input in inputs:
            _dict = luigi.DictParameter().parse(json_input)
            self.assertEqual(json_input, luigi.DictParameter().serialize(_dict))

    def test_parse_interface(self):
        in_parse(["DictParameterTask", "--param", '{"username": "me", "password": "secret"}'],
                 lambda task: self.assertEqual(task.param, DictParameterTest._dict))

    def test_serialize_task(self):
        t = DictParameterTask(DictParameterTest._dict)
        self.assertEqual(str(t), 'DictParameterTask(param={"username": "me", "password": "secret"})')

    def test_parse_invalid_input(self):
        self.assertRaises(ValueError, lambda: luigi.DictParameter().parse('{"invalid"}'))

    def test_hash_normalize(self):
        self.assertRaises(TypeError, lambda: hash(luigi.DictParameter().parse('{"a": {"b": []}}')))
        a = luigi.DictParameter().normalize({"a": [{"b": []}]})
        b = luigi.DictParameter().normalize({"a": [{"b": []}]})
        self.assertEqual(hash(a), hash(b))

    def test_schema(self):
        a = luigi.parameter.DictParameter(
            schema={
                "type": "object",
                "properties": {
                    "an_int": {"type": "integer"},
                    "an_optional_str": {"type": "string"},
                },
                "additionalProperties": False,
                "required": ["an_int"],
            },
        )

        # Check that the default value is validated
        with pytest.raises(
            ValidationError,
            match=r"Additional properties are not allowed \('INVALID_ATTRIBUTE' was unexpected\)",
        ):
            a.normalize({"INVALID_ATTRIBUTE": 0})

        # Check that empty dict is not valid
        with pytest.raises(ValidationError, match="'an_int' is a required property"):
            a.normalize({})

        # Check that valid dicts work
        a.normalize({"an_int": 1})
        a.normalize({"an_int": 1, "an_optional_str": "hello"})

        # Check that invalid dicts raise correct errors
        with pytest.raises(ValidationError, match="'999' is not of type 'integer'"):
            a.normalize({"an_int": "999"})

        with pytest.raises(ValidationError, match="999 is not of type 'string'"):
            a.normalize({"an_int": 1, "an_optional_str": 999})

        # Test the example given in docstring
        b = luigi.DictParameter(
            schema={
              "type": "object",
              "patternProperties": {
                ".*": {"type": "string", "enum": ["web", "staging"]},
              }
            }
          )
        b.normalize({"role": "web", "env": "staging"})
        with pytest.raises(ValidationError, match=r"'UNKNOWN_VALUE' is not one of \['web', 'staging'\]"):
            b.normalize({"role": "UNKNOWN_VALUE", "env": "staging"})

        # Check that warnings are properly emitted
        with mock.patch('luigi.parameter._JSONSCHEMA_ENABLED', False):
            with pytest.warns(
                UserWarning,
                match=(
                    "The 'jsonschema' package is not installed so the parameter can not be "
                    "validated even though a schema is given."
                )
            ):
                luigi.ListParameter(schema={"type": "object"})

        # Test with a custom validator
        validator = Draft4Validator(
            schema={
              "type": "object",
              "patternProperties": {
                ".*": {"type": "string", "enum": ["web", "staging"]},
              },
            }
        )
        c = luigi.DictParameter(schema=validator)
        c.normalize({"role": "web", "env": "staging"})
        with pytest.raises(ValidationError, match=r"'UNKNOWN_VALUE' is not one of \['web', 'staging'\]"):
            c.normalize({"role": "UNKNOWN_VALUE", "env": "staging"})

        # Test with frozen data
        frozen_data = luigi.freezing.recursively_freeze({"role": "web", "env": "staging"})
        c.normalize(frozen_data)
