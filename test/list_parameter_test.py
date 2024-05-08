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
import json
import mock
import pytest


class ListParameterTask(luigi.Task):
    param = luigi.ListParameter()


class ListParameterTest(unittest.TestCase):

    _list = [1, "one", True]

    def test_parse(self):
        d = luigi.ListParameter().parse(json.dumps(ListParameterTest._list))
        self.assertEqual(d, ListParameterTest._list)

    def test_serialize(self):
        d = luigi.ListParameter().serialize(ListParameterTest._list)
        self.assertEqual(d, '[1, "one", true]')

    def test_list_serialize_parse(self):
        a = luigi.ListParameter()
        b_list = [1, 2, 3]
        self.assertEqual(b_list, a.parse(a.serialize(b_list)))

    def test_parse_interface(self):
        in_parse(["ListParameterTask", "--param", '[1, "one", true]'],
                 lambda task: self.assertEqual(task.param, tuple(ListParameterTest._list)))

    def test_serialize_task(self):
        t = ListParameterTask(ListParameterTest._list)
        self.assertEqual(str(t), 'ListParameterTask(param=[1, "one", true])')

    def test_parse_invalid_input(self):
        self.assertRaises(ValueError, lambda: luigi.ListParameter().parse('{"invalid"}'))

    def test_hash_normalize(self):
        self.assertRaises(TypeError, lambda: hash(luigi.ListParameter().parse('"NOT A LIST"')))
        a = luigi.ListParameter().normalize([0])
        b = luigi.ListParameter().normalize([0])
        self.assertEqual(hash(a), hash(b))

    def test_schema(self):
        a = luigi.ListParameter(
            schema={
                "type": "array",
                "items": {
                    "type": "number",
                    "minimum": 0,
                    "maximum": 10,
                },
                "minItems": 1,
            }
        )

        # Check that the default value is validated
        with pytest.raises(ValidationError, match=r"'INVALID_ATTRIBUTE' is not of type 'number'"):
            a.normalize(["INVALID_ATTRIBUTE"])

        # Check that empty list is not valid
        with pytest.raises(ValidationError):
            a.normalize([])

        # Check that valid lists work
        valid_list = [1, 2, 3]
        a.normalize(valid_list)

        # Check that invalid lists raise correct errors
        invalid_list_type = ["NOT AN INT"]
        invalid_list_value = [-999, 4]

        with pytest.raises(ValidationError, match="'NOT AN INT' is not of type 'number'"):
            a.normalize(invalid_list_type)

        with pytest.raises(ValidationError, match="-999 is less than the minimum of 0"):
            a.normalize(invalid_list_value)

        # Check that warnings are properly emitted
        with mock.patch('luigi.parameter._JSONSCHEMA_ENABLED', False):
            with pytest.warns(
                UserWarning,
                match=(
                    "The 'jsonschema' package is not installed so the parameter can not be "
                    "validated even though a schema is given."
                )
            ):
                luigi.ListParameter(schema={"type": "array", "items": {"type": "number"}})

        # Test with a custom validator
        validator = Draft4Validator(
            schema={
                "type": "array",
                "items": {
                    "type": "number",
                    "minimum": 0,
                    "maximum": 10,
                },
                "minItems": 1,
            }
        )
        c = luigi.DictParameter(schema=validator)
        c.normalize(valid_list)
        with pytest.raises(ValidationError, match=r"'INVALID_ATTRIBUTE' is not of type 'number'",):
            c.normalize(["INVALID_ATTRIBUTE"])

        # Test with frozen data
        frozen_data = luigi.freezing.recursively_freeze(valid_list)
        c.normalize(frozen_data)
