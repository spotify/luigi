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

from helpers import unittest, in_parse

import luigi
import luigi.interface
import json
import collections


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
