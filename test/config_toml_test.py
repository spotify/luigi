# -*- coding: utf-8 -*-
#
# Copyright 2018 Vote inc.
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
from luigi.configuration import LuigiTomlParser, get_config, add_config_path


from helpers import LuigiTestCase


class TomlConfigParserTest(LuigiTestCase):
    @classmethod
    def setUpClass(cls):
        add_config_path('test/testconfig/luigi.toml')
        add_config_path('test/testconfig/luigi_local.toml')

    def setUp(self):
        LuigiTomlParser._instance = None
        super(TomlConfigParserTest, self).setUp()

    def test_get_config(self):
        config = get_config('toml')
        self.assertIsInstance(config, LuigiTomlParser)

    def test_file_reading(self):
        config = get_config('toml')
        self.assertIn('hdfs', config.data)

    def test_get(self):
        config = get_config('toml')

        # test getting
        self.assertEqual(config.get('hdfs', 'client'), 'hadoopcli')
        self.assertEqual(config.get('hdfs', 'client', 'test'), 'hadoopcli')

        # test default
        self.assertEqual(config.get('hdfs', 'test', 'check'), 'check')
        with self.assertRaises(KeyError):
            config.get('hdfs', 'test')

        # test override
        self.assertEqual(config.get('hdfs', 'namenode_host'), 'localhost')
        # test non-string values
        self.assertEqual(config.get('hdfs', 'namenode_port'), 50030)

    def test_set(self):
        config = get_config('toml')

        self.assertEqual(config.get('hdfs', 'client'), 'hadoopcli')
        config.set('hdfs', 'client', 'test')
        self.assertEqual(config.get('hdfs', 'client'), 'test')
        config.set('hdfs', 'check', 'test me')
        self.assertEqual(config.get('hdfs', 'check'), 'test me')

    def test_has_option(self):
        config = get_config('toml')
        self.assertTrue(config.has_option('hdfs', 'client'))
        self.assertFalse(config.has_option('hdfs', 'nope'))
        self.assertFalse(config.has_option('nope', 'client'))


class HelpersTest(LuigiTestCase):
    def test_add_without_install(self):
        enabled = LuigiTomlParser.enabled
        LuigiTomlParser.enabled = False
        with self.assertRaises(ImportError):
            add_config_path('test/testconfig/luigi.toml')
        LuigiTomlParser.enabled = enabled

    def test_get_without_install(self):
        enabled = LuigiTomlParser.enabled
        LuigiTomlParser.enabled = False
        with self.assertRaises(ImportError):
            get_config('toml')
        LuigiTomlParser.enabled = enabled
