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
import os

from luigi.configuration import LuigiConfigParser, get_config
from luigi.configuration.cfg_parser import InterpolationMissingEnvvarError

from helpers import LuigiTestCase, with_config


class ConfigParserTest(LuigiTestCase):

    environ = {
        "TESTVAR": "1",
    }

    def setUp(self):
        self.environ_backup = {
            os.environ[key] for key in self.environ
            if key in os.environ
        }
        for key, value in self.environ.items():
            os.environ[key] = value
        LuigiConfigParser._instance = None
        super(ConfigParserTest, self).setUp()

    def tearDown(self):
        for key in self.environ:
            os.environ.pop(key)
        for key, value in self.environ_backup:
            os.environ[key] = value

    @with_config({"test": {
        "a": "testval",
        "b": "%(a)s",
        "c": "%(a)s%(a)s",
    }})
    def test_basic_interpolation(self):
        # Make sure the default ConfigParser behaviour is not broken
        config = get_config()

        self.assertEqual(config.get("test", "b"), config.get("test", "a"))
        self.assertEqual(config.get("test", "c"), 2 * config.get("test", "a"))

    @with_config({"test": {
        "a": "${TESTVAR}",
        "b": "${TESTVAR} ${TESTVAR}",
        "c": "${TESTVAR} %(a)s",
        "d": "${NONEXISTING}",
    }})
    def test_env_interpolation(self):
        config = get_config()

        self.assertEqual(config.get("test", "a"), "1")
        self.assertEqual(config.getint("test", "a"), 1)
        self.assertEqual(config.getboolean("test", "a"), True)

        self.assertEqual(config.get("test", "b"), "1 1")

        self.assertEqual(config.get("test", "c"), "1 1")

        with self.assertRaises(InterpolationMissingEnvvarError):
            config.get("test", "d")

    @with_config({"test": {
        "foo-bar": "fob",
        "baz_qux": "bax",
    }})
    def test_underscore_vs_dash_style(self):
        config = get_config()
        self.assertEqual(config.get("test", "foo-bar"), "fob")
        self.assertEqual(config.get("test", "foo_bar"), "fob")
        self.assertEqual(config.get("test", "baz-qux"), "bax")
        self.assertEqual(config.get("test", "baz_qux"), "bax")

    @with_config({"test": {
        "foo-bar": "fob",
        "foo_bar": "bax",
    }})
    def test_underscore_vs_dash_style_priority(self):
        config = get_config()
        self.assertEqual(config.get("test", "foo-bar"), "bax")
        self.assertEqual(config.get("test", "foo_bar"), "bax")
