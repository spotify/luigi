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

from unittest import TestCase

from luigi import hive


class TestHiveTask(TestCase):

    def test_error_cmd(self):
        self.assertRaises(hive.HiveCommandError, hive.run_hive_cmd, "this is a bogus command and should cause an error;")

    def test_ok_cmd(self):
        "Test that SHOW TABLES doesn't throw an error"
        hive.run_hive_cmd("SHOW TABLES;")
