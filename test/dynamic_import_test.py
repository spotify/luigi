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

from helpers import LuigiTestCase, temporary_unloaded_module

import luigi
import luigi.interface

CONTENTS = b'''
import luigi

class FooTask(luigi.Task):
    x = luigi.IntParameter()

    def run(self):
        luigi._testing_glob_var = self.x
'''


class CmdlineTest(LuigiTestCase):

    def test_dynamic_loading(self):
        with temporary_unloaded_module(CONTENTS) as temp_module_name:
            luigi.interface.run(['--module', temp_module_name, 'FooTask', '--x', '123', '--local-scheduler', '--no-lock'])
            self.assertEqual(luigi._testing_glob_var, 123)
