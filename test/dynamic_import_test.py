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

from helpers import unittest, LuigiTestCase

import luigi
import luigi.interface
import tempfile
import re


class ExtraArgs(luigi.Task):
    blah = luigi.Parameter(default=444)


class CmdlineTest(LuigiTestCase):

    def test_dynamic_loading(self):
        interface = luigi.interface.DynamicArgParseInterface()
        with tempfile.NamedTemporaryFile(dir='test/', prefix="_foo_module", suffix='.py') as temp_module_file:
            temp_module_file.file.write(b'''
import luigi

class FooTask(luigi.Task):
    x = luigi.IntParameter()
''')
            temp_module_file.file.flush()
            temp_module_path = temp_module_file.name
            temp_module_name = re.search(r'/(_foo_module.*).py', temp_module_path).group(1)
            tasks = interface.parse(['--module', temp_module_name, 'FooTask', '--ExtraArgs-blah', 'xyz', '--x', '123'])

            self.assertEqual(ExtraArgs().blah, 'xyz')

            self.assertEqual(len(tasks), 1)

            task, = tasks
            self.assertEqual(task.x, 123)

            temp_module = __import__(temp_module_name)
            self.assertEqual(task.__class__, temp_module.FooTask)
            self.assertEqual(task, temp_module.FooTask(x=123))
