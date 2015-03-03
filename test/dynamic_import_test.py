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
import luigi.interface


class ExtraArgs(luigi.Task):
    blah = luigi.Parameter(is_global=True, default=444)


class CmdlineTest(unittest.TestCase):

    def test_dynamic_loading(self):
        interface = luigi.interface.ArgParseInterface()
        self.assertRaises(SystemExit, interface.parse, (['FooTask', '--blah', 'xyz', '--x', '123'],))  # should raise since it's not imported

        interface = luigi.interface.DynamicArgParseInterface()
        tasks = interface.parse(['--module', 'foo_module', 'FooTask', '--blah', 'xyz', '--x', '123'])

        self.assertEqual(ExtraArgs().blah, 'xyz')

        self.assertEqual(len(tasks), 1)

        task, = tasks
        self.assertEqual(task.x, 123)

        import foo_module
        self.assertEqual(task.__class__, foo_module.FooTask)
        self.assertEqual(task, foo_module.FooTask(x=123))

    def test_run(self):
        # TODO: this needs to run after the existing module, since by now foo_module is already imported

        luigi.run(['--local-scheduler', '--no-lock', '--module', 'foo_module', 'FooTask', '--x', '100'], use_dynamic_argparse=True)
