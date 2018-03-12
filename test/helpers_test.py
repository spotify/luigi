# -*- coding: utf-8 -*-
#
# Copyright 2012-2016 Spotify AB
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
import luigi
import luigi.date_interval
import luigi.interface
import luigi.notifications
from helpers import LuigiTestCase, RunOnceTask


class LuigiTestCaseTest(LuigiTestCase):

    def test_1(self):
        class MyClass(luigi.Task):
            pass

        self.assertTrue(self.run_locally(['MyClass']))

    def test_2(self):
        class MyClass(luigi.Task):
            pass

        self.assertTrue(self.run_locally(['MyClass']))


class RunOnceTaskTest(LuigiTestCase):

    def test_complete_behavior(self):
        """
        Verify that RunOnceTask works as expected.

        This task will fail if it is a normal ``luigi.Task``, because
        RequiringTask will not run (missing dependency at runtime).
        """
        class MyTask(RunOnceTask):
            pass

        class RequiringTask(luigi.Task):
            counter = 0

            def requires(self):
                yield MyTask()

            def run(self):
                RequiringTask.counter += 1

        self.run_locally(['RequiringTask'])
        self.assertEqual(1, RequiringTask.counter)
