# -*- coding: utf-8 -*-
#
# Copyright 2015-2015 Spotify AB
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
from helpers import LuigiTestCase, with_config
import mock
import luigi
import luigi.scheduler
from luigi.cmdline import luigi_run


class RetcodesTest(LuigiTestCase):

    def run_and_expect(self, joined_params, retcode):
        with mock.patch('sys.exit') as se:
            luigi_run(joined_params.split(' ') + ['--local-scheduler', '--no-lock'])
            se.assert_called_once_with(retcode)

    def test_task_failed(self):
        class FailingTask(luigi.Task):
            def run(self):
                raise ValueError()

        self.run_and_expect('FailingTask', 0)  # Test default value to be 0
        self.run_and_expect('FailingTask --retcode-task-failed 5', 5)
        with_config(dict(cmdline=dict(retcode_task_failed='3')))(self.run_and_expect)('FailingTask', 3)

    def test_missing_data(self):
        class MissingDataTask(luigi.ExternalTask):
            def complete(self):
                return False

        self.run_and_expect('MissingDataTask', 0)  # Test default value to be 0
        self.run_and_expect('MissingDataTask --retcode-missing-data 5', 5)
        with_config(dict(cmdline=dict(retcode_missing_data='3')))(self.run_and_expect)('MissingDataTask', 3)

    def test_already_running(self):
        class AlreadyRunningTask(luigi.Task):
            def run(self):
                pass

        old_func = luigi.scheduler.CentralPlannerScheduler.get_work

        def new_func(*args, **kwargs):
            old_func(*args, **kwargs)
            res = old_func(*args, **kwargs)
            res['running_tasks'][0]['worker'] = "not me :)"  # Otherwise it will be filtered
            return res

        with mock.patch('luigi.scheduler.CentralPlannerScheduler.get_work', new_func):
            self.run_and_expect('AlreadyRunningTask', 0)  # Test default value to be 0
            self.run_and_expect('AlreadyRunningTask --retcode-already-running 5', 5)
            with_config(dict(cmdline=dict(retcode_already_running='3')))(self.run_and_expect)('AlreadyRunningTask', 3)
