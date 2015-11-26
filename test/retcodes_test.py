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

    def run_and_expect(self, joined_params, retcode, extra_args=['--local-scheduler', '--no-lock']):
        with self.assertRaises(SystemExit) as cm:
            luigi_run((joined_params.split(' ') + extra_args))
        self.assertEqual(cm.exception.code, retcode)

    def run_with_config(self, retcode_config, *args, **kwargs):
        with_config(dict(retcode=retcode_config))(self.run_and_expect)(*args, **kwargs)

    def test_task_failed(self):
        class FailingTask(luigi.Task):
            def run(self):
                raise ValueError()

        self.run_and_expect('FailingTask', 0)  # Test default value to be 0
        self.run_and_expect('FailingTask --retcode-task-failed 5', 5)
        self.run_with_config(dict(task_failed='3'), 'FailingTask', 3)

    def test_missing_data(self):
        class MissingDataTask(luigi.ExternalTask):
            def complete(self):
                return False

        self.run_and_expect('MissingDataTask', 0)  # Test default value to be 0
        self.run_and_expect('MissingDataTask --retcode-missing-data 5', 5)
        self.run_with_config(dict(missing_data='3'), 'MissingDataTask', 3)

    def test_already_running(self):
        class AlreadyRunningTask(luigi.Task):
            def run(self):
                pass

        old_func = luigi.scheduler.CentralPlannerScheduler.get_work

        def new_func(*args, **kwargs):
            kwargs['current_tasks'] = None
            old_func(*args, **kwargs)
            res = old_func(*args, **kwargs)
            res['running_tasks'][0]['worker'] = "not me :)"  # Otherwise it will be filtered
            return res

        with mock.patch('luigi.scheduler.CentralPlannerScheduler.get_work', new_func):
            self.run_and_expect('AlreadyRunningTask', 0)  # Test default value to be 0
            self.run_and_expect('AlreadyRunningTask --retcode-already-running 5', 5)
            self.run_with_config(dict(already_running='3'), 'AlreadyRunningTask', 3)

    def test_when_locked(self):
        def new_func(*args, **kwargs):
            return False

        with mock.patch('luigi.lock.acquire_for', new_func):
            self.run_and_expect('Task', 0, extra_args=['--local-scheduler'])
            self.run_and_expect('Task --retcode-already-running 5', 5, extra_args=['--local-scheduler'])
            self.run_with_config(dict(already_running='3'), 'Task', 3, extra_args=['--local-scheduler'])

    def test_unhandled_exception(self):
        def new_func(*args, **kwargs):
            raise Exception()

        with mock.patch('luigi.worker.Worker.add', new_func):
            self.run_and_expect('Task', 4)
            self.run_and_expect('Task --retcode-unhandled-exception 2', 2)

        class TaskWithRequiredParam(luigi.Task):
            param = luigi.Parameter()

        self.run_and_expect('TaskWithRequiredParam --param hello', 0)
        self.run_and_expect('TaskWithRequiredParam', 4)

    def test_when_mixed_errors(self):

        class FailingTask(luigi.Task):
            def run(self):
                raise ValueError()

        class MissingDataTask(luigi.ExternalTask):
            def complete(self):
                return False

        class RequiringTask(luigi.Task):
            def requires(self):
                yield FailingTask()
                yield MissingDataTask()

        self.run_and_expect('RequiringTask --retcode-task-failed 4 --retcode-missing-data 5', 5)
        self.run_and_expect('RequiringTask --retcode-task-failed 7 --retcode-missing-data 6', 7)
