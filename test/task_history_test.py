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
import luigi.scheduler
import luigi.task_history
import luigi.worker

luigi.notifications.DEBUG = True


class SimpleTaskHistory(luigi.task_history.TaskHistory):

    def __init__(self):
        self.actions = []

    def task_scheduled(self, task_id):
        self.actions.append(('scheduled', task_id))

    def task_finished(self, task_id, successful):
        self.actions.append(('finished', task_id))

    def task_started(self, task_id, worker_host):
        self.actions.append(('started', task_id))


class TaskHistoryTest(unittest.TestCase):

    def setUp(self):
        self.th = SimpleTaskHistory()
        self.sch = luigi.scheduler.CentralPlannerScheduler(task_history_impl=self.th)
        self.w = luigi.worker.Worker(scheduler=self.sch)

    def tearDown(self):
        self.w.stop()

    def test_run(self):
        class MyTask(luigi.Task):
            pass

        self.w.add(MyTask())
        self.w.run()

        self.assertEqual(self.th.actions, [
            ('scheduled', 'MyTask()'),
            ('started', 'MyTask()'),
            ('finished', 'MyTask()')
        ])


if __name__ == '__main__':
    unittest.main()
