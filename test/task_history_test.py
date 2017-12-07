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

from helpers import LuigiTestCase

import luigi
import luigi.scheduler
import luigi.task_history
import luigi.worker

luigi.notifications.DEBUG = True


class SimpleTaskHistory(luigi.task_history.TaskHistory):

    def __init__(self):
        self.actions = []

    def task_scheduled(self, task):
        self.actions.append(('scheduled', task.id))

    def task_finished(self, task, successful):
        self.actions.append(('finished', task.id))

    def task_started(self, task, worker_host):
        self.actions.append(('started', task.id))


class TaskHistoryTest(LuigiTestCase):

    def test_run(self):
        th = SimpleTaskHistory()
        sch = luigi.scheduler.Scheduler(task_history_impl=th)
        with luigi.worker.Worker(scheduler=sch) as w:
            class MyTask(luigi.Task):
                pass

            task = MyTask()
            w.add(task)
            w.run()

            self.assertEqual(th.actions, [
                ('scheduled', task.task_id),
                ('started', task.task_id),
                ('finished', task.task_id)
            ])
