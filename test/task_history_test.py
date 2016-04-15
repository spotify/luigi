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

from luigi.task_status import PENDING, DONE, RUNNING

luigi.notifications.DEBUG = True


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

            self.check_event_for(sch, task.task_id, PENDING)
            self.check_event_for(sch, task.task_id, RUNNING)
            self.check_event_for(sch, task.task_id, DONE)

    def check_event_for(self, sch, task_id, status):
        status_update = sch._history_queue.get()
        self.assertEqual(status_update.task.id, task_id)
        self.assertEqual(status_update.status, status)
