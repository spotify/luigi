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
import luigi.worker

luigi.notifications.DEBUG = True


class TaskStatusMessageTest(LuigiTestCase):

    def test_run(self):
        message = "test message"
        sch = luigi.scheduler.Scheduler()
        with luigi.worker.Worker(scheduler=sch) as w:
            class MyTask(luigi.Task):
                def run(self):
                    self.set_status_message(message)

            task = MyTask()
            w.add(task)
            w.run()

            self.assertEqual(sch.get_task_status_message(task.task_id)["statusMessage"], message)
