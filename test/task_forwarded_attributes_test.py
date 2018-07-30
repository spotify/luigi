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

from helpers import LuigiTestCase, RunOnceTask

import luigi
import luigi.scheduler
import luigi.worker


forwarded_attributes = set(luigi.worker.TaskProcess.forward_reporter_attributes.values())


class NonYieldingTask(RunOnceTask):

    # need to accept messages in order for the "scheduler_message" attribute to be not None
    accepts_messages = True

    def gather_forwarded_attributes(self):
        attrs = set()
        for attr in forwarded_attributes:
            if getattr(self, attr, None) is not None:
                attrs.add(attr)
        return attrs

    def run(self):
        self.attributes_while_running = self.gather_forwarded_attributes()

        RunOnceTask.run(self)


class YieldingTask(NonYieldingTask):

    def run(self):
        self.attributes_before_yield = self.gather_forwarded_attributes()

        yield RunOnceTask()

        self.attributes_after_yield = self.gather_forwarded_attributes()

        RunOnceTask.run(self)


class TaskForwardedAttributesTest(LuigiTestCase):

    def run_task(self, task):
        sch = luigi.scheduler.Scheduler()
        with luigi.worker.Worker(scheduler=sch) as w:
            w.add(task)
            w.run()
        return task

    def test_non_yielding_task(self):
        task = self.run_task(NonYieldingTask())

        self.assertEqual(task.attributes_while_running, forwarded_attributes)

    def test_yielding_task(self):
        task = self.run_task(YieldingTask())

        self.assertEqual(task.attributes_before_yield, forwarded_attributes)
        self.assertEqual(task.attributes_after_yield, forwarded_attributes)
