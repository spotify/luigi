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

import logging
import unittest

import luigi.notifications
import luigi.worker
from luigi import Parameter, RemoteScheduler, Task
from luigi.worker import Worker
from mock import Mock

luigi.notifications.DEBUG = True


class DummyTask(Task):
    param = Parameter()

    def __init__(self, *args, **kwargs):
        super(DummyTask, self).__init__(*args, **kwargs)
        self.has_run = False

    def complete(self):
        old_value = self.has_run
        self.has_run = True
        return old_value

    def run(self):
        logging.debug("%s - setting has_run", self.task_id)
        self.has_run = True


class MultiprocessWorkerTest(unittest.TestCase):

    def setUp(self):
        self.scheduler = RemoteScheduler()
        self.scheduler.add_worker = Mock()
        self.scheduler.add_task = Mock()
        self.worker = Worker(scheduler=self.scheduler, worker_id='X', worker_processes=2)

    def tearDown(self):
        self.worker.stop()

    def test_positive_path(self):
        a = DummyTask("a")
        b = DummyTask("b")

        class MultipleRequirementTask(DummyTask):

            def requires(self):
                return [a, b]

        c = MultipleRequirementTask("C")

        self.assertTrue(self.worker.add(c))

        self.scheduler.get_work = Mock(side_effect=[(3, str(a)), (2, str(b)), (1, str(c)), (0, None), (0, None)])

        self.assertTrue(self.worker.run())
        self.assertTrue(c.has_run)

    def test_path_with_task_failures(self):
        class FailingTask(DummyTask):

            def run(self):
                raise Exception("I am failing")

        a = FailingTask("a")
        b = FailingTask("b")

        class MultipleRequirementTask(DummyTask):

            def requires(self):
                return [a, b]

        c = MultipleRequirementTask("C")

        self.assertTrue(self.worker.add(c))

        self.scheduler.get_work = Mock(side_effect=[(3, str(a)), (2, str(b)), (1, str(c)), (0, None), (0, None)])

        self.assertFalse(self.worker.run())


if __name__ == '__main__':
    unittest.main()
