# Copyright (c) 2013 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import luigi
import luigi.scheduler
import luigi.worker
import luigi.hadoop
import unittest
import time
import logging


class DummyTask(luigi.Task):
    n = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super(DummyTask, self).__init__(*args, **kwargs)
        self.has_run = False

    def complete(self):
        return self.has_run

    def run(self):
        logging.debug("%s - setting has_run" % self.task_id)
        self.has_run = True


class CustomizedScheduler(luigi.scheduler.CentralPlannerScheduler):
    def __init__(self, *args, **kwargs):
        super(CustomizedScheduler, self).__init__(*args, **kwargs)
        self.has_run = False

    def get_work(self, worker, host=None):
        locally_pending_tasks, best_task = super(CustomizedScheduler, self).get_work(worker, host)
        self.has_run = True
        return locally_pending_tasks, best_task

    def complete(self):
        return self.has_run


class CustomizedWorker(luigi.worker.Worker):
    def __init__(self, *args, **kwargs):
        super(CustomizedWorker, self).__init__(*args, **kwargs)
        self.has_run = False

    def _run_task(self, task_id):
        super(CustomizedWorker, self)._run_task(task_id)
        self.has_run = True

    def complete(self):
        return self.has_run


class CustomizedSchedulerTest(unittest.TestCase):
    ''' Test that luigi's build method (and ultimately the run method) can accept a customized scheduler '''
    def setUp(self):
        self.scheduler = CustomizedScheduler()
        self.time = time.time

    def tearDown(self):
        if time.time != self.time:
            time.time = self.time

    def setTime(self, t):
        time.time = lambda: t

    def test_customized_scheduler(self):
        a = DummyTask(1)
        self.assertFalse(self.scheduler.complete())
        luigi.build([a], scheduler_instance=self.scheduler)
        self.assertTrue(a.complete())
        self.assertTrue(self.scheduler.complete())

    def test_cmdline_custom_scheduler(self):
        self.assertFalse(self.scheduler.complete())
        luigi.run(['DummyTask', '--n', '2'], scheduler_instance=self.scheduler)
        self.assertTrue(self.scheduler.complete())


class CustomizedWorkerTest(unittest.TestCase):
    ''' Test that luigi's build method (and ultimately the run method) can accept a customized worker '''
    def setUp(self):
        self.scheduler = luigi.scheduler.CentralPlannerScheduler()
        self.worker = CustomizedWorker(scheduler=self.scheduler)
        self.time = time.time

    def tearDown(self):
        if time.time != self.time:
            time.time = self.time

    def setTime(self, t):
        time.time = lambda: t

    def test_customized_worker(self):
        a = DummyTask(3)
        self.assertFalse(a.complete())
        self.assertFalse(self.worker.complete())
        luigi.build([a], scheduler_instance=self.scheduler, worker_instance=self.worker)
        self.assertTrue(a.complete())
        self.assertTrue(self.worker.complete())

    def test_cmdline_custom_worker(self):
        self.assertFalse(self.worker.complete())
        luigi.run(['DummyTask', '--n', '4'], scheduler_instance=self.scheduler, worker_instance=self.worker)
        self.assertTrue(self.worker.complete())

if __name__ == '__main__':
    unittest.main()
