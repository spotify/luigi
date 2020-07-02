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
import time
from helpers import unittest

import luigi
import luigi.contrib.hadoop
import luigi.rpc
import luigi.scheduler
import luigi.worker


class DummyTask(luigi.Task):
    n = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super(DummyTask, self).__init__(*args, **kwargs)
        self.has_run = False

    def complete(self):
        return self.has_run

    def run(self):
        logging.debug("%s - setting has_run", self)
        self.has_run = True


class CustomizedLocalScheduler(luigi.scheduler.Scheduler):

    def __init__(self, *args, **kwargs):
        super(CustomizedLocalScheduler, self).__init__(*args, **kwargs)
        self.has_run = False

    def get_work(self, worker, host=None, **kwargs):
        r = super(CustomizedLocalScheduler, self).get_work(worker=worker, host=host)
        self.has_run = True
        return r

    def complete(self):
        return self.has_run


class CustomizedRemoteScheduler(luigi.rpc.RemoteScheduler):

    def __init__(self, *args, **kwargs):
        super(CustomizedRemoteScheduler, self).__init__(*args, **kwargs)
        self.has_run = False

    def get_work(self, worker, host=None):
        r = super(CustomizedRemoteScheduler, self).get_work(worker=worker, host=host)
        self.has_run = True
        return r

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


class CustomizedWorkerSchedulerFactory:

    def __init__(self, *args, **kwargs):
        self.scheduler = CustomizedLocalScheduler()
        self.worker = CustomizedWorker(self.scheduler)

    def create_local_scheduler(self):
        return self.scheduler

    def create_remote_scheduler(self, url):
        return CustomizedRemoteScheduler(url)

    def create_worker(self, scheduler, worker_processes=None, assistant=False):
        return self.worker


class CustomizedWorkerTest(unittest.TestCase):

    ''' Test that luigi's build method (and ultimately the run method) can accept a customized worker and scheduler '''

    def setUp(self):
        self.worker_scheduler_factory = CustomizedWorkerSchedulerFactory()
        self.time = time.time

    def tearDown(self):
        if time.time != self.time:
            time.time = self.time

    def setTime(self, t):
        time.time = lambda: t

    def test_customized_worker(self):
        a = DummyTask(3)
        self.assertFalse(a.complete())
        self.assertFalse(self.worker_scheduler_factory.worker.complete())
        luigi.build([a], worker_scheduler_factory=self.worker_scheduler_factory)
        self.assertTrue(a.complete())
        self.assertTrue(self.worker_scheduler_factory.worker.complete())

    def test_cmdline_custom_worker(self):
        self.assertFalse(self.worker_scheduler_factory.worker.complete())
        luigi.run(['DummyTask', '--n', '4'], worker_scheduler_factory=self.worker_scheduler_factory)
        self.assertTrue(self.worker_scheduler_factory.worker.complete())
