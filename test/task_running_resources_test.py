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

import os
import time
import signal
import multiprocessing
from contextlib import contextmanager

from helpers import unittest, RunOnceTask, with_config

import luigi
import luigi.server


class ResourceTestTask(RunOnceTask):

    param = luigi.Parameter()
    reduce_foo = luigi.BoolParameter()

    def process_resources(self):
        return {"foo": 2}

    def run(self):
        if self.reduce_foo:
            self.decrease_running_resources({"foo": 1})

        time.sleep(2)

        super(ResourceTestTask, self).run()


class ResourceWrapperTask(RunOnceTask):

    reduce_foo = ResourceTestTask.reduce_foo

    def requires(self):
        return [
            ResourceTestTask(param="a", reduce_foo=self.reduce_foo),
            ResourceTestTask(param="b"),
        ]


class LocalRunningResourcesTest(unittest.TestCase):

    def test_resource_reduction(self):
        # trivial resource reduction on local scheduler
        # test the running_task_resources setter and getter
        sch = luigi.scheduler.Scheduler(resources={"foo": 2})

        with luigi.worker.Worker(scheduler=sch) as w:
            task = ResourceTestTask(param="a", reduce_foo=True)

            w.add(task)
            w.run()

            self.assertEqual(sch.get_running_task_resources(task.task_id)["resources"]["foo"], 1)


class ConcurrentRunningResourcesTest(unittest.TestCase):

    @with_config({'scheduler': {'stable_done_cooldown_secs': '0'}})
    def setUp(self):
        super(ConcurrentRunningResourcesTest, self).setUp()

        # run the luigi server in a new process and wait for its startup
        self._process = multiprocessing.Process(target=luigi.server.run)
        self._process.start()
        time.sleep(0.5)

        # configure the rpc scheduler, update the foo resource
        self.sch = luigi.rpc.RemoteScheduler()
        self.sch.update_resource("foo", 3)

    def tearDown(self):
        super(ConcurrentRunningResourcesTest, self).tearDown()

        # graceful server shutdown
        self._process.terminate()
        self._process.join(timeout=1)
        if self._process.is_alive():
            os.kill(self._process.pid, signal.SIGKILL)

    @contextmanager
    def worker(self, scheduler=None, processes=2):
        with luigi.worker.Worker(scheduler=scheduler or self.sch, worker_processes=processes) as w:
            w._config.wait_interval = 0.2
            w._config.check_unfulfilled_deps = False
            yield w

    @contextmanager
    def assert_duration(self, min_duration=0, max_duration=-1):
        t0 = time.time()
        try:
            yield
        finally:
            duration = time.time() - t0
            self.assertGreater(duration, min_duration)
            if max_duration > 0:
                self.assertLess(duration, max_duration)

    def test_tasks_serial(self):
        # serial test
        # run two tasks that do not reduce the "foo" resource
        # as the total foo resource (3) is smaller than the requirement of two tasks (4),
        # the scheduler is forced to run them serially which takes longer than 4 seconds
        with self.worker() as w:
            w.add(ResourceWrapperTask(reduce_foo=False))

            with self.assert_duration(min_duration=4):
                w.run()

    def test_tasks_parallel(self):
        # parallel test
        # run two tasks and the first one lowers its requirement on the "foo" resource, so that
        # the total "foo" resource (3) is sufficient to run both tasks in parallel shortly after
        # the first task started, so the entire process should not exceed 4 seconds
        with self.worker() as w:
            w.add(ResourceWrapperTask(reduce_foo=True))

            with self.assert_duration(max_duration=4):
                w.run()
