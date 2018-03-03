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

import time
from contextlib import contextmanager

from helpers import RunOnceTask

import luigi
import luigi.scheduler
import luigi.worker
import luigi.rpc
import server_test


luigi.notifications.DEBUG = True


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


class WorkerSchedulerFactory(luigi.interface._WorkerSchedulerFactory):

    def create_remote_scheduler(self, *args, **kwargs):
        # patch to set the foo resource
        sch = super(WorkerSchedulerFactory, self).create_remote_scheduler(*args, **kwargs)
        sch.update_resource("foo", 3)
        return sch

    def create_worker(self, *args, **kwargs):
        # patch to overwrite the wait interval
        w = super(WorkerSchedulerFactory, self).create_worker(*args, **kwargs)
        w._config.wait_interval = 0.2
        return w


class TaskRunningResourcesTest(server_test.ServerTestBase):

    factory = WorkerSchedulerFactory()

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

    def test_remote_scheduler_serial(self):
        # serial test
        # run two tasks that do not reduce the "foo" resource
        # as the total foo resource (3) is smaller than the requirement of two tasks (4),
        # the scheduler is forced to run them serially which takes longer than 4 seconds
        task_a = ResourceTestTask(param="a")
        task_b = ResourceTestTask(param="b")

        with self.assert_duration(min_duration=4):
            luigi.build([task_a, task_b], self.factory, workers=2,
                        scheduler_port=self.get_http_port())

    def test_remote_scheduler_parallel(self):
        # parallel test
        # run two tasks and the first one lowers its requirement on the "foo" resource, so that
        # the total "foo" resource (3) is sufficient to run both tasks in parallel shortly after
        # the first task started, so the entire process should not exceed 4 seconds
        task_c = ResourceTestTask(param="c", reduce_foo=True)
        task_d = ResourceTestTask(param="d")

        with self.assert_duration(max_duration=4):
            luigi.build([task_c, task_d], self.factory, workers=2,
                        scheduler_port=self.get_http_port())

    def test_local_scheduler(self):
        # trivial local scheduler test
        # as each worker process would communicate with its own scheduler, concurrency tests with
        # multiple processes like the ones above don't make sense here, but one can at least test
        # the running_task_resources setter and getter
        sch = luigi.scheduler.Scheduler(resources={"foo": 2})

        with luigi.worker.Worker(scheduler=sch) as w:

            task = ResourceTestTask(param="x", reduce_foo=True)

            w.add(task)
            w.run()

            self.assertEqual(sch.get_running_task_resources(task.task_id)["resources"]["foo"], 1)
