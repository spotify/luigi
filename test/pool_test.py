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

import unittest
import luigi
import worker_test

from luigi.scheduler import CentralPlannerScheduler
from luigi.worker import Worker


class A(worker_test.DummyTask):
    pool = luigi.Pool("a_pool")
    foo = luigi.IntParameter()


class B(worker_test.DummyTask):
    pool = luigi.Pool("b_pool")


class AB(worker_test.DummyTask):
    a_pool = luigi.Pool("a_pool")
    b_pool = luigi.Pool("b_pool")


class PoolTest(unittest.TestCase):
    def setUp(self):
        self.sch = CentralPlannerScheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)
        self.w1 = Worker(scheduler=self.sch, worker_id='X')
        self.w2 = Worker(scheduler=self.sch, worker_id='Y', wait_interval=0.1)

    def test_pool(self):
        a1 = A(foo=1)
        a2 = A(foo=2)
        self.w1.add(a1)
        self.w2.add(a2)

        task_id1, _, _ = self.w1._get_work()  # a1 takes lock
        task_id2, _, _ = self.w2._get_work()

        self.assertEquals(task_id2, None)
        self.w1._run_task(task_id1)

        task_id, _, _ = self.w2._get_work()
        self.assertEquals(task_id, repr(a2))

    def test_multiple_pools(self):
        a = A(1)
        b = B()
        ab = AB()
        self.w1.add(ab)
        self.w2.add(a)
        self.w2.add(b)

        task_id1, _, _ = self.w1._get_work()
        task_id2, _, _ = self.w2._get_work()

        self.assertEquals(task_id2, None)
        self.w1._run_task(task_id1)

        task_id, _, _ = self.w2._get_work()
        self.assertTrue(task_id == repr(a) or task_id == repr(b), "worker2 should get work for A or B")
