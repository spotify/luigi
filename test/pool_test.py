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

from server_test import ServerTestBase
from luigi.scheduler import CentralPlannerScheduler
from luigi.worker import Worker


class A(worker_test.DummyTask):
    foo = luigi.IntParameter()

    def requires_resources(self):
        return luigi.Pool("a_pool")


class B(worker_test.DummyTask):
    def requires_resources(self):
        return luigi.Pool("b_pool")


class AB(worker_test.DummyTask):
    def requires_resources(self):
        return [luigi.Pool("a_pool"), luigi.Pool("b_pool")]


def run_workers(w1, w2):
    task_id1, _, _ = w1._get_work()
    task_id2, _, _ = w2._get_work()

    assert task_id1 is not None
    assert task_id2 is None, "w1 did not take lock"

    w1._run_task(task_id1)

    task_id, _, _ = w2._get_work()
    return task_id


class PoolTest(unittest.TestCase):
    def setUp(self):
        self.sch = CentralPlannerScheduler()
        self.w1 = Worker(scheduler=self.sch, worker_id='X')
        self.w2 = Worker(scheduler=self.sch, worker_id='Y', wait_interval=0.1)

    def test_pool(self):
        a1 = A(1)
        a2 = A(2)
        self.w1.add(a1)
        self.w2.add(a2)

        task_id = run_workers(self.w1, self.w2)
        self.assertEquals(task_id, repr(a2))

    def test_multiple_pools(self):
        a = A(3)
        b = B()
        ab = AB()
        self.w1.add(ab)
        self.w2.add(a)
        self.w2.add(b)

        task_id = run_workers(self.w1, self.w2)

        self.assertTrue(task_id == repr(a) or task_id == repr(b), "worker2 should get work for A or B")


class RemotePoolTest(ServerTestBase):
    def _get_sch(self):
        sch = luigi.rpc.RemoteScheduler(host='localhost', port=self._api_port)
        sch._wait = lambda: None
        return sch

    def test_remote_pools(self):
        sch = self._get_sch()
        w1 = Worker(scheduler=sch, worker_id='X')
        w2 = Worker(scheduler=sch, worker_id='Y', wait_interval=0.1)
        w1.add(A(4))
        w2.add(A(5))

        task_id = run_workers(w1, w2)
        self.assertEquals(task_id, repr(A(5)))


