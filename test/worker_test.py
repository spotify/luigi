# Copyright (c) 2012 Spotify AB
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

import time
from luigi.scheduler import CentralPlannerScheduler
from luigi.worker import Worker
from luigi import *
import unittest
import logging


class DummyTask(Task):
    def __init__(self, *args, **kwargs):
        super(DummyTask, self).__init__(*args, **kwargs)
        self.has_run = False

    def complete(self):
        return self.has_run

    def run(self):
        logging.debug("%s - setting has_run" % self.task_id)
        self.has_run = True


class WorkerTest(unittest.TestCase):
    def setUp(self):
        # InstanceCache.disable()
        self.sch = CentralPlannerScheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)
        self.w = Worker(scheduler=self.sch, worker_id='X')
        self.w2 = Worker(scheduler=self.sch, worker_id='Y')
        self.time = time.time

    def tearDown(self):
        if time.time != self.time:
            time.time = self.time

    def setTime(self, t):
        time.time = lambda: t

    def test_dep(self):
        class A(Task):
            def run(self):
                self.has_run = True

            def complete(self):
                return self.has_run
        a = A()

        class B(Task):
            def requires(self):
                return a

            def run(self):
                self.has_run = True

            def complete(self):
                return self.has_run

        b = B()
        a.has_run = False
        b.has_run = False

        self.w.add(b)
        self.w.run()
        self.assertTrue(a.has_run)
        self.assertTrue(b.has_run)

    def test_external_dep(self):
        class A(ExternalTask):
            def complete(self):
                return False
        a = A()

        class B(Task):
            def requires(self):
                return a

            def run(self):
                self.has_run = True

            def complete(self):
                return self.has_run

        b = B()

        a.has_run = False
        b.has_run = False

        self.w.add(b)
        self.w.run()

        self.assertFalse(a.has_run)
        self.assertFalse(b.has_run)

    def test_fail(self):
        class A(Task):
            def run(self):
                self.has_run = True
                raise Exception()

            def complete(self):
                return self.has_run

        a = A()

        class B(Task):
            def requires(self):
                return a

            def run(self):
                self.has_run = True

            def complete(self):
                return self.has_run

        b = B()

        a.has_run = False
        b.has_run = False

        self.w.add(b)
        self.w.run()

        self.assertTrue(a.has_run)
        self.assertFalse(b.has_run)

    def test_unknown_dep(self):
        # see central_planner_test.CentralPlannerTest.test_remove_dep
        class A(ExternalTask):
            def complete(self):
                return False

        class C(Task):
            def complete(self):
                return True

        def get_b(dep):
            class B(Task):
                def requires(self):
                    return dep

                def run(self):
                    self.has_run = True

                def complete(self):
                    return False

            b = B()
            b.has_run = False
            return b

        b_a = get_b(A())
        b_c = get_b(C())

        self.w.add(b_a)
        # So now another worker goes in and schedules C -> B
        # This should remove the dep A -> B but will screw up the first worker
        self.w2.add(b_c)

        self.w.run()  # should not run anything - the worker should detect that A is broken
        self.assertFalse(b_a.has_run)
        # not sure what should happen??
        # self.w2.run() # should run B since C is fulfilled
        # self.assertTrue(b_c.has_run)

    def test_interleaved_workers(self):
        class A(DummyTask):
            pass

        a = A()

        class B(DummyTask):
            def requires(self):
                return a

        class ExternalB(ExternalTask):
            task_family = "B"

            def complete(self):
                return False

        b = B()
        eb = ExternalB()
        self.assertEquals(eb.task_id, "B()")

        sch = CentralPlannerScheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)
        w = Worker(scheduler=sch, worker_id='X')
        w2 = Worker(scheduler=sch, worker_id='Y')

        w.add(b)
        w2.add(eb)
        logging.debug("RUNNING BROKEN WORKER")
        w2.run()
        self.assertFalse(a.complete())
        self.assertFalse(b.complete())
        logging.debug("RUNNING FUNCTIONAL WORKER")
        w.run()
        self.assertTrue(a.complete())
        self.assertTrue(b.complete())

    def test_interleaved_workers2(self):
        # two tasks without dependencies, one external, one not
        class B(DummyTask):
            pass

        class ExternalB(ExternalTask):
            task_family = "B"

            def complete(self):
                return False

        b = B()
        eb = ExternalB()

        self.assertEquals(eb.task_id, "B()")

        sch = CentralPlannerScheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)
        w = Worker(scheduler=sch, worker_id='X')
        w2 = Worker(scheduler=sch, worker_id='Y')

        w2.add(eb)
        w.add(b)

        w2.run()
        self.assertFalse(b.complete())
        w.run()
        self.assertTrue(b.complete())

if __name__ == '__main__':
    unittest.main()
