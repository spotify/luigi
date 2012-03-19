import datetime, os, time
from luigi.central_planner import CentralPlannerScheduler
from luigi.worker import Worker
from luigi import *
from luigi.task import InstanceCache
import unittest

class WorkerTest(unittest.TestCase):
    def setUp(self):
        # InstanceCache.disable()
        self.sch = CentralPlannerScheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)
        self.w = Worker(sch=self.sch, pass_exceptions=True, worker_id='X')
        self.w2 = Worker(sch=self.sch, pass_exceptions=True, worker_id='Y')
        self.time = time.time

    def tearDown(self):
        if time.time != self.time:
            time.time = self.time

    def setTime(self, t):
        time.time = lambda: t

    def test_dep(self):
        class A(Task):
            def run(self): self.has_run = True
            def complete(self): return self.has_run
        a = A()

        class B(Task):
            def requires(self): return a
            def run(self): self.has_run = True
        b = B()

        a.has_run = False
        b.has_run = False

        self.w.add(b)
        self.w.run()
        self.assertTrue(a.has_run)
        self.assertTrue(b.has_run)

    def test_external_dep(self):
        class A(ExternalTask): pass
        a = A()

        class B(Task):
            def requires(self): return a
            def run(self): self.has_run = True
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
        a = A()

        class B(Task):
            def requires(self): return a
            def run(self): self.has_run = True
        b = B()

        a.has_run = False
        b.has_run = False

        self.w.add(b)
        self.w.run()

        self.assertTrue(a.has_run)
        self.assertFalse(b.has_run)

    def test_unknown_dep(self):
        # see central_planner_test.CentralPlannerTest.test_remove_dep
        class A(ExternalTask): pass
        class C(Task):
            def complete(self): return True

        def get_b(dep):
            class B(Task):
                def requires(self): return dep
                def run(self): self.has_run = True
            b = B()
            b.has_run = False
            return b

        b_a = get_b(A())
        b_c = get_b(C())

        self.w.add(b_a)
        # So now another worker goes in and schedules C -> B
        # This should remove the dep A -> B but will screw up the first worker
        self.w2.add(b_c)

        self.w.run() # should not run anything - the worker should detect that A is broken
        self.assertFalse(b_a.has_run)

        # not sure what should happen??
        # self.w2.run() # should run B since C is fulfilled
        # self.assertTrue(b_c.has_run)

if __name__ == '__main__':
    unittest.main()
