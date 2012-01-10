import datetime, os, time
from luigi.central_planner import CentralPlannerScheduler
from luigi.worker import Worker
from luigi import *
from luigi.task import InstanceCache
import unittest

class WorkerTest(unittest.TestCase):
    def setUp(self):
        InstanceCache.clear()
        self.sch = CentralPlannerScheduler(retry_delay=100, remove_delay=1000, client_disconnect_delay=10)
        self.w = Worker(sch=self.sch, pass_exceptions=False)
        self.time = time.time

    def tearDown(self):
        if time.time != self.time:
            time.time = self.time

    def setTime(self, t):
        time.time = lambda: t

    def test_dep(self):
        class A(Task):
            def run(self): self.has_run = True
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

if __name__ == '__main__':
    unittest.main()
