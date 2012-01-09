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
        self.w = Worker(scheduler=self.sch)
        self.time = time.time

    def tearDown(self):
        if time.time != self.time:
            time.time = self.time

    def setTime(self, t):
        time.time = lambda: t

    def test_dep(self):
        class A(Task):
            def run(self): self.has_run = True

        class B(Task):
            def requires(self): return A()
            def run(self): self.has_run = True

        self.w.add(B())
        self.w.run()
        self.assertTrue(A().has_run)
        self.assertTrue(B().has_run)

    def test_external_dep(self):
        class A(ExternalTask):
            pass

        class B(Task):
            def requires(self): return A()
            def run(self): self.has_run = True

        A().has_run = False
        B().has_run = False

        self.w.add(B())
        self.w.run()

        self.assertFalse(A().has_run)
        self.assertFalse(B().has_run)

if __name__ == '__main__':
    unittest.main()
