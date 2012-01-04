import datetime, os
import luigi
from spotify.util.test import *

class InstanceTest(TestCase):
    def test_simple(self):
        class DummyTask(luigi.Task):
            x = luigi.Parameter()

        dummy_1 = DummyTask(1)
        dummy_2 = DummyTask(2)
        dummy_1b = DummyTask(1)

        self.assertNotEqual(dummy_1, dummy_2)
        self.assertEqual(dummy_1, dummy_1b)

    def test_dep(self):
        test = self

        class A(luigi.Task):
            def __init__(self):
                self.has_run = False
                super(A, self).__init__()

            def run(self):
                self.has_run = True

        class B(luigi.Task):
            x = luigi.Parameter()
            def requires(self): return A() # This will end up referring to the same object
            def run(self): test.assertTrue(self.input().has_run)
        
        w = luigi.scheduler.Worker(luigi.scheduler.LocalScheduler())
        w.add(B(1))
        w.add(B(2))
        

if __name__ == '__main__':
    luigi.run()
