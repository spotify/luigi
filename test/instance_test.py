import datetime, os
from spotify import luigi
from spotify.util.test import *

class DummyTask(luigi.Task):
    x = luigi.Parameter()

class FibTest(TestCase):
    def test_local(self):
        dummy_1 = DummyTask(1)
        dummy_2 = DummyTask(2)
        dummy_1b = DummyTask(1)

        self.assertNotEqual(dummy_1, dummy_2)
        self.assertEqual(dummy_1, dummy_1b)

if __name__ == '__main__':
    luigi.run()
