import datetime, os, time
from luigi.central_planner import CentralPlannerScheduler
import unittest

class CentralPlannerTest(unittest.TestCase):
    def setUp(self):
        self.sch = CentralPlannerScheduler(retry_delay=100, remove_delay=1000, client_disconnect_delay=10)
        self.time = time.time

    def tearDown(self):
        if time.time != self.time:
            time.time = self.time

    def setTime(self, t):
        print 'setting time to', t
        time.time = lambda: t

    def test_dep(self):
        self.sch.add_task('B')
        self.sch.add_task('A')
        self.sch.add_dep('B', 'A')

        self.assertEqual(self.sch.get_work(), (False, 'A'))
        self.sch.status('A', 'DONE')
        self.assertEqual(self.sch.get_work(), (False, 'B'))
        self.sch.status('B', 'DONE')
        self.assertEqual(self.sch.get_work(), (True, None))

    def test_two_clients(self):
        # Client X wants to build A -> B
        # Client Y wants to build A -> C
        self.sch.add_task('A', client='X')
        self.sch.add_task('A', client='Y')
        self.sch.add_task('B', client='X')
        self.sch.add_task('C', client='Y')
        self.sch.add_dep('B', 'A')
        self.sch.add_dep('C', 'A')

        self.assertEqual(self.sch.get_work(client='X'), (False, 'A'))
        self.assertEqual(self.sch.get_work(client='Y'), (False, None)) # Client Y is pending on A to be done
        self.sch.status('A', 'DONE')
        self.assertEqual(self.sch.get_work(client='Y'), (False, 'C'))
        self.assertEqual(self.sch.get_work(client='X'), (False, 'B'))

    def test_retry(self):
        # Try to build A but fails, will retry after 100s
        self.setTime(0)
        self.sch.add_task('A')
        self.assertEqual(self.sch.get_work(), (False, 'A'))
        self.sch.status('A', 'FAILED')
        for t in xrange(100):
            self.setTime(t)
            self.assertEqual(self.sch.get_work(), (True, None))
            self.sch.ping()

        self.setTime(101)
        self.assertEqual(self.sch.get_work(), (False, 'A'))

    def test_disconnect_running(self):
        # X and Y wants to run A.
        # X starts but does not report back. Y does.
        # After some timeout, Y will build it instead
        self.setTime(0)
        self.sch.add_task('A', client='X')
        self.sch.add_task('A', client='Y')
        self.assertEqual(self.sch.get_work(client='X'), (False, 'A'))
        for t in xrange(200):
            self.setTime(t)
            self.sch.ping(client='Y')

        self.assertEqual(self.sch.get_work(client='Y'), (False, 'A'))
