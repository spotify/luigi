import time
from luigi.scheduler import CentralPlannerScheduler
import unittest


class CentralPlannerTest(unittest.TestCase):
    def setUp(self):
        self.sch = CentralPlannerScheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)
        self.time = time.time

    def tearDown(self):
        if time.time != self.time:
            time.time = self.time

    def setTime(self, t):
        time.time = lambda: t

    def test_dep(self):
        self.sch.add_task('B')
        self.sch.add_task('A')
        self.sch.add_dep('B', 'A')

        self.assertEqual(self.sch.get_work()[1], 'A')
        self.sch.status('A', 'DONE')
        self.assertEqual(self.sch.get_work()[1], 'B')
        self.sch.status('B', 'DONE')
        self.assertEqual(self.sch.get_work(), (0, None))

    def test_failed_dep(self):
        self.sch.add_task('B')
        self.sch.add_task('A')
        self.sch.add_dep('B', 'A')

        self.assertEqual(self.sch.get_work()[1], 'A')
        self.sch.status('A', 'FAILED')
        self.assertEqual(self.sch.get_work()[1], None)  # can still wait and retry: TODO: do we want this?
        self.sch.status('A', 'DONE')
        self.assertEqual(self.sch.get_work()[1], 'B')
        self.sch.status('B', 'DONE')
        self.assertEqual(self.sch.get_work(), (0, None))

    def test_broken_dep(self):
        self.sch.add_task('B')
        self.sch.add_task('A', status='BROKEN')
        self.sch.add_dep('B', 'A')

        self.assertEqual(self.sch.get_work()[1], None)  # can still wait and retry: TODO: do we want this?
        self.sch.status('A', 'DONE')
        self.assertEqual(self.sch.get_work()[1], 'B')
        self.sch.status('B', 'DONE')
        self.assertEqual(self.sch.get_work(), (0, None))

    def test_two_workers(self):
        # Worker X wants to build A -> B
        # Worker Y wants to build A -> C
        self.sch.add_task('A', worker='X')
        self.sch.add_task('A', worker='Y')
        self.sch.add_task('B', worker='X')
        self.sch.add_task('C', worker='Y')
        self.sch.add_dep('B', 'A')
        self.sch.add_dep('C', 'A')

        self.assertEqual(self.sch.get_work(worker='X')[1], 'A')
        self.assertEqual(self.sch.get_work(worker='Y')[1], None)  # Worker Y is pending on A to be done
        self.sch.status('A', 'DONE')
        self.assertEqual(self.sch.get_work(worker='Y')[1], 'C')
        self.assertEqual(self.sch.get_work(worker='X')[1], 'B')

    def test_retry(self):
        # Try to build A but fails, will retry after 100s
        self.setTime(0)
        self.sch.add_task('A')
        self.assertEqual(self.sch.get_work()[1], 'A')
        self.sch.status('A', 'FAILED')
        for t in xrange(100):
            self.setTime(t)
            self.assertEqual(self.sch.get_work()[1], None)
            self.sch.ping()

        self.setTime(101)
        self.assertEqual(self.sch.get_work()[1], 'A')

    def test_disconnect_running(self):
        # X and Y wants to run A.
        # X starts but does not report back. Y does.
        # After some timeout, Y will build it instead
        self.setTime(0)
        self.sch.add_task('A', worker='X')
        self.sch.add_task('A', worker='Y')
        self.assertEqual(self.sch.get_work(worker='X')[1], 'A')
        for t in xrange(200):
            self.setTime(t)
            self.sch.ping(worker='Y')

        self.assertEqual(self.sch.get_work(worker='Y')[1], 'A')

    def test_remove_dep(self):
        # X schedules A -> B, A is broken
        # Y schedules C -> B: this should remove A as a dep of B
        self.sch.add_task('A', worker='X', status='BROKEN')
        self.sch.add_task('B', worker='X')
        self.sch.add_dep('B', 'A')

        # X can't build anything
        self.assertEqual(self.sch.get_work(worker='X')[1], None)

        self.sch.add_task('B', worker='Y')  # should reset dependencies for A
        self.sch.add_task('C', worker='Y', status='DONE')
        self.sch.add_dep('B', 'C')

        self.assertEqual(self.sch.get_work(worker='Y')[1], 'B')

    def test_timeout(self):
        # A bug that was earlier present when restarting the same flow
        self.setTime(0)
        self.sch.add_task('A', worker='X')
        self.assertEqual(self.sch.get_work(worker='X')[1], 'A')
        self.setTime(10000)
        self.sch.add_task('A', worker='Y')  # Will timeout X but not schedule A for removal
        for i in xrange(2000):
            self.setTime(10000 + i)
            self.sch.ping(worker='Y')
        self.sch.status('A', 'DONE', worker='Y')  # This used to raise an exception since A was removed

    def test_disallowed_state_changes(self):
        # Test that we can not schedule an already running task
        t = 'A'
        self.sch.add_task(t, worker='X')
        self.assertEqual(self.sch.get_work(worker='X')[1], t)
        self.sch.add_task(t, worker='Y')
        self.assertEqual(self.sch.get_work(worker='Y')[1], None)

if __name__ == '__main__':
    unittest.main()
