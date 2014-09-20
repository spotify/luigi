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
from luigi.scheduler import CentralPlannerScheduler, DONE, FAILED
import unittest
import luigi.notifications
luigi.notifications.DEBUG = True
WORKER = 'myworker'


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
        self.sch.add_task(WORKER, 'B', deps=('A',))
        self.sch.add_task(WORKER, 'A')
        self.assertEqual(self.sch.get_work(WORKER)['task_id'], 'A')
        self.sch.add_task(WORKER, 'A', status=DONE)
        self.assertEqual(self.sch.get_work(WORKER)['task_id'], 'B')
        self.sch.add_task(WORKER, 'B', status=DONE)
        self.assertEqual(self.sch.get_work(WORKER)['task_id'], None)

    def test_failed_dep(self):
        self.sch.add_task(WORKER, 'B', deps=('A',))
        self.sch.add_task(WORKER, 'A')

        self.assertEqual(self.sch.get_work(WORKER)['task_id'], 'A')
        self.sch.add_task(WORKER, 'A', status=FAILED)

        self.assertEqual(self.sch.get_work(WORKER)['task_id'], None)  # can still wait and retry: TODO: do we want this?
        self.sch.add_task(WORKER, 'A', DONE)
        self.assertEqual(self.sch.get_work(WORKER)['task_id'], 'B')
        self.sch.add_task(WORKER, 'B', DONE)
        self.assertEqual(self.sch.get_work(WORKER)['task_id'], None)

    def test_broken_dep(self):
        self.sch.add_task(WORKER, 'B', deps=('A',))
        self.sch.add_task(WORKER, 'A', runnable=False)

        self.assertEqual(self.sch.get_work(WORKER)['task_id'], None)  # can still wait and retry: TODO: do we want this?
        self.sch.add_task(WORKER, 'A', DONE)
        self.assertEqual(self.sch.get_work(WORKER)['task_id'], 'B')
        self.sch.add_task(WORKER, 'B', DONE)
        self.assertEqual(self.sch.get_work(WORKER)['task_id'], None)

    def test_two_workers(self):
        # Worker X wants to build A -> B
        # Worker Y wants to build A -> C
        self.sch.add_task(worker='X', task_id='A')
        self.sch.add_task(worker='Y', task_id='A')
        self.sch.add_task(task_id='B', deps=('A',), worker='X')
        self.sch.add_task(task_id='C', deps=('A',), worker='Y')

        self.assertEqual(self.sch.get_work(worker='X')['task_id'], 'A')
        self.assertEqual(self.sch.get_work(worker='Y')['task_id'], None)  # Worker Y is pending on A to be done
        self.sch.add_task(worker='X', task_id='A', status=DONE)
        self.assertEqual(self.sch.get_work(worker='Y')['task_id'], 'C')
        self.assertEqual(self.sch.get_work(worker='X')['task_id'], 'B')

    def test_retry(self):
        # Try to build A but fails, will retry after 100s
        self.setTime(0)
        self.sch.add_task(WORKER, 'A')
        self.assertEqual(self.sch.get_work(WORKER)['task_id'], 'A')
        self.sch.add_task(WORKER, 'A', FAILED)
        for t in xrange(100):
            self.setTime(t)
            self.assertEqual(self.sch.get_work(WORKER)['task_id'], None)
            self.sch.ping(WORKER)
            if t % 10 == 0:
                self.sch.prune()

        self.setTime(101)
        self.sch.prune()
        self.assertEqual(self.sch.get_work(WORKER)['task_id'], 'A')

    def test_disconnect_running(self):
        # X and Y wants to run A.
        # X starts but does not report back. Y does.
        # After some timeout, Y will build it instead
        self.setTime(0)
        self.sch.add_task(task_id='A', worker='X')
        self.sch.add_task(task_id='A', worker='Y')
        self.assertEqual(self.sch.get_work(worker='X')['task_id'], 'A')
        for t in xrange(200):
            self.setTime(t)
            self.sch.ping(worker='Y')
            if t % 10 == 0:
                self.sch.prune()

        self.assertEqual(self.sch.get_work(worker='Y')['task_id'], 'A')

    def test_remove_dep(self):
        # X schedules A -> B, A is broken
        # Y schedules C -> B: this should remove A as a dep of B
        self.sch.add_task(task_id='A', worker='X', runnable=False)
        self.sch.add_task(task_id='B', deps=('A',), worker='X')

        # X can't build anything
        self.assertEqual(self.sch.get_work(worker='X')['task_id'], None)

        self.sch.add_task(task_id='B', deps=('C',), worker='Y')  # should reset dependencies for A
        self.sch.add_task(task_id='C', worker='Y', status=DONE)

        self.assertEqual(self.sch.get_work(worker='Y')['task_id'], 'B')

    def test_timeout(self):
        # A bug that was earlier present when restarting the same flow
        self.setTime(0)
        self.sch.add_task(task_id='A', worker='X')
        self.assertEqual(self.sch.get_work(worker='X')['task_id'], 'A')
        self.setTime(10000)
        self.sch.add_task(task_id='A', worker='Y')  # Will timeout X but not schedule A for removal
        for i in xrange(2000):
            self.setTime(10000 + i)
            self.sch.ping(worker='Y')
        self.sch.add_task(task_id='A', status=DONE, worker='Y')  # This used to raise an exception since A was removed

    def test_disallowed_state_changes(self):
        # Test that we can not schedule an already running task
        t = 'A'
        self.sch.add_task(task_id=t, worker='X')
        self.assertEqual(self.sch.get_work(worker='X')['task_id'], t)
        self.sch.add_task(task_id=t, worker='Y')
        self.assertEqual(self.sch.get_work(worker='Y')['task_id'], None)

    def test_two_worker_info(self):
        # Make sure the scheduler returns info that some other worker is running task A
        self.sch.add_task(worker='X', task_id='A')
        self.sch.add_task(worker='Y', task_id='A')

        self.assertEqual(self.sch.get_work(worker='X')['task_id'], 'A')
        r = self.sch.get_work(worker='Y')
        self.assertEqual(r['task_id'], None)  # Worker Y is pending on A to be done
        s = r['running_tasks'][0]
        self.assertEqual(s['task_id'], 'A')
        self.assertEqual(s['worker'], 'X')

    def test_scheduler_resources_none_allow_one(self):
        self.sch.add_task(worker='X', task_id='A', resources={'R1': 1})
        self.assertEqual(self.sch.get_work(worker='X')['task_id'], 'A')

    def test_scheduler_resources_none_disallow_two(self):
        self.sch.add_task(worker='X', task_id='A', resources={'R1': 2})
        self.assertFalse(self.sch.get_work(worker='X')['task_id'], 'A')

    def test_scheduler_with_insufficient_resources(self):
        self.sch.add_task(worker='X', task_id='A', resources={'R1': 3})
        self.sch.update_resources(R1=2)
        self.assertFalse(self.sch.get_work(worker='X')['task_id'])

    def test_scheduler_with_sufficient_resources(self):
        self.sch.add_task(worker='X', task_id='A', resources={'R1': 3})
        self.sch.update_resources(R1=3)
        self.assertEqual(self.sch.get_work(worker='X')['task_id'], 'A')

    def test_scheduler_with_resources_used(self):
        self.sch.add_task(worker='X', task_id='A', resources={'R1': 1})
        self.assertEqual(self.sch.get_work(worker='X')['task_id'], 'A')

        self.sch.add_task(worker='Y', task_id='B', resources={'R1': 1})
        self.sch.update_resources(R1=1)
        self.assertFalse(self.sch.get_work(worker='Y')['task_id'])

    def test_scheduler_overprovisioned_on_other_resource(self):
        self.sch.add_task(worker='X', task_id='A', resources={'R1': 2})
        self.sch.update_resources(R1=2)
        self.assertEqual(self.sch.get_work(worker='X')['task_id'], 'A')

        self.sch.add_task(worker='Y', task_id='B', resources={'R2': 2})
        self.sch.update_resources(R1=1, R2=2)
        self.assertEqual(self.sch.get_work(worker='Y')['task_id'], 'B')

    def test_scheduler_with_priority_and_competing_resources(self):
        self.sch.add_task(worker='X', task_id='A')
        self.assertEqual(self.sch.get_work(worker='X')['task_id'], 'A')

        self.sch.add_task(worker='X', task_id='B', resources={'R': 1}, priority=10)
        self.sch.add_task(worker='Y', task_id='C', resources={'R': 1}, priority=1)
        self.sch.update_resources(R=1)
        self.assertFalse(self.sch.get_work(worker='Y')['task_id'])

        self.sch.add_task(worker='Y', task_id='D', priority=0)
        self.assertEqual(self.sch.get_work(worker='Y')['task_id'], 'D')

    def test_update_resources(self):
        self.sch.add_task(WORKER, task_id='A', deps=['B'])
        self.sch.add_task(WORKER, task_id='B', resources={'r': 2})
        self.sch.update_resources(r=1)

        # B requires too many resources, we can't schedule
        self.check_task_order([])

        self.sch.add_task(WORKER, task_id='B', resources={'r': 1})

        # now we have enough resources
        self.check_task_order(['B', 'A'])

    def check_task_order(self, order):
        for expected_id in order:
            self.assertEqual(self.sch.get_work(WORKER)['task_id'], expected_id)
            self.sch.add_task(WORKER, expected_id, status=DONE)
        self.assertEqual(self.sch.get_work(WORKER)['task_id'], None)

    def test_priorities(self):
        self.sch.add_task(WORKER, 'A', priority=10)
        self.sch.add_task(WORKER, 'B', priority=5)
        self.sch.add_task(WORKER, 'C', priority=15)
        self.sch.add_task(WORKER, 'D', priority=9)
        self.check_task_order(['C', 'A', 'D', 'B'])

    def test_priorities_default_and_negative(self):
        self.sch.add_task(WORKER, 'A', priority=10)
        self.sch.add_task(WORKER, 'B')
        self.sch.add_task(WORKER, 'C', priority=15)
        self.sch.add_task(WORKER, 'D', priority=-20)
        self.sch.add_task(WORKER, 'E', priority=1)
        self.check_task_order(['C', 'A', 'E', 'B', 'D'])

    def test_priorities_and_dependencies(self):
        self.sch.add_task(WORKER, 'A', deps=['Z'], priority=10)
        self.sch.add_task(WORKER, 'B', priority=5)
        self.sch.add_task(WORKER, 'C', deps=['Z'], priority=3)
        self.sch.add_task(WORKER, 'D', priority=2)
        self.sch.add_task(WORKER, 'Z', priority=1)
        self.check_task_order(['Z', 'A', 'B', 'C', 'D'])

    def test_priority_update_dependency_after_scheduling(self):
        self.sch.add_task(WORKER, 'A', priority=1)
        self.sch.add_task(WORKER, 'B', priority=5, deps=['A'])
        self.sch.add_task(WORKER, 'C', priority=10, deps=['B'])
        self.sch.add_task(WORKER, 'D', priority=6)
        self.check_task_order(['A', 'B', 'C', 'D'])

    def test_priority_update_dependency_chain(self):
        self.sch.add_task(WORKER, 'A', priority=10, deps=['B'])
        self.sch.add_task(WORKER, 'B', priority=5, deps=['C'])
        self.sch.add_task(WORKER, 'C', priority=1)
        self.sch.add_task(WORKER, 'D', priority=6)
        self.check_task_order(['C', 'B', 'A', 'D'])

    def test_priority_no_decrease_with_multiple_updates(self):
        self.sch.add_task(WORKER, 'A', priority=1)
        self.sch.add_task(WORKER, 'B', priority=10, deps=['A'])
        self.sch.add_task(WORKER, 'C', priority=5, deps=['A'])
        self.sch.add_task(WORKER, 'D', priority=6)
        self.check_task_order(['A', 'B', 'D', 'C'])


if __name__ == '__main__':
    unittest.main()
