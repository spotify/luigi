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

class TestParameterSplit(unittest.TestCase):
    task_id_examples = [
        "TrackIsrcs()",
        "CrazyTask(foo=foo_table_id, bar={'keyName': 'com.my.org', 'parameters': {'this.is.tricky': '1'}}, what_is_dis=foo bar, oh hippo)",
        "MyOldDateHourTask(datehour=2013-07-21 11:00:00)",
        "MyOldDateHourTask(datehour=2013-07-21T11:00:00)"
    ]

    def setUp(self):
        self.sch = CentralPlannerScheduler()

    def test_parameter_split(self):
        for task_id in self.task_id_examples:
            self.sch._get_task_params(task_id)

if __name__ == '__main__':
    unittest.main()
