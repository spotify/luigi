# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import time
from helpers import unittest

import luigi.notifications
from luigi.scheduler import DISABLED, DONE, FAILED, CentralPlannerScheduler

luigi.notifications.DEBUG = True
WORKER = 'myworker'


class CentralPlannerTest(unittest.TestCase):

    def setUp(self):
        super(CentralPlannerTest, self).setUp()
        conf = self.get_scheduler_config()
        self.sch = CentralPlannerScheduler(**conf)
        self.time = time.time

    def get_scheduler_config(self):
        return {
            'retry_delay': 100,
            'remove_delay': 1000,
            'worker_disconnect_delay': 10,
            'disable_persist': 10,
            'disable_window': 10,
            'disable_failures': 3,
        }

    def tearDown(self):
        super(CentralPlannerTest, self).tearDown()
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
        for t in range(100):
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
        for t in range(200):
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
        for i in range(2000):
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

    def test_assistant_get_work(self):
        self.sch.add_task(worker='X', task_id='A')
        self.sch.add_worker('Y', [])

        self.assertEqual(self.sch.get_work('Y', assistant=True)['task_id'], 'A')

        # check that the scheduler recognizes tasks as running
        running_tasks = self.sch.task_list('RUNNING', '')
        self.assertEqual(len(running_tasks), 1)
        self.assertEqual(list(running_tasks.keys()), ['A'])
        self.assertEqual(running_tasks['A']['worker_running'], 'Y')

    def test_assistant_get_work_external_task(self):
        self.sch.add_task('X', task_id='A', runnable=False)
        self.assertTrue(self.sch.get_work('Y', assistant=True)['task_id'] is None)

    def test_task_fails_when_assistant_dies(self):
        self.setTime(0)
        self.sch.add_task(worker='X', task_id='A')
        self.sch.add_worker('Y', [])

        self.assertEqual(self.sch.get_work('Y', assistant=True)['task_id'], 'A')
        self.assertEqual(list(self.sch.task_list('RUNNING', '').keys()), ['A'])

        # Y dies for 50 seconds, X stays alive
        self.setTime(50)
        self.sch.ping('X')
        self.assertEqual(list(self.sch.task_list('FAILED', '').keys()), ['A'])

    def test_prune_with_live_assistant(self):
        self.setTime(0)
        self.sch.add_task(worker='X', task_id='A')
        self.sch.get_work('Y', assistant=True)
        self.sch.add_task(worker='Y', task_id='A', status=DONE, assistant=True)

        # worker X stops communicating, A should be marked for removal
        self.setTime(600)
        self.sch.ping('Y')
        self.sch.prune()

        # A will now be pruned
        self.setTime(2000)
        self.sch.prune()
        self.assertFalse(list(self.sch.task_list('', '')))

    def test_prune_done_tasks(self, expected=None):
        self.setTime(0)
        self.sch.add_task(WORKER, task_id='A', status=DONE)
        self.sch.add_task(WORKER, task_id='B', deps=['A'], status=DONE)
        self.sch.add_task(WORKER, task_id='C', deps=['B'])

        self.setTime(600)
        self.sch.ping('ASSISTANT')
        self.sch.prune()
        self.setTime(2000)
        self.sch.ping('ASSISTANT')
        self.sch.prune()

        self.assertEqual(set(expected or ()), set(self.sch.task_list('', '').keys()))

    def test_keep_tasks_for_assistant(self):
        self.sch.get_work('ASSISTANT', assistant=True)  # tell the scheduler this is an assistant
        self.test_prune_done_tasks(['B', 'C'])

    def test_keep_scheduler_disabled_tasks_for_assistant(self):
        self.sch.get_work('ASSISTANT', assistant=True)  # tell the scheduler this is an assistant

        # create a scheduler disabled task and a worker disabled task
        for i in range(10):
            self.sch.add_task(WORKER, 'D', status=FAILED)
        self.sch.add_task(WORKER, 'E', status=DISABLED)

        # scheduler prunes the worker disabled task
        self.assertEqual(set(['D', 'E']), set(self.sch.task_list(DISABLED, '')))
        self.test_prune_done_tasks(['B', 'C', 'D'])

    def test_keep_failed_tasks_for_assistant(self):
        self.sch.get_work('ASSISTANT', assistant=True)  # tell the scheduler this is an assistant
        self.sch.add_task(WORKER, 'D', status=FAILED, deps='A')
        self.test_prune_done_tasks(['A', 'B', 'C', 'D'])

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

    def test_do_not_lock_resources_when_not_ready(self):
        """ Test to make sure that resources won't go unused waiting on workers """
        self.sch.add_task(worker='X', task_id='A', priority=10)
        self.sch.add_task(worker='X', task_id='B', resources={'R': 1}, priority=5)
        self.sch.add_task(worker='Y', task_id='C', resources={'R': 1}, priority=1)

        self.sch.update_resources(R=1)
        self.sch.add_worker('X', [('workers', 1)])
        self.assertEqual('C', self.sch.get_work(worker='Y')['task_id'])

    def test_lock_resources_when_one_of_multiple_workers_is_ready(self):
        self.sch.add_task(worker='X', task_id='A', priority=10)
        self.sch.add_task(worker='X', task_id='B', resources={'R': 1}, priority=5)
        self.sch.add_task(worker='Y', task_id='C', resources={'R': 1}, priority=1)

        self.sch.update_resources(R=1)
        self.sch.add_worker('X', [('workers', 2)])
        self.sch.add_worker('Y', [])
        self.assertFalse(self.sch.get_work('Y')['task_id'])

    def test_do_not_lock_resources_while_running_higher_priority(self):
        """ Test to make sure that resources won't go unused waiting on workers """
        self.sch.add_task(worker='X', task_id='A', priority=10)
        self.sch.add_task(worker='X', task_id='B', resources={'R': 1}, priority=5)
        self.sch.add_task(worker='Y', task_id='C', resources={'R': 1}, priority=1)

        self.sch.update_resources(R=1)
        self.sch.add_worker('X', [('workers', 1)])
        self.assertEqual('A', self.sch.get_work('X')['task_id'])
        self.assertEqual('C', self.sch.get_work('Y')['task_id'])

    def test_lock_resources_while_running_lower_priority(self):
        """ Make sure resources will be made available while working on lower priority tasks """
        self.sch.add_task(worker='X', task_id='A', priority=4)
        self.assertEqual('A', self.sch.get_work('X')['task_id'])
        self.sch.add_task(worker='X', task_id='B', resources={'R': 1}, priority=5)
        self.sch.add_task(worker='Y', task_id='C', resources={'R': 1}, priority=1)

        self.sch.update_resources(R=1)
        self.sch.add_worker('X', [('workers', 1)])
        self.assertFalse(self.sch.get_work('Y')['task_id'])

    def test_lock_resources_for_second_worker(self):
        self.sch.add_task(worker='X', task_id='A', resources={'R': 1})
        self.sch.add_task(worker='X', task_id='B', resources={'R': 1})
        self.sch.add_task(worker='Y', task_id='C', resources={'R': 1}, priority=10)

        self.sch.add_worker('X', {'workers': 2})
        self.sch.add_worker('Y', {'workers': 1})
        self.sch.update_resources(R=2)

        self.assertEqual('A', self.sch.get_work('X')['task_id'])
        self.assertFalse(self.sch.get_work('X')['task_id'])

    def test_can_work_on_lower_priority_while_waiting_for_resources(self):
        self.sch.add_task(worker='X', task_id='A', resources={'R': 1}, priority=0)
        self.assertEqual('A', self.sch.get_work('X')['task_id'])

        self.sch.add_task(worker='Y', task_id='B', resources={'R': 1}, priority=10)
        self.sch.add_task(worker='Y', task_id='C', priority=0)
        self.sch.update_resources(R=1)

        self.assertEqual('C', self.sch.get_work('Y')['task_id'])

    def test_priority_update_with_pruning(self):
        self.setTime(0)
        self.sch.add_task(task_id='A', worker='X')

        self.setTime(50)  # after worker disconnects
        self.sch.prune()
        self.sch.add_task(task_id='B', deps=['A'], worker='X')

        self.setTime(2000)  # after remove for task A
        self.sch.prune()

        # Here task A that B depends on is missing
        self.sch.add_task(WORKER, task_id='C', deps=['B'], priority=100)
        self.sch.add_task(WORKER, task_id='B', deps=['A'])
        self.sch.add_task(WORKER, task_id='A')
        self.sch.add_task(WORKER, task_id='D', priority=10)

        self.check_task_order('ABCD')

    def test_update_resources(self):
        self.sch.add_task(WORKER, task_id='A', deps=['B'])
        self.sch.add_task(WORKER, task_id='B', resources={'r': 2})
        self.sch.update_resources(r=1)

        # B requires too many resources, we can't schedule
        self.check_task_order([])

        self.sch.add_task(WORKER, task_id='B', resources={'r': 1})

        # now we have enough resources
        self.check_task_order(['B', 'A'])

    def test_hendle_multiple_resources(self):
        self.sch.add_task(WORKER, task_id='A', resources={'r1': 1, 'r2': 1})
        self.sch.add_task(WORKER, task_id='B', resources={'r1': 1, 'r2': 1})
        self.sch.add_task(WORKER, task_id='C', resources={'r1': 1})
        self.sch.update_resources(r1=2, r2=1)

        self.assertEqual('A', self.sch.get_work(WORKER)['task_id'])
        self.check_task_order('C')

    def test_single_resource_lock(self):
        self.sch.add_task('X', task_id='A', resources={'r': 1})
        self.assertEqual('A', self.sch.get_work('X')['task_id'])

        self.sch.add_task(WORKER, task_id='B', resources={'r': 2}, priority=10)
        self.sch.add_task(WORKER, task_id='C', resources={'r': 1})
        self.sch.update_resources(r=2)

        # Should wait for 2 units of r to be available for B before scheduling C
        self.check_task_order([])

    def test_no_lock_if_too_many_resources_required(self):
        self.sch.add_task(WORKER, task_id='A', resources={'r': 2}, priority=10)
        self.sch.add_task(WORKER, task_id='B', resources={'r': 1})
        self.sch.update_resources(r=1)
        self.check_task_order('B')

    def test_multiple_resources_lock(self):
        self.sch.add_task('X', task_id='A', resources={'r1': 1, 'r2': 1}, priority=10)
        self.sch.add_task(WORKER, task_id='B', resources={'r2': 1})
        self.sch.add_task(WORKER, task_id='C', resources={'r1': 1})
        self.sch.update_resources(r1=1, r2=1)

        # should preserve both resources for worker 'X'
        self.check_task_order([])

    def test_multiple_resources_no_lock(self):
        self.sch.add_task(WORKER, task_id='A', resources={'r1': 1}, priority=10)
        self.sch.add_task(WORKER, task_id='B', resources={'r1': 1, 'r2': 1}, priority=10)
        self.sch.add_task(WORKER, task_id='C', resources={'r2': 1})
        self.sch.update_resources(r1=1, r2=2)

        self.assertEqual('A', self.sch.get_work(WORKER)['task_id'])
        # C doesn't block B, so it can go first
        self.check_task_order('C')

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

    def test_disable(self):
        self.sch.add_task(WORKER, 'A')
        self.sch.add_task(WORKER, 'A', status=FAILED)
        self.sch.add_task(WORKER, 'A', status=FAILED)
        self.sch.add_task(WORKER, 'A', status=FAILED)

        # should be disabled at this point
        self.assertEqual(len(self.sch.task_list('DISABLED', '')), 1)
        self.assertEqual(len(self.sch.task_list('FAILED', '')), 0)
        self.sch.add_task(WORKER, 'A')
        self.assertEqual(self.sch.get_work(WORKER)['task_id'], None)

    def test_disable_and_reenable(self):
        self.sch.add_task(WORKER, 'A')
        self.sch.add_task(WORKER, 'A', status=FAILED)
        self.sch.add_task(WORKER, 'A', status=FAILED)
        self.sch.add_task(WORKER, 'A', status=FAILED)

        # should be disabled at this point
        self.assertEqual(len(self.sch.task_list('DISABLED', '')), 1)
        self.assertEqual(len(self.sch.task_list('FAILED', '')), 0)

        self.sch.re_enable_task('A')

        # should be enabled at this point
        self.assertEqual(len(self.sch.task_list('DISABLED', '')), 0)
        self.assertEqual(len(self.sch.task_list('FAILED', '')), 1)
        self.sch.add_task(WORKER, 'A')
        self.assertEqual(self.sch.get_work(WORKER)['task_id'], 'A')

    def test_disable_and_reenable_and_disable_again(self):
        self.sch.add_task(WORKER, 'A')
        self.sch.add_task(WORKER, 'A', status=FAILED)
        self.sch.add_task(WORKER, 'A', status=FAILED)
        self.sch.add_task(WORKER, 'A', status=FAILED)

        # should be disabled at this point
        self.assertEqual(len(self.sch.task_list('DISABLED', '')), 1)
        self.assertEqual(len(self.sch.task_list('FAILED', '')), 0)

        self.sch.re_enable_task('A')

        # should be enabled at this point
        self.assertEqual(len(self.sch.task_list('DISABLED', '')), 0)
        self.assertEqual(len(self.sch.task_list('FAILED', '')), 1)
        self.sch.add_task(WORKER, 'A')
        self.assertEqual(self.sch.get_work(WORKER)['task_id'], 'A')

        self.sch.add_task(WORKER, 'A', status=FAILED)

        # should be still enabled
        self.assertEqual(len(self.sch.task_list('DISABLED', '')), 0)
        self.assertEqual(len(self.sch.task_list('FAILED', '')), 1)
        self.sch.add_task(WORKER, 'A')
        self.assertEqual(self.sch.get_work(WORKER)['task_id'], 'A')

        self.sch.add_task(WORKER, 'A', status=FAILED)
        self.sch.add_task(WORKER, 'A', status=FAILED)

        # should be disabled now
        self.assertEqual(len(self.sch.task_list('DISABLED', '')), 1)
        self.assertEqual(len(self.sch.task_list('FAILED', '')), 0)
        self.sch.add_task(WORKER, 'A')
        self.assertEqual(self.sch.get_work(WORKER)['task_id'], None)

    def test_disable_and_done(self):
        self.sch.add_task(WORKER, 'A')
        self.sch.add_task(WORKER, 'A', status=FAILED)
        self.sch.add_task(WORKER, 'A', status=FAILED)
        self.sch.add_task(WORKER, 'A', status=FAILED)

        # should be disabled at this point
        self.assertEqual(len(self.sch.task_list('DISABLED', '')), 1)
        self.assertEqual(len(self.sch.task_list('FAILED', '')), 0)

        self.sch.add_task(WORKER, 'A', status=DONE)

        # should be enabled at this point
        self.assertEqual(len(self.sch.task_list('DISABLED', '')), 0)
        self.assertEqual(len(self.sch.task_list('DONE', '')), 1)
        self.sch.add_task(WORKER, 'A')
        self.assertEqual(self.sch.get_work(WORKER)['task_id'], 'A')

    def test_disable_by_worker(self):
        self.sch.add_task(WORKER, 'A', status=DISABLED)
        self.assertEqual(len(self.sch.task_list('DISABLED', '')), 1)

        self.sch.add_task(WORKER, 'A')

        # should be enabled at this point
        self.assertEqual(len(self.sch.task_list('DISABLED', '')), 0)
        self.sch.add_task(WORKER, 'A')
        self.assertEqual(self.sch.get_work(WORKER)['task_id'], 'A')

    def test_task_list_beyond_limit(self):
        sch = CentralPlannerScheduler(max_shown_tasks=3)
        for c in 'ABCD':
            sch.add_task(WORKER, c)
        self.assertEqual(set('ABCD'), set(sch.task_list('PENDING', '', False).keys()))
        self.assertEqual({'num_tasks': 4}, sch.task_list('PENDING', ''))

    def test_task_list_within_limit(self):
        sch = CentralPlannerScheduler(max_shown_tasks=4)
        for c in 'ABCD':
            sch.add_task(WORKER, c)
        self.assertEqual(set('ABCD'), set(sch.task_list('PENDING', '').keys()))

    def test_task_lists_some_beyond_limit(self):
        sch = CentralPlannerScheduler(max_shown_tasks=3)
        for c in 'ABCD':
            sch.add_task(WORKER, c, 'DONE')
        for c in 'EFG':
            sch.add_task(WORKER, c)
        self.assertEqual(set('EFG'), set(sch.task_list('PENDING', '').keys()))
        self.assertEqual({'num_tasks': 4}, sch.task_list('DONE', ''))

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

    def test_unique_tasks(self):
        self.sch.add_task(WORKER, 'A')
        self.sch.add_task(WORKER, 'B')
        self.sch.add_task(WORKER, 'C')
        self.sch.add_task(WORKER + "_2", 'B')

        response = self.sch.get_work(WORKER)
        self.assertEqual(3, response['n_pending_tasks'])
        self.assertEqual(2, response['n_unique_pending'])

    def test_pending_downstream_disable(self):
        self.sch.add_task(WORKER, 'A', status=DISABLED)
        self.sch.add_task(WORKER, 'B', deps=('A',))
        self.sch.add_task(WORKER, 'C', deps=('B',))

        response = self.sch.get_work(WORKER)
        self.assertTrue(response['task_id'] is None)
        self.assertEqual(0, response['n_pending_tasks'])
        self.assertEqual(0, response['n_unique_pending'])

    def test_pending_downstream_failure(self):
        self.sch.add_task(WORKER, 'A', status=FAILED)
        self.sch.add_task(WORKER, 'B', deps=('A',))
        self.sch.add_task(WORKER, 'C', deps=('B',))

        response = self.sch.get_work(WORKER)
        self.assertTrue(response['task_id'] is None)
        self.assertEqual(2, response['n_pending_tasks'])
        self.assertEqual(2, response['n_unique_pending'])

    def test_prefer_more_dependents(self):
        self.sch.add_task(WORKER, 'A')
        self.sch.add_task(WORKER, 'B')
        self.sch.add_task(WORKER, 'C', deps=['B'])
        self.sch.add_task(WORKER, 'D', deps=['B'])
        self.sch.add_task(WORKER, 'E', deps=['A'])
        self.check_task_order('BACDE')

    def test_prefer_readier_dependents(self):
        self.sch.add_task(WORKER, 'A')
        self.sch.add_task(WORKER, 'B')
        self.sch.add_task(WORKER, 'C')
        self.sch.add_task(WORKER, 'D')
        self.sch.add_task(WORKER, 'F', deps=['A', 'B', 'C'])
        self.sch.add_task(WORKER, 'G', deps=['A', 'B', 'C'])
        self.sch.add_task(WORKER, 'E', deps=['D'])
        self.check_task_order('DABCFGE')

    def test_ignore_done_dependents(self):
        self.sch.add_task(WORKER, 'A')
        self.sch.add_task(WORKER, 'B')
        self.sch.add_task(WORKER, 'C')
        self.sch.add_task(WORKER, 'D', priority=1)
        self.sch.add_task(WORKER, 'E', deps=['C', 'D'])
        self.sch.add_task(WORKER, 'F', deps=['A', 'B'])
        self.check_task_order('DCABEF')


if __name__ == '__main__':
    unittest.main()
