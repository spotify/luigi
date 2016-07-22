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
from __future__ import print_function

import pickle
import tempfile
import time
from helpers import unittest

import luigi.scheduler
from helpers import with_config
import logging

logging.config.fileConfig('test/testconfig/logging.cfg', disable_existing_loggers=False)
luigi.notifications.DEBUG = True


class SchedulerTest(unittest.TestCase):

    def test_load_old_state(self):
        tasks = {}
        active_workers = {'Worker1': 1e9, 'Worker2': time.time()}

        with tempfile.NamedTemporaryFile(delete=True) as fn:
            with open(fn.name, 'wb') as fobj:
                state = (tasks, active_workers)
                pickle.dump(state, fobj)

            state = luigi.scheduler.SimpleTaskState(
                state_path=fn.name)
            state.load()

            self.assertEqual(set(state.get_worker_ids()),
                             set(['Worker1', 'Worker2']))

    def test_load_broken_state(self):
        with tempfile.NamedTemporaryFile(delete=True) as fn:
            with open(fn.name, 'w') as fobj:
                print("b0rk", file=fobj)

            state = luigi.scheduler.SimpleTaskState(
                state_path=fn.name)
            state.load()  # bad if this crashes

            self.assertEqual(list(state.get_worker_ids()), [])

    @with_config({'scheduler': {'disable-num-failures': '44', 'worker-disconnect-delay': '55'}})
    def test_scheduler_with_config(self):
        cps = luigi.scheduler.CentralPlannerScheduler()
        self.assertEqual(44, cps._config.disable_failures)
        self.assertEqual(55, cps._config.worker_disconnect_delay)

        # Override
        cps = luigi.scheduler.CentralPlannerScheduler(disable_failures=66,
                                                      worker_disconnect_delay=77)
        self.assertEqual(66, cps._config.disable_failures)
        self.assertEqual(77, cps._config.worker_disconnect_delay)

    @with_config({'resources': {'a': '100', 'b': '200'}})
    def test_scheduler_with_resources(self):
        cps = luigi.scheduler.CentralPlannerScheduler()
        self.assertEqual({'a': 100, 'b': 200}, cps._resources)

    @with_config({'scheduler': {'record_task_history': 'True'},
                  'task_history': {'db_connection': 'sqlite:////none/existing/path/hist.db'}})
    def test_local_scheduler_task_history_status(self):
        ls = luigi.interface._WorkerSchedulerFactory().create_local_scheduler()
        self.assertEqual(False, ls._config.record_task_history)

    def test_load_recovers_tasks_index(self):
        cps = luigi.scheduler.CentralPlannerScheduler()
        cps.add_task(worker='A', task_id='1')
        cps.add_task(worker='B', task_id='2')
        cps.add_task(worker='C', task_id='3')
        cps.add_task(worker='D', task_id='4')
        self.assertEqual(cps.get_work(worker='A')['task_id'], '1')

        with tempfile.NamedTemporaryFile(delete=True) as fn:
            def reload_from_disk(cps):
                cps._state._state_path = fn.name
                cps.dump()
                cps = luigi.scheduler.CentralPlannerScheduler()
                cps._state._state_path = fn.name
                cps.load()
                return cps
            cps = reload_from_disk(cps=cps)
            self.assertEqual(cps.get_work(worker='B')['task_id'], '2')
            self.assertEqual(cps.get_work(worker='C')['task_id'], '3')
            cps = reload_from_disk(cps=cps)
            self.assertEqual(cps.get_work(worker='D')['task_id'], '4')

    def test_worker_prune_after_init(self):
        worker = luigi.scheduler.Worker(123)

        class TmpCfg:
            def __init__(self):
                self.worker_disconnect_delay = 10

        worker.prune(TmpCfg())

    @with_config({'scheduler': {'disable-num-failures': '44'}})
    def test_scheduler_with_task_level_config(self):
        cps = luigi.scheduler.CentralPlannerScheduler()

        cps.add_task(worker='test_worker1', task_id='test_task_1', deps=['test_task_2', 'test_task_3'])
        tasks = list(cps._state.get_active_tasks())
        self.assertEqual(3, len(tasks))

        tasks = sorted(tasks, key=lambda x: x.id)
        task_1 = tasks[0]
        task_2 = tasks[1]
        task_3 = tasks[2]

        self.assertEqual('test_task_1', task_1.id)
        self.assertEqual('test_task_2', task_2.id)
        self.assertEqual('test_task_3', task_3.id)

        self.assertDictEqual({}, task_1.config)
        self.assertDictEqual({}, task_2.config)
        self.assertDictEqual({}, task_3.config)

        self.assertEqual(44, task_1.disable_failures)
        self.assertEqual(False, task_1.upstream_status_when_all)
        self.assertEqual(44, task_2.disable_failures)
        self.assertEqual(False, task_2.upstream_status_when_all)
        self.assertEqual(44, task_3.disable_failures)
        self.assertEqual(False, task_3.upstream_status_when_all)

        cps._state._tasks = {}
        cps.add_task(worker='test_worker2', task_id='test_task_4', deps=['test_task_5', 'test_task_6'],
                     task_config={
                         'disable-num-failures': 99,
                         'disable-hard-timeout': 999,
                         'disable-window-seconds': 9999,
                         'upstream-status-when-all': True
                     })

        tasks = list(cps._state.get_active_tasks())
        self.assertEqual(3, len(tasks))

        tasks = sorted(tasks, key=lambda x: x.id)
        task_4 = tasks[0]
        task_5 = tasks[1]
        task_6 = tasks[2]

        self.assertEqual('test_task_4', task_4.id)
        self.assertEqual('test_task_5', task_5.id)
        self.assertEqual('test_task_6', task_6.id)

        self.assertEqual(99, task_4.disable_failures)
        self.assertEqual(999, task_4.disable_hard_timeout)
        self.assertEqual(9999, task_4.failures.window)
        self.assertEqual(True, task_4.upstream_status_when_all)

        self.assertEqual(44, task_5.disable_failures)
        self.assertEqual(False, task_5.upstream_status_when_all)

        self.assertEqual(44, task_6.disable_failures)
        self.assertEqual(False, task_6.upstream_status_when_all)

        cps._state._tasks = {}
        cps.add_task(worker='test_worker3', task_id='test_task_7', deps=['test_task_8', 'test_task_9'],
                     deps_configs={
                         'test_task_8': {
                             'disable-num-failures': 99,
                             'disable-hard-timeout': 999,
                             'disable-window-seconds': 9999,
                             'upstream-status-when-all': True
                         },
                         'test_task_9': {
                             'disable-num-failures': 11,
                             'disable-hard-timeout': 111,
                             'disable-window-seconds': 1111,
                             'upstream-status-when-all': False
                         }
                     })

        tasks = list(cps._state.get_active_tasks())
        self.assertEqual(3, len(tasks))

        tasks = sorted(tasks, key=lambda x: x.id)
        task_7 = tasks[0]
        task_8 = tasks[1]
        task_9 = tasks[2]

        self.assertEqual('test_task_7', task_7.id)
        self.assertEqual('test_task_8', task_8.id)
        self.assertEqual('test_task_9', task_9.id)

        self.assertEqual(44, task_7.disable_failures)
        self.assertEqual(False, task_7.upstream_status_when_all)

        self.assertEqual(99, task_8.disable_failures)
        self.assertEqual(999, task_8.disable_hard_timeout)
        self.assertEqual(9999, task_8.failures.window)
        self.assertEqual(True, task_8.upstream_status_when_all)

        self.assertEqual(11, task_9.disable_failures)
        self.assertEqual(111, task_9.disable_hard_timeout)
        self.assertEqual(1111, task_9.failures.window)
        self.assertEqual(False, task_9.upstream_status_when_all)

        # Task 7 which is disable-failures 44 and its has_excessive_failures method returns False under 44
        for i in range(43):
            task_7.add_failure()
        self.assertFalse(task_7.has_excessive_failures())
        task_7.add_failure()
        self.assertTrue(task_7.has_excessive_failures())

        # Task 8 which is disable-failures 99 and its has_excessive_failures method returns False under 44
        for i in range(98):
            task_8.add_failure()
        self.assertFalse(task_8.has_excessive_failures())
        task_8.add_failure()
        self.assertTrue(task_8.has_excessive_failures())

        # Task 9 which is disable-failures 1 and its has_excessive_failures method returns False under 44
        for i in range(10):
            task_9.add_failure()
        self.assertFalse(task_9.has_excessive_failures())
        task_9.add_failure()
        self.assertTrue(task_9.has_excessive_failures())


if __name__ == '__main__':
    unittest.main()
