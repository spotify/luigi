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
            del cps._state.get_worker('B').tasks  # If you upgrade from old server
            cps = reload_from_disk(cps=cps)  # tihii, cps == continuation passing style ;)
            self.assertEqual(cps.get_work(worker='B')['task_id'], '2')
            self.assertEqual(cps.get_work(worker='C')['task_id'], '3')
            cps = reload_from_disk(cps=cps)  # This time without deleting
            self.assertEqual(cps.get_work(worker='D')['task_id'], '4')

    def test_worker_prune_after_init(self):
        worker = luigi.scheduler.Worker(123)

        class TmpCfg:
            def __init__(self):
                self.worker_disconnect_delay = 10

        worker.prune(TmpCfg())

if __name__ == '__main__':
    unittest.main()
