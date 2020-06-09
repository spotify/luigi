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

import pickle
import tempfile
import time
import os
import shutil
from multiprocessing import Process
from helpers import unittest

import luigi.scheduler
import luigi.server
import luigi.configuration
from helpers import with_config
from luigi.target import FileAlreadyExists


class SchedulerIoTest(unittest.TestCase):

    def test_pretty_id_unicode(self):
        scheduler = luigi.scheduler.Scheduler()
        scheduler.add_task(worker='A', task_id='1', params={u'foo': u'\u2192bar'})
        [task] = list(scheduler._state.get_active_tasks())
        task.pretty_id

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

            self.assertEqual(set(state.get_worker_ids()), {'Worker1', 'Worker2'})

    def test_load_broken_state(self):
        with tempfile.NamedTemporaryFile(delete=True) as fn:
            with open(fn.name, 'w') as fobj:
                print("b0rk", file=fobj)

            state = luigi.scheduler.SimpleTaskState(
                state_path=fn.name)
            state.load()  # bad if this crashes

            self.assertEqual(list(state.get_worker_ids()), [])

    @with_config({'scheduler': {'retry_count': '44', 'worker_disconnect_delay': '55'}})
    def test_scheduler_with_config(self):
        scheduler = luigi.scheduler.Scheduler()
        self.assertEqual(44, scheduler._config.retry_count)
        self.assertEqual(55, scheduler._config.worker_disconnect_delay)

        # Override
        scheduler = luigi.scheduler.Scheduler(retry_count=66,
                                              worker_disconnect_delay=77)
        self.assertEqual(66, scheduler._config.retry_count)
        self.assertEqual(77, scheduler._config.worker_disconnect_delay)

    @with_config({'resources': {'a': '100', 'b': '200'}})
    def test_scheduler_with_resources(self):
        scheduler = luigi.scheduler.Scheduler()
        self.assertEqual({'a': 100, 'b': 200}, scheduler._resources)

    @with_config({'scheduler': {'record_task_history': 'True'},
                  'task_history': {'db_connection': 'sqlite:////none/existing/path/hist.db'}})
    def test_local_scheduler_task_history_status(self):
        ls = luigi.interface._WorkerSchedulerFactory().create_local_scheduler()
        self.assertEqual(False, ls._config.record_task_history)

    def test_load_recovers_tasks_index(self):
        scheduler = luigi.scheduler.Scheduler()
        scheduler.add_task(worker='A', task_id='1')
        scheduler.add_task(worker='B', task_id='2')
        scheduler.add_task(worker='C', task_id='3')
        scheduler.add_task(worker='D', task_id='4')
        self.assertEqual(scheduler.get_work(worker='A')['task_id'], '1')

        with tempfile.NamedTemporaryFile(delete=True) as fn:
            def reload_from_disk(scheduler):
                scheduler._state._state_path = fn.name
                scheduler.dump()
                scheduler = luigi.scheduler.Scheduler()
                scheduler._state._state_path = fn.name
                scheduler.load()
                return scheduler
            scheduler = reload_from_disk(scheduler=scheduler)
            self.assertEqual(scheduler.get_work(worker='B')['task_id'], '2')
            self.assertEqual(scheduler.get_work(worker='C')['task_id'], '3')
            scheduler = reload_from_disk(scheduler=scheduler)
            self.assertEqual(scheduler.get_work(worker='D')['task_id'], '4')

    def test_worker_prune_after_init(self):
        """
        See https://github.com/spotify/luigi/pull/1019
        """
        worker = luigi.scheduler.Worker(123)

        class TmpCfg:
            def __init__(self):
                self.worker_disconnect_delay = 10

        worker.prune(TmpCfg())

    def test_get_empty_retry_policy(self):
        retry_policy = luigi.scheduler._get_empty_retry_policy()
        self.assertEqual(3, len(retry_policy))
        self.assertEqual(["retry_count", "disable_hard_timeout", "disable_window"], list(retry_policy._asdict().keys()))
        self.assertEqual([None, None, None], list(retry_policy._asdict().values()))

    @with_config({'scheduler': {'retry_count': '9', 'disable_hard_timeout': '99', 'disable_window': '999'}})
    def test_scheduler_get_retry_policy(self):
        s = luigi.scheduler.Scheduler()
        self.assertEqual(luigi.scheduler.RetryPolicy(9, 99, 999), s._config._get_retry_policy())

    @with_config({'scheduler': {'retry_count': '9', 'disable_hard_timeout': '99', 'disable_window': '999'}})
    def test_generate_retry_policy(self):
        s = luigi.scheduler.Scheduler()

        try:
            s._generate_retry_policy({'inexist_attr': True})
            self.assertFalse(True, "'unexpected keyword argument' error must have been thrown")
        except TypeError:
            self.assertTrue(True)

        retry_policy = s._generate_retry_policy({})
        self.assertEqual(luigi.scheduler.RetryPolicy(9, 99, 999), retry_policy)

        retry_policy = s._generate_retry_policy({'retry_count': 1})
        self.assertEqual(luigi.scheduler.RetryPolicy(1, 99, 999), retry_policy)

        retry_policy = s._generate_retry_policy({'retry_count': 1, 'disable_hard_timeout': 11, 'disable_window': 111})
        self.assertEqual(luigi.scheduler.RetryPolicy(1, 11, 111), retry_policy)

    @with_config({'scheduler': {'retry_count': '44'}})
    def test_per_task_retry_policy(self):
        cps = luigi.scheduler.Scheduler()

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

        self.assertEqual(luigi.scheduler.RetryPolicy(44, 999999999, 3600), task_1.retry_policy)
        self.assertEqual(luigi.scheduler.RetryPolicy(44, 999999999, 3600), task_2.retry_policy)
        self.assertEqual(luigi.scheduler.RetryPolicy(44, 999999999, 3600), task_3.retry_policy)

        cps._state._tasks = {}
        cps.add_task(worker='test_worker2', task_id='test_task_4', deps=['test_task_5', 'test_task_6'],
                     retry_policy_dict=luigi.scheduler.RetryPolicy(99, 999, 9999)._asdict())

        tasks = list(cps._state.get_active_tasks())
        self.assertEqual(3, len(tasks))

        tasks = sorted(tasks, key=lambda x: x.id)
        task_4 = tasks[0]
        task_5 = tasks[1]
        task_6 = tasks[2]

        self.assertEqual('test_task_4', task_4.id)
        self.assertEqual('test_task_5', task_5.id)
        self.assertEqual('test_task_6', task_6.id)

        self.assertEqual(luigi.scheduler.RetryPolicy(99, 999, 9999), task_4.retry_policy)
        self.assertEqual(luigi.scheduler.RetryPolicy(44, 999999999, 3600), task_5.retry_policy)
        self.assertEqual(luigi.scheduler.RetryPolicy(44, 999999999, 3600), task_6.retry_policy)

        cps._state._tasks = {}
        cps.add_task(worker='test_worker3', task_id='test_task_7', deps=['test_task_8', 'test_task_9'])
        cps.add_task(worker='test_worker3', task_id='test_task_8', retry_policy_dict=luigi.scheduler.RetryPolicy(99, 999, 9999)._asdict())
        cps.add_task(worker='test_worker3', task_id='test_task_9', retry_policy_dict=luigi.scheduler.RetryPolicy(11, 111, 1111)._asdict())

        tasks = list(cps._state.get_active_tasks())
        self.assertEqual(3, len(tasks))

        tasks = sorted(tasks, key=lambda x: x.id)
        task_7 = tasks[0]
        task_8 = tasks[1]
        task_9 = tasks[2]

        self.assertEqual('test_task_7', task_7.id)
        self.assertEqual('test_task_8', task_8.id)
        self.assertEqual('test_task_9', task_9.id)

        self.assertEqual(luigi.scheduler.RetryPolicy(44, 999999999, 3600), task_7.retry_policy)
        self.assertEqual(luigi.scheduler.RetryPolicy(99, 999, 9999), task_8.retry_policy)
        self.assertEqual(luigi.scheduler.RetryPolicy(11, 111, 1111), task_9.retry_policy)

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

    @with_config({'scheduler': {'record_task_history': 'true'}})
    def test_has_task_history(self):
        cfg = luigi.configuration.get_config()
        with tempfile.NamedTemporaryFile(suffix='.db', delete=True) as fn:
            cfg.set('task_history', 'db_connection', 'sqlite:///' + fn.name)
            s = luigi.scheduler.Scheduler()
            self.assertTrue(s.has_task_history())

    @with_config({'scheduler': {'record_task_history': 'false'}})
    def test_has_no_task_history(self):
        s = luigi.scheduler.Scheduler()
        self.assertFalse(s.has_task_history())

    @with_config({'scheduler': {'pause_enabled': 'false'}})
    def test_pause_disabled(self):
        s = luigi.scheduler.Scheduler()
        self.assertFalse(s.is_pause_enabled()['enabled'])
        self.assertFalse(s.is_paused()['paused'])
        s.pause()
        self.assertFalse(s.is_paused()['paused'])

    def test_default_metrics_collector(self):
        from luigi.metrics import MetricsCollector

        s = luigi.scheduler.Scheduler()
        scheduler_state = s._state
        collector = scheduler_state._metrics_collector
        self.assertTrue(isinstance(collector, MetricsCollector))

    @with_config({'scheduler': {'metrics_collector': 'datadog'}})
    def test_datadog_metrics_collector(self):
        from luigi.contrib.datadog_metric import DatadogMetricsCollector

        s = luigi.scheduler.Scheduler()
        scheduler_state = s._state
        collector = scheduler_state._metrics_collector
        self.assertTrue(isinstance(collector, DatadogMetricsCollector))

    @with_config({'scheduler': {'metrics_collector': 'prometheus'}})
    def test_prometheus_metrics_collector(self):
        from luigi.contrib.prometheus_metric import PrometheusMetricsCollector

        s = luigi.scheduler.Scheduler()
        scheduler_state = s._state
        collector = scheduler_state._metrics_collector
        self.assertTrue(isinstance(collector, PrometheusMetricsCollector))


class SchedulerWorkerTest(unittest.TestCase):
    def get_pending_ids(self, worker, state):
        return {task.id for task in worker.get_tasks(state, 'PENDING')}

    def test_get_pending_tasks_with_many_done_tasks(self):
        sch = luigi.scheduler.Scheduler()
        sch.add_task(worker='NON_TRIVIAL', task_id='A', resources={'a': 1})
        sch.add_task(worker='TRIVIAL', task_id='B', status='PENDING')
        sch.add_task(worker='TRIVIAL', task_id='C', status='DONE')
        sch.add_task(worker='TRIVIAL', task_id='D', status='DONE')

        scheduler_state = sch._state
        trivial_worker = scheduler_state.get_worker('TRIVIAL')
        self.assertEqual({'B'}, self.get_pending_ids(trivial_worker, scheduler_state))

        non_trivial_worker = scheduler_state.get_worker('NON_TRIVIAL')
        self.assertEqual({'A'}, self.get_pending_ids(non_trivial_worker, scheduler_state))


class FailingOnDoubleRunTask(luigi.Task):
    time_to_check_secs = 1
    time_to_run_secs = 2
    output_dir = luigi.Parameter(default="")

    def __init__(self, *args, **kwargs):
        super(FailingOnDoubleRunTask, self).__init__(*args, **kwargs)
        self.file_name = os.path.join(self.output_dir, "AnyTask")

    def complete(self):
        time.sleep(self.time_to_check_secs)  # e.g., establish connection
        exists = os.path.exists(self.file_name)
        time.sleep(self.time_to_check_secs)  # e.g., close connection
        return exists

    def run(self):
        time.sleep(self.time_to_run_secs)
        if os.path.exists(self.file_name):
            raise FileAlreadyExists(self.file_name)
        open(self.file_name, 'w').close()


class StableDoneCooldownSecsTest(unittest.TestCase):

    def setUp(self):
        self.p = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.p)

    def run_task(self):
        return luigi.build([FailingOnDoubleRunTask(output_dir=self.p)],
                           detailed_summary=True,
                           parallel_scheduling=True,
                           parallel_scheduling_processes=2)

    @with_config({'worker': {'keep_alive': 'false'}})
    def get_second_run_result_on_double_run(self):
        server_process = Process(target=luigi.server.run)
        process = Process(target=self.run_task)
        try:
            # scheduler is started
            server_process.start()
            # first run is started
            process.start()
            time.sleep(FailingOnDoubleRunTask.time_to_run_secs + FailingOnDoubleRunTask.time_to_check_secs)
            # second run of the same task is started
            second_run_result = self.run_task()
            return second_run_result
        finally:
            process.join(1)
            server_process.terminate()
            server_process.join(1)

    @with_config({'scheduler': {'stable_done_cooldown_secs': '5'}})
    def test_sending_same_task_twice_with_cooldown_does_not_lead_to_double_run(self):
        second_run_result = self.get_second_run_result_on_double_run()
        self.assertEqual(second_run_result.scheduling_succeeded, True)

    @with_config({'scheduler': {'stable_done_cooldown_secs': '0'}})
    def test_sending_same_task_twice_without_cooldown_leads_to_double_run(self):
        second_run_result = self.get_second_run_result_on_double_run()
        self.assertEqual(second_run_result.scheduling_succeeded, False)
