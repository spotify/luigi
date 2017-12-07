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

from helpers import unittest

from luigi import six

from helpers import with_config
import luigi
from luigi.db_task_history import DbTaskHistory
from luigi.task_status import DONE, PENDING, RUNNING
import luigi.scheduler


class DummyTask(luigi.Task):
    foo = luigi.Parameter(default='foo')


class ParamTask(luigi.Task):
    param1 = luigi.Parameter()
    param2 = luigi.IntParameter()


class DbTaskHistoryTest(unittest.TestCase):

    @with_config(dict(task_history=dict(db_connection='sqlite:///:memory:')))
    def setUp(self):
        self.history = DbTaskHistory()

    def test_task_list(self):
        self.run_task(DummyTask())
        self.run_task(DummyTask(foo='bar'))

        tasks = list(self.history.find_all_by_name('DummyTask'))

        self.assertEqual(len(tasks), 2)
        for task in tasks:
            self.assertEqual(task.name, 'DummyTask')
            self.assertEqual(task.host, 'hostname')

    def test_task_events(self):
        self.run_task(DummyTask())
        tasks = list(self.history.find_all_by_name('DummyTask'))
        self.assertEqual(len(tasks), 1)
        [task] = tasks
        self.assertEqual(task.name, 'DummyTask')
        self.assertEqual(len(task.events), 3)
        for (event, name) in zip(task.events, [DONE, RUNNING, PENDING]):
            self.assertEqual(event.event_name, name)

    def test_task_by_params(self):
        task1 = ParamTask('foo', 'bar')
        task2 = ParamTask('bar', 'foo')

        self.run_task(task1)
        self.run_task(task2)
        task1_record = self.history.find_all_by_parameters(task_name='ParamTask', param1='foo', param2='bar')
        task2_record = self.history.find_all_by_parameters(task_name='ParamTask', param1='bar', param2='foo')
        for task, records in zip((task1, task2), (task1_record, task2_record)):
            records = list(records)
            self.assertEqual(len(records), 1)
            [record] = records
            self.assertEqual(task.task_family, record.name)
            for param_name, param_value in six.iteritems(task.param_kwargs):
                self.assertTrue(param_name in record.parameters)
                self.assertEqual(str(param_value), record.parameters[param_name].value)

    def test_task_blank_param(self):
        self.run_task(DummyTask(foo=""))

        tasks = list(self.history.find_all_by_name('DummyTask'))

        self.assertEqual(len(tasks), 1)
        task_record = tasks[0]
        self.assertEqual(task_record.name, 'DummyTask')
        self.assertEqual(task_record.host, 'hostname')
        self.assertIn('foo', task_record.parameters)
        self.assertEqual(task_record.parameters['foo'].value, '')

    def run_task(self, task):
        task2 = luigi.scheduler.Task(task.task_id, PENDING, [], family=task.task_family,
                                     params=task.param_kwargs, retry_policy=luigi.scheduler._get_empty_retry_policy())

        self.history.task_scheduled(task2)
        self.history.task_started(task2, 'hostname')
        self.history.task_finished(task2, successful=True)


class MySQLDbTaskHistoryTest(unittest.TestCase):

    @with_config(dict(task_history=dict(db_connection='mysql+mysqlconnector://travis@localhost/luigi_test')))
    def setUp(self):
        try:
            self.history = DbTaskHistory()
        except Exception:
            raise unittest.SkipTest('DBTaskHistory cannot be created: probably no MySQL available')

    def test_subsecond_timestamp(self):
        # Add 2 events in <1s
        task = DummyTask()
        self.run_task(task)

        task_record = six.advance_iterator(self.history.find_all_by_name('DummyTask'))
        print(task_record.events)
        self.assertEqual(task_record.events[0].event_name, DONE)

    def test_utc_conversion(self):
        from luigi.server import from_utc

        task = DummyTask()
        self.run_task(task)

        task_record = six.advance_iterator(self.history.find_all_by_name('DummyTask'))
        last_event = task_record.events[0]
        try:
            print(from_utc(str(last_event.ts)))
        except ValueError:
            self.fail("Failed to convert timestamp {} to UTC".format(last_event.ts))

    def run_task(self, task):
        task2 = luigi.scheduler.Task(task.task_id, PENDING, [],
                                     family=task.task_family, params=task.param_kwargs,
                                     retry_policy=luigi.scheduler._get_empty_retry_policy())

        self.history.task_scheduled(task2)
        self.history.task_started(task2, 'hostname')
        self.history.task_finished(task2, successful=True)
