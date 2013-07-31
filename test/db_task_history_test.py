# Copyright (c) 2013 Spotify AB
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

import helpers
import unittest

import luigi

from luigi.task_status import PENDING, RUNNING, DONE

try:
    from luigi.db_task_history import DbTaskHistory
except ImportError as e:
    raise unittest.SkipTest('Could not test db_task_history: %s' % e)


class DummyTask(luigi.Task):
    foo = luigi.Parameter(default='foo')


class ParamTask(luigi.Task):
    param1 = luigi.Parameter()
    param2 = luigi.IntParameter()


class DbTaskHistoryTest(unittest.TestCase):
    @helpers.with_config(dict(task_history=dict(db_connection='sqlite:///:memory:')))
    def setUp(self):
        self.history = DbTaskHistory()

    def test_task_list(self):
        self.run_task(DummyTask())
        self.run_task(DummyTask(foo='bar'))

        tasks = list(self.history.find_all_by_name('DummyTask'))

        self.assertEquals(len(tasks), 2)
        for task in tasks:
            self.assertEquals(task.name, 'DummyTask')
            self.assertEquals(task.host, 'hostname')

    def test_task_events(self):
        self.run_task(DummyTask())
        tasks = list(self.history.find_all_by_name('DummyTask'))
        self.assertEquals(len(tasks), 1)
        [task] = tasks
        self.assertEquals(task.name, 'DummyTask')
        self.assertEquals(len(task.events), 3)
        for (event, name) in zip(task.events, [DONE, RUNNING, PENDING]):
            self.assertEquals(event.event_name, name)

    def test_task_by_params(self):
        task1 = ParamTask('foo', 'bar')
        task2 = ParamTask('bar', 'foo')

        self.run_task(task1)
        self.run_task(task2)
        task1_record = self.history.find_all_by_parameters(task_name='ParamTask', param1='foo', param2='bar')
        task2_record = self.history.find_all_by_parameters(task_name='ParamTask', param1='bar', param2='foo')
        for task, records in zip((task1, task2), (task1_record, task2_record)):
            records = list(records)
            self.assertEquals(len(records), 1)
            [record] = records
            self.assertEqual(task.task_family, record.name)
            for param_name, param_value in task.param_kwargs.iteritems():
                self.assertTrue(param_name in record.parameters)
                self.assertEquals(str(param_value), record.parameters[param_name].value)

    def run_task(self, task):
        self.history.task_scheduled(task.task_id)
        self.history.task_started(task.task_id, 'hostname')
        self.history.task_finished(task.task_id, successful=True)
