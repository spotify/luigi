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

import sqlalchemy as sqla
from helpers import unittest, with_config

import luigi
import luigi.scheduler
from luigi.db_task_history import DbTaskHistory, _upgrade_schema
from luigi.parameter import ParameterVisibility
from luigi.task_status import DONE, PENDING, RUNNING


class DummyTask(luigi.Task):
    foo = luigi.Parameter(default="foo")


class ParamTask(luigi.Task):
    param1 = luigi.Parameter()
    param2 = luigi.IntParameter(visibility=ParameterVisibility.HIDDEN)
    param3 = luigi.Parameter(default="empty", visibility=ParameterVisibility.PRIVATE)


class DbTaskHistoryTest(unittest.TestCase):
    @with_config(dict(task_history=dict(db_connection="sqlite:///:memory:")))
    def setUp(self):
        self.history = DbTaskHistory()

    def test_upgrade_schema(self):
        """Test that the task_id column and index are added if they don't exist."""
        # Create a temporary SQLite database
        engine = sqla.create_engine("sqlite:///:memory:")

        # Create the tasks table without the task_id column
        with engine.connect() as conn:
            conn.execute(sqla.text("CREATE TABLE tasks (id INTEGER PRIMARY KEY, name TEXT)"))
            conn.execute(sqla.text("CREATE TABLE task_parameters (task_id INTEGER PRIMARY KEY, name TEXT, value TEXT)"))
            conn.commit()

        # Run the upgrade schema function
        _upgrade_schema(engine)

        # Verify that the task_id column and index were created
        with engine.connect() as conn:
            columns = conn.execute(sqla.text("PRAGMA table_info(tasks)")).fetchall()
            column_names = [column.name for column in columns]
            column_types = [column.type for column in columns]
            column_name_idx = column_names.index("task_id")
            assert "task_id" == column_names[column_name_idx]
            assert "VARCHAR(200)" == column_types[column_name_idx]

            indexes = conn.execute(sqla.text("PRAGMA index_list(tasks)")).fetchall()
            index_names = [index.name for index in indexes]
            assert "ix_task_id" in index_names

            columns = conn.execute(sqla.text("PRAGMA table_info(task_parameters)")).fetchall()
            column_names = [column.name for column in columns]
            assert "value" in column_names

    def test_task_list(self):
        self.run_task(DummyTask())
        self.run_task(DummyTask(foo="bar"))

        with self.history._session() as session:
            tasks = list(self.history.find_all_by_name("DummyTask", session))

            self.assertEqual(len(tasks), 2)
            for task in tasks:
                self.assertEqual(task.name, "DummyTask")
                self.assertEqual(task.host, "hostname")

    def test_task_events(self):
        self.run_task(DummyTask())

        with self.history._session() as session:
            tasks = list(self.history.find_all_by_name("DummyTask", session))
            self.assertEqual(len(tasks), 1)
            [task] = tasks
            self.assertEqual(task.name, "DummyTask")
            self.assertEqual(len(task.events), 3)
            for event, name in zip(task.events, [DONE, RUNNING, PENDING]):
                self.assertEqual(event.event_name, name)

    def test_task_by_params(self):
        task1 = ParamTask("foo", "bar")
        task2 = ParamTask("bar", "foo")

        with self.history._session() as session:
            self.run_task(task1)
            self.run_task(task2)
            task1_record = self.history.find_all_by_parameters(task_name="ParamTask", session=session, param1="foo", param2="bar")
            task2_record = self.history.find_all_by_parameters(task_name="ParamTask", session=session, param1="bar", param2="foo")
            for task, records in zip((task1, task2), (task1_record, task2_record)):
                records = list(records)
                self.assertEqual(len(records), 1)
                [record] = records
                self.assertEqual(task.task_family, record.name)
                for param_name, param_value in task.param_kwargs.items():
                    self.assertTrue(param_name in record.parameters)
                    self.assertEqual(str(param_value), record.parameters[param_name].value)

    def test_task_blank_param(self):
        self.run_task(DummyTask(foo=""))

        with self.history._session() as session:
            tasks = list(self.history.find_all_by_name("DummyTask", session))

            self.assertEqual(len(tasks), 1)
            task_record = tasks[0]
            self.assertEqual(task_record.name, "DummyTask")
            self.assertEqual(task_record.host, "hostname")
            self.assertIn("foo", task_record.parameters)
            self.assertEqual(task_record.parameters["foo"].value, "")

    def run_task(self, task):
        task2 = luigi.scheduler.Task(
            task.task_id, PENDING, [], family=task.task_family, params=task.param_kwargs, retry_policy=luigi.scheduler._get_empty_retry_policy()
        )

        self.history.task_scheduled(task2)
        self.history.task_started(task2, "hostname")
        self.history.task_finished(task2, successful=True)


class MySQLDbTaskHistoryTest(unittest.TestCase):
    @with_config(dict(task_history=dict(db_connection="mysql+mysqlconnector://travis@localhost/luigi_test")))
    def setUp(self):
        try:
            self.history = DbTaskHistory()
        except Exception:
            raise unittest.SkipTest("DBTaskHistory cannot be created: probably no MySQL available")

    def test_subsecond_timestamp(self):
        with self.history._session() as session:
            # Add 2 events in <1s
            task = DummyTask()
            self.run_task(task)

            task_record = next(self.history.find_all_by_name("DummyTask", session))
            print(task_record.events)
            self.assertEqual(task_record.events[0].event_name, DONE)

    def test_utc_conversion(self):
        from luigi.server import from_utc

        with self.history._session() as session:
            task = DummyTask()
            self.run_task(task)

            task_record = next(self.history.find_all_by_name("DummyTask", session))
            last_event = task_record.events[0]
            try:
                print(from_utc(str(last_event.ts)))
            except ValueError:
                self.fail("Failed to convert timestamp {} to UTC".format(last_event.ts))

    def run_task(self, task):
        task2 = luigi.scheduler.Task(
            task.task_id, PENDING, [], family=task.task_family, params=task.param_kwargs, retry_policy=luigi.scheduler._get_empty_retry_policy()
        )

        self.history.task_scheduled(task2)
        self.history.task_started(task2, "hostname")
        self.history.task_finished(task2, successful=True)
