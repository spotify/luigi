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

import luigi
import luigi.date_interval
import luigi.notifications
import sys
from luigi.interface import _WorkerSchedulerFactory
from luigi.worker import Worker
from mock import Mock, patch, MagicMock
from helpers import LuigiTestCase

luigi.notifications.DEBUG = True


class InterfaceTest(LuigiTestCase):

    def setUp(self):
        self.worker = Worker()

        self.worker_scheduler_factory = _WorkerSchedulerFactory()
        self.worker_scheduler_factory.create_worker = Mock(return_value=self.worker)
        self.worker_scheduler_factory.create_local_scheduler = Mock()
        super(InterfaceTest, self).setUp()

        class NoOpTask(luigi.Task):
            param = luigi.Parameter()

        self.task_a = NoOpTask("a")
        self.task_b = NoOpTask("b")

    def test_interface_run_positive_path(self):
        self.worker.add = Mock(side_effect=[True, True])
        self.worker.run = Mock(return_value=True)

        self.assertTrue(self._run_interface())

    def test_interface_run_with_add_failure(self):
        self.worker.add = Mock(side_effect=[True, False])
        self.worker.run = Mock(return_value=True)

        self.assertFalse(self._run_interface())

    def test_interface_run_with_run_failure(self):
        self.worker.add = Mock(side_effect=[True, True])
        self.worker.run = Mock(return_value=False)

        self.assertFalse(self._run_interface())

    def test_stops_worker_on_add_exception(self):
        worker = MagicMock()
        self.worker_scheduler_factory.create_worker = Mock(return_value=worker)
        worker.add = Mock(side_effect=AttributeError)

        self.assertRaises(AttributeError, self._run_interface)
        self.assertTrue(worker.__exit__.called)

    def test_stops_worker_on_run_exception(self):
        worker = MagicMock()
        self.worker_scheduler_factory.create_worker = Mock(return_value=worker)
        worker.add = Mock(side_effect=[True, True])
        worker.run = Mock(side_effect=AttributeError)

        self.assertRaises(AttributeError, self._run_interface)
        self.assertTrue(worker.__exit__.called)

    def test_just_run_main_task_cls(self):
        class MyTestTask(luigi.Task):
            pass

        class MyOtherTestTask(luigi.Task):
            my_param = luigi.Parameter()

        with patch.object(sys, 'argv', ['my_module.py', '--no-lock', '--local-scheduler']):
            luigi.run(main_task_cls=MyTestTask)

        with patch.object(sys, 'argv', ['my_module.py', '--no-lock', '--my-param', 'my_value', '--local-scheduler']):
            luigi.run(main_task_cls=MyOtherTestTask)

    def _run_interface(self):
        return luigi.interface.build([self.task_a, self.task_b], worker_scheduler_factory=self.worker_scheduler_factory)
