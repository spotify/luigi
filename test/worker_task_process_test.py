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

from helpers import LuigiTestCase, temporary_unloaded_module
import luigi
from luigi.worker import Worker
import multiprocessing


class ContextManagedTaskProcessTest(LuigiTestCase):

    def _test_context_manager(self, force_multiprocessing):
        CONTEXT_MANAGER_MODULE = b'''
class MyContextManager:
    def __init__(self, task_process):
        self.task = task_process.task
    def __enter__(self):
        assert not self.task.run_event.is_set(), "the task should not have run yet"
        self.task.enter_event.set()
        return self
    def __exit__(self, exc_type=None, exc_value=None, traceback=None):
        assert self.task.run_event.is_set(), "the task should have run"
        self.task.exit_event.set()
'''

        class DummyEventRecordingTask(luigi.Task):
            def __init__(self, *args, **kwargs):
                self.enter_event = multiprocessing.Event()
                self.exit_event = multiprocessing.Event()
                self.run_event = multiprocessing.Event()
                super(DummyEventRecordingTask, self).__init__(*args, **kwargs)

            def run(self):
                assert self.enter_event.is_set(), "the context manager should have been entered"
                assert not self.exit_event.is_set(), "the context manager should not have been exited yet"
                assert not self.run_event.is_set(), "the task should not have run yet"
                self.run_event.set()

            def complete(self):
                return self.run_event.is_set()

        with temporary_unloaded_module(CONTEXT_MANAGER_MODULE) as module_name:
            t = DummyEventRecordingTask()
            w = Worker(task_process_context=module_name + '.MyContextManager',
                       force_multiprocessing=force_multiprocessing)
            w.add(t)
            self.assertTrue(w.run())
            self.assertTrue(t.complete())
            self.assertTrue(t.enter_event.is_set())
            self.assertTrue(t.exit_event.is_set())

    def test_context_manager_without_multiprocessing(self):
        self._test_context_manager(False)

    def test_context_manager_with_multiprocessing(self):
        self._test_context_manager(True)
