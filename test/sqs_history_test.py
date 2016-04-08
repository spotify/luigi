# Copyright (c) 2014 Mortar Data, Inc
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

import luigi
from luigi.task_status import PENDING, RUNNING, DONE
from luigi.worker_status import STARTED, STOPPED


try:
    from luigi.sqs_history import SqsHistory, SqsTaskHistory, SqsWorkerHistory
except ImportError as e:
    raise unittest.SkipTest('Could not test sqs_history: %s' % e)

from datetime import datetime
import json
import helpers
import unittest


class DummyQueue():
    """
    Mock Queue for Testing.
    """
    def __init__(self):
        self.messages = []

    def write(self, message):
        self.messages.append(message)


class ParamTask(luigi.Task):
    param1 = luigi.Parameter()
    param2 = luigi.IntParameter()


class ListDateParamTask(luigi.Task):
    param1 = luigi.Parameter()
    param2 = luigi.DateSecondParameter(default=datetime.now())
    param3 = luigi.Parameter(default=['something'])


class SqsTaskHistoryTest(unittest.TestCase):
    def setUp(self):
        # Replace _config method with one that uses our dummy queue.
        def fake_config(s, *args):
            s._queue = DummyQueue()
        SqsHistory._config = fake_config

    @helpers.with_config(dict(task_history=dict(sqs_queue_name='name', aws_access_key_id='key', aws_secret_access_key='secret_key'),
                              server_metadata=dict(meta1='data1')))
    def test_task_events(self):
        self.history = SqsTaskHistory()
        self.run_task(ParamTask("foo", "bar"), "worker-id")

        sent_messages = [json.loads(m.get_body()) for m in self.history._queue.messages]

        # Check pending:
        pending_message_fields = sent_messages[0]

        task = pending_message_fields.get("task")
        self.assertEquals(PENDING, task.get("status"))

        params = task.get("params")
        self.assertEquals("foo", params.get("param1"))
        self.assertEquals("bar", params.get("param2"))

        meta = pending_message_fields.get("server_metadata")
        self.assertEquals("data1", meta[0][1])

        # Check Running
        running_message_fields = sent_messages[1]

        task = running_message_fields.get("task")
        self.assertEquals(RUNNING, task.get("status"))

        # Check Done
        done_message_fields = sent_messages[2]
        task = done_message_fields.get("task")
        self.assertEquals(DONE, task.get("status"))

    @helpers.with_config(dict(task_history=dict(sqs_queue_name='name', aws_access_key_id='key', aws_secret_access_key='secret_key'),
                              server_metadata=dict(meta1='data1')))
    def test_task_events_list_date_params(self):
        self.history = SqsTaskHistory()
        curr_time = datetime.now()
        curr_time_str = curr_time.strftime(luigi.DateSecondParameter.date_format)
        t = ListDateParamTask("foo", param2=curr_time)
        self.run_task(t, "worker-id")
        sent_messages = [json.loads(m.get_body()) for m in self.history._queue.messages]

        # Check pending:
        pending_message_fields = sent_messages[0]

        task = pending_message_fields.get("task")
        self.assertEquals(PENDING, task.get("status"))

        params = task.get("params")
        self.assertEquals("foo", params.get("param1"))
        self.assertEquals(curr_time_str, params.get("param2"))
        self.assertEquals([u"'something'"], params.get("param3"))

        meta = pending_message_fields.get("server_metadata")
        self.assertEquals("data1", meta[0][1])

        # Check Running
        running_message_fields = sent_messages[1]

        task = running_message_fields.get("task")
        self.assertEquals(RUNNING, task.get("status"))

        # Check Done
        done_message_fields = sent_messages[2]
        task = done_message_fields.get("task")
        self.assertEquals(DONE, task.get("status"))

    @helpers.with_config(dict(task_history=dict(sqs_queue_name='name', aws_access_key_id='key', aws_secret_access_key='secret_key')))
    def test_task_events_no_metadata(self):
        self.history = SqsTaskHistory()
        self.run_task(ParamTask("foo", "bar"), "worker-id")

        sent_messages = [json.loads(m.get_body()) for m in self.history._queue.messages]
        self.assertEquals([], sent_messages[0].get("server_metadata"))

    def run_task(self, task, worker_id):
        self.history.task_scheduled(task.task_id, worker_id)
        self.history.task_started(task.task_id, 'hostname', worker_id)
        self.history.task_finished(task.task_id, successful=True, worker_id=worker_id)


class SqsWorkerHistoryTest(unittest.TestCase):
    @helpers.with_config(dict(worker_history=dict(sqs_queue_name='name', aws_access_key_id='key', aws_secret_access_key='secret_key'),
                              worker_metadata=dict(meta1='data1')))
    def setUp(self):
        # Replace _config method with one that uses our dummy queue.
        def fake_config(s, *args):
            s._queue = DummyQueue()
        SqsHistory._config = fake_config
        self.history = SqsWorkerHistory()

    def test_worker_events(self):
        self.run_worker("worker-id")

        sent_messages = [json.loads(m.get_body()) for m in self.history._queue.messages]

        # Check started:
        started_message_fields = sent_messages[0]
        self.assertEquals(STARTED, started_message_fields.get("worker_status"))
        self.assertEquals("worker-id", started_message_fields.get("worker_id"))
        meta = started_message_fields.get("worker_metadata")
        self.assertEquals("data1", meta[0][1])

        # Check Stoped
        stopped_message_fields = sent_messages[1]
        self.assertEquals(STOPPED, stopped_message_fields.get("worker_status"))

    def run_worker(self, worker_id):
        self.history.worker_started(worker_id)
        self.history.worker_stopped(worker_id)
