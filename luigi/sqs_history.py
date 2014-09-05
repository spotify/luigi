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

import configuration
import dateutils
import json
import logging

import task_history
from task_status import PENDING, FAILED, DONE, RUNNING
import worker_history

from boto.sqs.connection import SQSConnection
from boto.sqs.message import Message



logger = logging.getLogger('luigi-interface')

class SqsHistory(object):
    def _send_message(self, fields):
        text = json.dumps(fields)
        message = Message()
        message.set_body(text)
        self._queue.write(message)

    def _config(self, queue_name, aws_access_key_id, aws_secret_access_key):
        cx = SQSConnection(aws_access_key_id=aws_access_key_id,
                           aws_secret_access_key=aws_secret_access_key,
                           region=None,
                           is_secure=True)
        self._queue = cx.get_queue(queue_name)
        if not self._queue:
            raise Exception('Unable to create sqs queue %s' % queue_name)


class SqsTaskHistory(SqsHistory, task_history.TaskHistory):

    def __init__(self):
        self.config = configuration.get_config()
        queue_name = self.config.get('task_history', 'sqs_queue_name')
        aws_access_key_id  = self.config.get('task_history', 'aws_access_key_id')
        aws_secret_access_key = self.config.get('task_history', 'aws_secret_access_key')
        self._config(queue_name, aws_access_key_id, aws_secret_access_key)

    def task_scheduled(self, task_id, worker_id):
        task = self._get_task(task_id, status=PENDING)
        self._send_task_message(task, worker_id)

    def task_finished(self, task_id, successful, worker_id):
        status = DONE if successful else FAILED
        task = self._get_task(task_id, status=status)
        self._send_task_message(task, worker_id)

    def task_started(self, task_id, worker_host, worker_id):
        task = self._get_task(task_id, status=RUNNING, host=worker_host)
        self._send_task_message(task, worker_id)


    def _get_task(self, task_id, status, host=None):
        return task_history.Task(task_id, status, host)

    def _send_task_message(self, task, worker_id):
        self.config.reload()
        fields = {
                   'task': {
                      'task_family': task.task_family,
                      'params': task.parameters,
                      'status': task.status,
                      'host': task.host,
                   },
                  'timestamp': dateutils.utcnow().isoformat(),
                  'meta': self.config.items('meta'),
                  'worker_id': worker_id
                 }
        self._send_message(fields)



class SqsWorkerHistory(SqsHistory, worker_history.WorkerHistory):
    def __init__(self):
      self.config = configuration.get_config()
      queue_name = self.config.get('worker_history', 'sqs_queue_name')
      aws_access_key_id  = self.config.get('worker_history', 'aws_access_key_id')
      aws_secret_access_key = self.config.get('worker_history', 'aws_secret_access_key')
      self._config(queue_name, aws_access_key_id, aws_secret_access_key)

    def worker_started(self, worker_id):
        pass

    def worker_stopped(self, worker_id):
        fields = {
          'event': 'WORKER_STOPPED',
          'timestamp': dateutils.utcnow().isoformat(),
          'meta': self.config.items('meta'),
          'worker_id': worker_id
        }
        self._send_message(fields)

