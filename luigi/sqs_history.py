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

import configuration
import dateutils
import json
import logging

from event import Event
import task_history
from task_status import PENDING, FAILED, DONE, RUNNING
from worker_status import STARTED, STOPPED
import worker_history

from boto.sqs.connection import SQSConnection
from boto.sqs.message import Message

logger = logging.getLogger('luigi-interface')

class SqsHistory(object):
    """ A History class that sends messages to an SQS (http://aws.amazon.com/sqs/) 
    queue using boto for events. 
    """

    def _send_message(self, fields):
        text = json.dumps(fields)
        message = Message()
        message.set_body(text)
        self._queue.write(message)

    def _config(self, queue_name, aws_access_key_id, aws_secret_access_key, region):
        cx = SQSConnection(aws_access_key_id=aws_access_key_id,
                           aws_secret_access_key=aws_secret_access_key,
                           region=region,
                           is_secure=True)
        self._queue = cx.get_queue(queue_name)
        if not self._queue:
            raise Exception('Unable to load sqs queue %s with access_key_id %s in region %s' \
               % (queue_name, aws_access_key_id, region))


class SqsTaskHistory(SqsHistory, task_history.TaskHistory):
    def __init__(self):
        self.config = configuration.get_config()
        queue_name = self.config.get('task_history', 'sqs_queue_name')
        aws_access_key_id  = self.config.get('task_history', 'aws_access_key_id')
        aws_secret_access_key = self.config.get('task_history', 'aws_secret_access_key')
        region = self.config.get('task_history', 'region', None)
        self._config(queue_name, aws_access_key_id, aws_secret_access_key, region)

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
        fields = {
                   'task': {
                      'task_family': task.task_family,
                      'params': task.parameters,
                      'status': task.status,
                      'host': task.host,
                   },
                  'timestamp': dateutils.utcnow().isoformat(),
                  'server_metadata': self.config.items('server_metadata'),
                  'worker_id': worker_id
                 }
        self._send_message(fields)


class SqsWorkerHistory(SqsHistory, worker_history.WorkerHistory):
    def __init__(self):
      self.config = configuration.get_config()
      queue_name = self.config.get('worker_history', 'sqs_queue_name')
      aws_access_key_id  = self.config.get('worker_history', 'aws_access_key_id')
      aws_secret_access_key = self.config.get('worker_history', 'aws_secret_access_key')
      region = self.config.get('worker_history', 'region', None)
      self._config(queue_name, aws_access_key_id, aws_secret_access_key, region)

    def worker_started(self, worker_id):
        fields = {
          'worker_status': STARTED,
          'timestamp': dateutils.utcnow().isoformat(),
          'worker_metadata': self.config.items('worker_metadata'),
          'worker_id': worker_id
        }
        self._send_message(fields)

    def worker_stopped(self, worker_id):
        fields = {
          'worker_status': STOPPED,
          'timestamp': dateutils.utcnow().isoformat(),
          'worker_metadata': self.config.items('worker_metadata'),
          'worker_id': worker_id
        }
        self._send_message(fields)

    def worker_task_event(self, worker_id, event, *args, **kwargs):
        task = args[0]
        fields = {
          'timestamp': dateutils.utcnow().isoformat(),
          'worker_metadata': self.config.items('worker_metadata'),
          'worker_id': worker_id,
          'event': event,
          'task': self._get_task_fields(task)
        }
        # Handle events with extra data.
        if event == Event.DEPENDENCY_DISCOVERED:
            fields['dependency_task'] = self._get_task_fields(args[1])
        elif event == Event.PROCESSING_TIME:
            fields['processing_time'] = args[1]
        elif event == Event.FAILURE:
            fields['exception'] = repr(args[1])
        elif event == Event.BROKEN_TASK:
            fields['exception'] = repr(args[1])
        else:
            pass

        self._send_message(fields)

    def _get_task_fields(self, task):
        return { 'id': task.task_id,
                 'task_family': task.task_family,
                 'params': task.get_params(),
               }


class SqsWorkerTaskEvents(SqsHistory, worker_history.WorkerHistory):
    def __init__(self):
      self.config = configuration.get_config()
      queue_name = self.config.get('worker_history', 'sqs_queue_name')
      aws_access_key_id  = self.config.get('worker_history', 'aws_access_key_id')
      aws_secret_access_key = self.config.get('worker_history', 'aws_secret_access_key')
      region = self.config.get('worker_history', 'region', None)
      self._config(queue_name, aws_access_key_id, aws_secret_access_key, region)

    def worker_started(self, worker_id):
        fields = {
          'worker_status': STARTED,
          'timestamp': dateutils.utcnow().isoformat(),
          'worker_metadata': self.config.items('worker_metadata'),
          'worker_id': worker_id
        }
        self._send_message(fields)

    def worker_stopped(self, worker_id):
        fields = {
          'worker_status': STOPPED,
          'timestamp': dateutils.utcnow().isoformat(),
          'worker_metadata': self.config.items('worker_metadata'),
          'worker_id': worker_id
        }
        self._send_message(fields)

