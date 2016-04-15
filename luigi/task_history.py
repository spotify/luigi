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
"""
Abstract class for task history.
Currently the only subclass is :py:class:`~luigi.db_task_history.DbTaskHistory`.
"""

import abc
import logging
import threading

from luigi import six
from luigi.task_status import PENDING, FAILED, DONE, RUNNING

logger = logging.getLogger('luigi-interface')


class StatusUpdate(object):
    ''' Interface Status updates used by background workers
    '''
    def __init__(self, task, status, host=None):
        self.task = task
        self.status = status
        self.host = host


class HistoryWorker(threading.Thread):
    def __init__(self, queue, task_history):
        threading.Thread.__init__(self)
        self._queue = queue
        self._task_history = task_history

    def run(self):
        while True:
            update = self._queue.get()
            try:
                if update.status == DONE or update.status == FAILED:
                    successful = (update.status == DONE)
                    self._task_history.task_finished(update.task, successful)
                elif update.status == PENDING:
                    self._task_history.task_scheduled(update.task)
                elif update.status == RUNNING:
                    self._task_history.task_started(update.task, update.host)
                else:
                    self._task_history.other_event(update.task, update.status)
                logger.info("Task history updated for %s with %s" % (update.task.id, update.status))
            except:
                logger.warning("Error saving Task history for %s with %s" % (update.task, update.status), exc_info=1)
            self._queue.task_done()
