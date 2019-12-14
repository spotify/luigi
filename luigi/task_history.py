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


logger = logging.getLogger('luigi-interface')


class StoredTask:
    """
    Interface for methods on TaskHistory
    """

    # TODO : do we need this task as distinct from luigi.scheduler.Task?
    #        this only records host and record_id in addition to task parameters.

    def __init__(self, task, status, host=None):
        self._task = task
        self.status = status
        self.record_id = None
        self.host = host

    @property
    def task_family(self):
        return self._task.family

    @property
    def parameters(self):
        return self._task.params


class TaskHistory(metaclass=abc.ABCMeta):
    """
    Abstract Base Class for updating the run history of a task
    """

    @abc.abstractmethod
    def task_scheduled(self, task):
        pass

    @abc.abstractmethod
    def task_finished(self, task, successful):
        pass

    @abc.abstractmethod
    def task_started(self, task, worker_host):
        pass

    # TODO(erikbern): should web method (find_latest_runs etc) be abstract?


class NopHistory(TaskHistory):

    def task_scheduled(self, task):
        pass

    def task_finished(self, task, successful):
        pass

    def task_started(self, task, worker_host):
        pass
