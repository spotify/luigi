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

import abc
import traceback
import logging
import task

logger = logging.getLogger('luigi-interface')


class Task(object):
    ''' Interface for methods on TaskHistory
    '''
    def __init__(self, task_id, status, host=None):
        self.task_family, self.parameters = task.id_to_name_and_params(task_id)
        self.status = status
        self.record_id = None
        self.host = host


class TaskHistory(object):
    ''' Abstract Base Class for updating the run history of a task
    '''
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def task_scheduled(self, task_id):
        pass

    @abc.abstractmethod
    def task_finished(self, task_id, successful):
        pass

    @abc.abstractmethod
    def task_started(self, task_id, worker_host):
        pass

    # TODO(erikbern): should web method (find_latest_runs etc) be abstract?


class NopHistory(TaskHistory):
    def task_scheduled(self, task_id):
        pass

    def task_finished(self, task_id, successful):
        pass

    def task_started(self, task_id, worker_host):
        pass
