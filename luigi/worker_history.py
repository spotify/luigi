# Copyright (c) 2014 Mortar Data Inc
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


class WorkerHistory(object):
    ''' Abstract Base Class for updating the run history of a worker
    '''
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def worker_started(self, worker_id):
        pass

    @abc.abstractmethod
    def worker_stopped(self, worker_id):
        pass

    @abc.abstractmethod
    def worker_task_event(self, worker_id, event, *args, **kwargs):
        pass


class NopWorkerHistory(WorkerHistory):
    def worker_started(self, worker_id):
        pass

    def worker_stopped(self, worker_id):
        pass

    def worker_task_event(self, worker_id, event, *args, **kwargs):
        pass
