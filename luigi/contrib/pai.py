# -*- coding: utf-8 -*-
#
# Copyright 2017 Open Targets
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

import logging
import luigi

try:
    from urlparse import urljoin
except ImportError:
    from urllib.parse import urljoin


from luigi import six
import json

logger = logging.getLogger('luigi-interface')

try:
    import requests as rs

except ImportError:
    logger.warning('requests is not installed. PaiTask requires requests.')

"""
The Open PAI job definition. 
Refer to here https://github.com/Microsoft/pai/blob/master/docs/job_tutorial.md

{
  "jobName":   String,
  "image":     String,
  "authFile":  String,
  "dataDir":   String,
  "outputDir": String,
  "codeDir":   String,
  "virtualCluster": String,
  "taskRoles": [
    {
      "name":       String,
      "taskNumber": Integer,
      "cpuNumber":  Integer,
      "memoryMB":   Integer,
      "shmMB":      Integer,
      "gpuNumber":  Integer,
      "portList": [
        {
          "label": String,
          "beginAt": Integer,
          "portNumber": Integer
        }
      ],
      "command":    String,
      "minFailedTaskCount": Integer,
      "minSucceededTaskCount": Integer
    }
  ],
  "gpuType": String,
  "retryCount": Integer
}

"""


def default(o):
    return o.__dict__

class PaiJob(object):
    # __slots__ = ['jobName', 'image', 'authFile', 'dataDir', 'outputDir', 'codeDir', 'virtualCluster', 'taskRoles',
    #              'gpuType', 'retryCount']

    def __init__(self, jobName, image, tasks):
        self.jobName = jobName
        self.image = image
        if isinstance(tasks, list) and len(tasks) != 0:
            self.taskRoles = tasks
        else:
            raise TypeError('you must specify one task at least.')


class Port(object):
    # __slots__ = ['label', 'beginAt', 'portNumber']

    def __init__(self, label, begin_at=0, port_number=1):
        self.label = label
        self.beginAt = begin_at
        self.portNumber = port_number


class TaskRole(object):
    # __slots__ = ['name', 'taskNumber', 'cpuNumber', 'memoryMB', 'shmMB', 'gpuNumber', 'portList', 'command',
    #              'minFailedTaskCount', 'minSucceededTaskCount']

    def __init__(self, name, command, taskNumber=1, cpuNumber=1, memoryMB=100, gpuNumber=0):
        self.name = name
        self.command = command
        self.taskNumber = taskNumber
        self.cpuNumber = cpuNumber
        self.memoryMB = memoryMB
        self.gpuNumber = gpuNumber


class PaiTask(luigi.Task):
    # __slots__ = ['name', 'image', 'command', 'tasks', 'auth_file_path', 'data_dir', 'code_dir', 'output_dir',
    #              'virtual_cluster', 'gpu_type', 'retry_count']
    @property
    def name(self):
        return 'pai_job'

    @property
    def image(self):
        return 'alpine'

    @property
    def command(self):
        return "echo hello world"

    @property
    def tasks(self):
        return []

    @property
    def auth_file_path(self):
        return None

    @property
    def data_dir(self):
        return None

    @property
    def code_dir(self):
        return None

    @property
    def output_dir(self):
        return '$PAI_DEFAULT_FS_URI/{0}/output'.format(self.name)

    @property
    def virtual_cluster(self):
        return 'default'

    @property
    def gpu_type(self):
        return None

    @property
    def retry_count(self):
        return 0

    @property
    def pai_url(self):
        return 'http://127.0.0.1:9186'

    def __init__(self, *args, **kwargs):
        super(PaiTask, self).__init__(*args, **kwargs)
        self.__logger = logger

        request_json = json.dumps({'username': 'admin', 'password': 'admin-password', 'expiration': '3600'})
        print(request_json)
        response = rs.post(urljoin(self.pai_url, '/api/v1/token'),
                           headers={'Content-Type': 'application/json'}, data=request_json)
        print(response.json())
        self._token = response.json()

    def run(self):
        print(self.tasks)
        job = PaiJob(self.name, self.image, self.tasks)
        request_json = json.dumps(job,  default=default)
        print(request_json)
        response = rs.post(urljoin(self.pai_url, '/api/v1/jobs'),
                           headers={'Content-Type': 'application/json',
                                    'Authorization': 'Bearer {}'.format(self._token['token'])}, data=request_json)
        print(response.json())

    def complete(self):
        response = rs.get(urljoin(self.pai_url, '/api/v1/jobs/{0}'.format(self.name)))
        print(response.json())
        print(response.status_code)
        if response.status_code == 404:
            return False
        job_state = response.json()
        if job_state['jobStatus']['state'] != 'RUNNING':
            return True
        else:
            return False
