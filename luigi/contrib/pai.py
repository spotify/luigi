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

"""
MicroSoft OpenPAI Job wrapper for Luigi.

  "OpenPAI is an open source platform that provides complete AI model training and resource management capabilities,
  it is easy to extend and supports on-premise, cloud and hybrid environments in various scale."

For more information about OpenPAI : https://github.com/Microsoft/pai/, this task is tested against OpenPAI 0.7.1

Requires:

- requests: ``pip install requests``

Written and maintained by Liu, Dongqing (@liudongqing).
"""
import time
import logging
import luigi
import abc

from urllib.parse import urljoin

import json

logger = logging.getLogger('luigi-interface')

try:
    import requests as rs
    from requests.exceptions import HTTPError

except ImportError:
    logger.warning('requests is not installed. PaiTask requires requests.')


def slot_to_dict(o):
    o_dict = {}
    for key in o.__slots__:
        if not key.startswith('__'):
            value = getattr(o, key, None)
            if value is not None:
                o_dict[key] = value
    return o_dict


class PaiJob:
    """
    The Open PAI job definition.
    Refer to here https://github.com/Microsoft/pai/blob/master/docs/job_tutorial.md
    ::

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
    __slots__ = (
        'jobName', 'image', 'authFile', 'dataDir', 'outputDir', 'codeDir', 'virtualCluster',
        'taskRoles', 'gpuType', 'retryCount'
    )

    def __init__(self, jobName, image, tasks):
        """
        Initialize a Job with required fields.

        :param jobName: Name for the job, need to be unique
        :param image: URL pointing to the Docker image for all tasks in the job
        :param tasks: List of taskRole, one task role at least
        """
        self.jobName = jobName
        self.image = image
        if isinstance(tasks, list) and len(tasks) != 0:
            self.taskRoles = tasks
        else:
            raise TypeError('you must specify one task at least.')


class Port:
    __slots__ = ('label', 'beginAt', 'portNumber')

    def __init__(self, label, begin_at=0, port_number=1):
        """
        The Port definition for TaskRole

        :param label: Label name for the port type, required
        :param begin_at: The port to begin with in the port type, 0 for random selection, required
        :param port_number: Number of ports for the specific type, required
        """
        self.label = label
        self.beginAt = begin_at
        self.portNumber = port_number


class TaskRole:
    __slots__ = (
        'name', 'taskNumber', 'cpuNumber', 'memoryMB', 'shmMB', 'gpuNumber', 'portList', 'command',
        'minFailedTaskCount', 'minSucceededTaskCount'
    )

    def __init__(self, name, command, taskNumber=1, cpuNumber=1, memoryMB=2048, shmMB=64, gpuNumber=0, portList=[]):
        """
        The TaskRole of PAI

        :param name: Name for the task role, need to be unique with other roles, required
        :param command: Executable command for tasks in the task role, can not be empty, required
        :param taskNumber: Number of tasks for the task role, no less than 1, required
        :param cpuNumber: CPU number for one task in the task role, no less than 1, required
        :param shmMB: Shared memory for one task in the task role, no more than memory size, required
        :param memoryMB: Memory for one task in the task role, no less than 100, required
        :param gpuNumber: GPU number for one task in the task role, no less than 0, required
        :param portList: List of portType to use, optional
        """
        self.name = name
        self.command = command
        self.taskNumber = taskNumber
        self.cpuNumber = cpuNumber
        self.memoryMB = memoryMB
        self.shmMB = shmMB
        self.gpuNumber = gpuNumber
        self.portList = portList


class OpenPai(luigi.Config):
    pai_url = luigi.Parameter(
        default='http://127.0.0.1:9186',
        description='rest server url, default is http://127.0.0.1:9186')
    username = luigi.Parameter(
        default='admin',
        description='your username')
    password = luigi.Parameter(
        default=None,
        description='your password')
    expiration = luigi.IntParameter(
        default=3600,
        description='expiration time in seconds')


class PaiTask(luigi.Task):
    __POLL_TIME = 5

    @property
    @abc.abstractmethod
    def name(self):
        """Name for the job, need to be unique, required"""
        return 'SklearnExample'

    @property
    @abc.abstractmethod
    def image(self):
        """URL pointing to the Docker image for all tasks in the job, required"""
        return 'openpai/pai.example.sklearn'

    @property
    @abc.abstractmethod
    def tasks(self):
        """List of taskRole, one task role at least, required"""
        return []

    @property
    def auth_file_path(self):
        """Docker registry authentication file existing on HDFS, optional"""
        return None

    @property
    def data_dir(self):
        """Data directory existing on HDFS, optional"""
        return None

    @property
    def code_dir(self):
        """Code directory existing on HDFS, should not contain any data and should be less than 200MB, optional"""
        return None

    @property
    def output_dir(self):
        """Output directory on HDFS, $PAI_DEFAULT_FS_URI/$jobName/output will be used if not specified, optional"""
        return '$PAI_DEFAULT_FS_URI/{0}/output'.format(self.name)

    @property
    def virtual_cluster(self):
        """The virtual cluster job runs on. If omitted, the job will run on default virtual cluster, optional"""
        return 'default'

    @property
    def gpu_type(self):
        """Specify the GPU type to be used in the tasks. If omitted, the job will run on any gpu type, optional"""
        return None

    @property
    def retry_count(self):
        """Job retry count, no less than 0, optional"""
        return 0

    def __init_token(self):
        self.__openpai = OpenPai()

        request_json = json.dumps({'username': self.__openpai.username, 'password': self.__openpai.password,
                                   'expiration': self.__openpai.expiration})
        logger.debug('Get token request {0}'.format(request_json))
        response = rs.post(urljoin(self.__openpai.pai_url, '/api/v1/token'),
                           headers={'Content-Type': 'application/json'}, data=request_json)
        logger.debug('Get token response {0}'.format(response.text))
        if response.status_code != 200:
            msg = 'Get token request failed, response is {}'.format(response.text)
            logger.error(msg)
            raise Exception(msg)
        else:
            self.__token = response.json()['token']

    def __init__(self, *args, **kwargs):
        """
        :param pai_url: The rest server url of PAI clusters, default is 'http://127.0.0.1:9186'.
        :param token: The token used to auth the rest server of PAI.
        """
        super(PaiTask, self).__init__(*args, **kwargs)
        self.__init_token()

    def __check_job_status(self):
        response = rs.get(urljoin(self.__openpai.pai_url, '/api/v1/jobs/{0}'.format(self.name)))
        logger.debug('Check job response {0}'.format(response.text))
        if response.status_code == 404:
            msg = 'Job {0} is not found'.format(self.name)
            logger.debug(msg)
            raise HTTPError(msg, response=response)
        elif response.status_code != 200:
            msg = 'Get job request failed, response is {}'.format(response.text)
            logger.error(msg)
            raise HTTPError(msg, response=response)
        job_state = response.json()['jobStatus']['state']
        if job_state in ['UNKNOWN', 'WAITING', 'RUNNING']:
            logger.debug('Job {0} is running in state {1}'.format(self.name, job_state))
            return False
        else:
            msg = 'Job {0} finished in state {1}'.format(self.name, job_state)
            logger.info(msg)
            if job_state == 'SUCCEED':
                return True
            else:
                raise RuntimeError(msg)

    def run(self):
        job = PaiJob(self.name, self.image, self.tasks)
        job.virtualCluster = self.virtual_cluster
        job.authFile = self.auth_file_path
        job.codeDir = self.code_dir
        job.dataDir = self.data_dir
        job.outputDir = self.output_dir
        job.retryCount = self.retry_count
        job.gpuType = self.gpu_type
        request_json = json.dumps(job,  default=slot_to_dict)
        logger.debug('Submit job request {0}'.format(request_json))
        response = rs.post(urljoin(self.__openpai.pai_url, '/api/v1/jobs'),
                           headers={'Content-Type': 'application/json',
                                    'Authorization': 'Bearer {}'.format(self.__token)}, data=request_json)
        logger.debug('Submit job response {0}'.format(response.text))
        # 202 is success for job submission, see https://github.com/Microsoft/pai/blob/master/docs/rest-server/API.md
        if response.status_code != 202:
            msg = 'Submit job failed, response code is {0}, body is {1}'.format(response.status_code, response.text)
            logger.error(msg)
            raise HTTPError(msg, response=response)
        while not self.__check_job_status():
            time.sleep(self.__POLL_TIME)

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(self.output())

    def complete(self):
        try:
            return self.__check_job_status()
        except HTTPError:
            return False
        except RuntimeError:
            return False
