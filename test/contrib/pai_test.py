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
Tests for OpenPAI wrapper for Luigi.


Written and maintained by Liu, Dongqing (@liudongqing).
"""
from helpers import unittest
import responses

import time
import luigi
import logging
from luigi.contrib.pai import PaiTask
from luigi.contrib.pai import TaskRole

logging.basicConfig(level=logging.DEBUG)

"""
The following configurations are required to run the test
[OpenPai]
pai_url:http://host:port/
username:admin
password:admin-password
expiration:3600

"""


class SklearnJob(PaiTask):
    image = "openpai/pai.example.sklearn"
    name = "test_job_sk_{0}".format(time.time())
    command = 'cd scikit-learn/benchmarks && python bench_mnist.py'
    virtual_cluster = 'spark'
    tasks = [TaskRole('test', 'cd scikit-learn/benchmarks && python bench_mnist.py', memoryMB=4096)]


class TestPaiTask(unittest.TestCase):

    @responses.activate
    def test_success(self):
        """
        Here using the responses lib to mock the PAI rest api call, the following specify the response of the call.
        """
        responses.add(responses.POST, 'http://127.0.0.1:9186/api/v1/token',
                      json={"token": "test", "user": "admin", "admin": True}, status=200)
        sk_task = SklearnJob()

        responses.add(responses.POST, 'http://127.0.0.1:9186/api/v1/jobs',
                      json={"message": "update job {0} successfully".format(sk_task.name)}, status=202)

        responses.add(responses.GET, 'http://127.0.0.1:9186/api/v1/jobs/{0}'.format(sk_task.name),
                      json={}, status=404)

        responses.add(responses.GET, 'http://127.0.0.1:9186/api/v1/jobs/{0}'.format(sk_task.name),
                      body='{"jobStatus": {"state":"SUCCEED"}}', status=200)

        success = luigi.build([sk_task], local_scheduler=True)
        self.assertTrue(success)
        self.assertTrue(sk_task.complete())

    @responses.activate
    def test_fail(self):
        """
        Here using the responses lib to mock the PAI rest api call, the following specify the response of the call.
        """
        responses.add(responses.POST, 'http://127.0.0.1:9186/api/v1/token',
                      json={"token": "test", "user": "admin", "admin": True}, status=200)
        fail_task = SklearnJob()

        responses.add(responses.POST, 'http://127.0.0.1:9186/api/v1/jobs',
                      json={"message": "update job {0} successfully".format(fail_task.name)}, status=202)

        responses.add(responses.GET, 'http://127.0.0.1:9186/api/v1/jobs/{0}'.format(fail_task.name),
                      json={}, status=404)

        responses.add(responses.GET, 'http://127.0.0.1:9186/api/v1/jobs/{0}'.format(fail_task.name),
                      body='{"jobStatus": {"state":"FAILED"}}', status=200)

        success = luigi.build([fail_task], local_scheduler=True)
        self.assertFalse(success)
        self.assertFalse(fail_task.complete())
