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
Tests for Docker container wrapper for Luigi.


Requires:

- docker: ``pip install docker``

Written and maintained by Andrea Pierleoni (@apierleoni).
Contributions by Eliseo Papa (@elipapa)
"""
from helpers import unittest

import luigi
import logging
from luigi.contrib.pai import PaiTask
from luigi.contrib.pai import TaskRole

logging.basicConfig(level=logging.DEBUG)



class SuccessJob(PaiTask):
    image = "busybox:latest"
    name = "test_job1"
    command = 'echo hello world'
    virtual_cluster = 'spark'
    tasks = [TaskRole('test', 'echo hello')]


class TestPaiTask(unittest.TestCase):
    def test_run(self):

        success = SuccessJob()
        luigi.build([success], local_scheduler=True)
        self.assertTrue(success)