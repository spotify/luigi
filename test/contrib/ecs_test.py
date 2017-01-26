# -*- coding: utf-8 -*-
#
# Copyright 2015 Outlier Bio, LLC
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
Integration test for the Luigi wrapper of EC2 Container Service (ECSTask)

Requires:

- boto3 package
- Amazon AWS credentials discoverable by boto3 (e.g., by using ``aws configure``
from awscli_)
- A running ECS cluster (see `ECS Get Started`_)

Written and maintained by Jake Feala (@jfeala) for Outlier Bio (@outlierbio)

.. _awscli: https://aws.amazon.com/cli
.. _`ECS Get Started`: http://docs.aws.amazon.com/AmazonECS/latest/developerguide/ECS_GetStarted.html
"""

import unittest

import luigi
from luigi.contrib.ecs import ECSTask, _get_task_statuses

use_moto = False

try:
    import boto3
    try:
        import botocore.exceptions
        client = boto3.client('ecs')
    except botocore.exceptions.NoRegionError:
        # the guess is that this is a Travis testing scenario, where
        # AWS is not actually available. Therefore, try setting up mocks instead.
        try:
            from moto import mock_ecs
            use_moto = True
        except ImportError:
            raise unittest.SkipTest('moto is not installed, and AWS config was unavailable. Skipping ECSTask tests.')
except ImportError:
    raise unittest.SkipTest('boto3 is not installed. ECSTasks require boto3')


TEST_TASK_DEF = {
    'family': 'hello-world',
    'volumes': [],
    'containerDefinitions': [
        {
            'memory': 1,
            'essential': True,
            'name': 'hello-world',
            'image': 'ubuntu',
            'command': ['/bin/echo', 'hello world']
        }
    ]
}


class ECSTaskNoOutput(ECSTask):

    def complete(self):
        if self.ecs_task_ids:
            return all([status == 'STOPPED'
                        for status in _get_task_statuses(self.ecs_task_ids)])
        return False


class ECSTaskOverrideCommand(ECSTaskNoOutput):

    @property
    def command(self):
        return [{'name': 'hello-world', 'command': ['/bin/sleep', '10']}]


class TestECSTask(unittest.TestCase):

    def setUp(self):
        if use_moto:
            self.mock_ecs = mock_ecs()
            self.mock_ecs.start()
            self.addCleanup(self.mock_ecs.stop)
            self.client = boto3.client('ecs', region_name='us-east-1', aws_access_key_id='XXXXXXXXXXXX', aws_secret_access_key='XXXXXXXXXXXXX')
            import luigi.contrib.ecs
            luigi.contrib.ecs.client = self.client  # necessary because contrib/ecs.py uses a module-level client.
        else:
            self.client = client
        # Register the test task definition
        response = self.client.register_task_definition(**TEST_TASK_DEF)
        self.arn = response['taskDefinition']['taskDefinitionArn']

    def test_unregistered_task(self):
        t = ECSTaskNoOutput(task_def=TEST_TASK_DEF)
        luigi.build([t], local_scheduler=True)

    def test_registered_task(self):
        t = ECSTaskNoOutput(task_def_arn=self.arn)
        luigi.build([t], local_scheduler=True)

    def test_override_command(self):
        t = ECSTaskOverrideCommand(task_def_arn=self.arn)
        luigi.build([t], local_scheduler=True)
