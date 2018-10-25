# -*- coding: utf-8 -*-
#
# Copyright 2018 Outlier Bio, LLC
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

import json
import luigi

from helpers import unittest

import luigi.contrib.batch as batch

import boto3
from moto import mock_batch, mock_ec2, mock_iam, mock_logs


def _setup_compute_environment(compute_environment_name):
    batch_client = boto3.client('batch')
    iam_client = boto3.client('iam')
    ec2_client = boto3.client('ec2')

    response = ec2_client.create_vpc(CidrBlock='172.30.0.0/24')
    vpc_id = response['Vpc']['VpcId']

    availability_zone = boto3.DEFAULT_SESSION.region_name + 'a'
    response = ec2_client.create_subnet(
        AvailabilityZone=availability_zone,
        CidrBlock='172.30.0.0/25',
        VpcId=vpc_id,
    )
    subnet_id = response['Subnet']['SubnetId']

    response = ec2_client.create_security_group(
        Description='test_sg_desc',
        GroupName='test_sg',
        VpcId=vpc_id,
    )
    security_group_id = response['GroupId']

    response = iam_client.create_role(
        RoleName='test_role',
        AssumeRolePolicyDocument='test_policy_document',
    )
    iam_role_arn = response['Role']['Arn']

    response = batch_client.create_compute_environment(
        computeEnvironmentName=compute_environment_name,
        type='MANAGED',
        state='ENABLED',
        computeResources={
            'type': 'EC2',
            'minvCpus': 5,
            'maxvCpus': 10,
            'desiredvCpus': 5,
            'instanceTypes': [
                'optimal',
            ],
            'imageId': 'test_image_id',
            'subnets': [
                subnet_id,
            ],
            'securityGroupIds': [
                security_group_id,
            ],
            'ec2KeyPair': 'test_key_pair',
            'instanceRole': iam_role_arn,
            'tags': {
                'key': 'value',
            },
            'bidPercentage': 123,
            'spotIamFleetRole': 'test_iam_role',
        },
        serviceRole=iam_role_arn,
    )
    compute_environment_arn = response['computeEnvironmentArn']
    return compute_environment_arn


def _setup_job_queue(job_queue_name, compute_environment_arn):
    batch_client = boto3.client('batch')
    response = batch_client.create_job_queue(
        jobQueueName=job_queue_name,
        state='ENABLED',
        priority=123,
        computeEnvironmentOrder=[
            {
                'order': 123,
                'computeEnvironment': compute_environment_arn,
            },
        ]
    )
    job_queue_arn = response['jobQueueArn']
    return job_queue_arn


def _setup_job_definition(job_definition_name):
    batch_client = boto3.client('batch')
    response = batch_client.register_job_definition(
        jobDefinitionName=job_definition_name,
        type='container',
        parameters={
            'key': 'value',
        },
        containerProperties={
            'image': 'test_image_id',
            'vcpus': 1,
            'memory': 1024,
        },
    )
    job_definition_arn = response['jobDefinitionArn']
    return job_definition_arn


def _setup_job(job_name, job_queue_arn, job_definition_arn):
    batch_client = boto3.client('batch')
    response = batch_client.submit_job(
        jobName=job_name,
        jobQueue=job_queue_arn,
        jobDefinition=job_definition_arn,
    )
    job_id = response['jobId']
    return job_id


class BatchClientTest(unittest.TestCase):

    def setUp(self):
        self.mock_batch = mock_batch()
        self.mock_ec2 = mock_ec2()
        self.mock_iam = mock_iam()
        self.mock_logs = mock_logs()
        self.mock_batch.start()
        self.mock_ec2.start()
        self.mock_iam.start()
        self.mock_logs.start()

        self.compute_environment_arn = _setup_compute_environment('test_compute_env')
        self.job_queue_arn = _setup_job_queue('test_job_queue', self.compute_environment_arn)
        self.job_definition_arn = _setup_job_definition('test_job_def')
        self.job_id = _setup_job('test_job', self.job_queue_arn, self.job_definition_arn)

        self.bc = batch.BatchClient(poll_time=10)

    def get_batch_backend(self):
        return mock_batch.backends[boto3.DEFAULT_SESSION.region_name]

    def test_get_active_queue(self):
        self.assertEqual(self.bc.get_active_queue(), 'test_job_queue')

    def test_get_job_id_from_name(self):
        job = self.get_batch_backend().get_job_by_id(self.job_id)
        job.job_state = 'RUNNING'
        self.assertEqual(self.bc.get_job_id_from_name('test_job'), self.job_id)

    def test_get_job_status(self):
        job = self.get_batch_backend().get_job_by_id(self.job_id)
        self.assertEqual(self.bc.get_job_status(self.job_id), job.job_state)

    def test_submit_job(self):
        job_id = self.bc.submit_job(
            job_definition=self.job_definition_arn,
            parameters={
                'param1': 'foo',
                'param2': 'bar',
            },
            job_name='test_job',
            queue=self.job_queue_arn,
            array_properties={
                'size': 123,
            },
            depends_on=[
                {
                    'jobId': 'string',
                    'type': 'N_TO_N',
                },
                {
                    'jobId': 'string',
                    'type': 'SEQUENTIAL',
                },
            ],
            container_overrides={
                'vcpus': 123,
                'memory': 123,
                'command': [
                    'string',
                ],
                'environment': [
                    {
                        'name': 'string',
                        'value': 'string'
                    },
                ],
            },
            retry_strategy={
                'attempts': 123,
            },
            timeout={
                'attemptDurationSeconds': 123,
            }
        )
        job = self.get_batch_backend().get_job_by_id(job_id)
        self.assertEqual(job_id, job.job_id)

    def test_wait_on_job(self):
        job = self.get_batch_backend().get_job_by_id(self.job_id)
        job.terminate('stop')
        job.job_state = 'SUCCEEDED'
        self.assertTrue(self.bc.wait_on_job(self.job_id))

    def tearDown(self):
        self.mock_batch.stop()
        self.mock_ec2.stop()
        self.mock_iam.stop()
        self.mock_logs.stop()


class BatchTaskTest(unittest.TestCase):

    def setUp(self):
        self.mock_batch = mock_batch()
        self.mock_ec2 = mock_ec2()
        self.mock_iam = mock_iam()
        self.mock_logs = mock_logs()
        self.mock_batch.start()
        self.mock_ec2.start()
        self.mock_iam.start()
        self.mock_logs.start()

        self.compute_environment_arn = _setup_compute_environment('test_compute_env')
        self.job_queue_arn = _setup_job_queue('test_job_queue', self.compute_environment_arn)
        self.job_definition_arn = _setup_job_definition('test_job_def')

        self.task = batch.BatchTask(
            job_definition=self.job_definition_arn,
            job_name='test_job',
            poll_time=1,
            queue=self.job_queue_arn,
            array_properties=json.dumps({
                'size': 123,
            }),
            depends_on=json.dumps([
                {
                    'jobId': 'string',
                    'type': 'N_TO_N',
                },
                {
                    'jobId': 'string',
                    'type': 'SEQUENTIAL',
                },
            ]),
            container_overrides=json.dumps({
                'vcpus': 123,
                'memory': 123,
                'command': [
                    'string',
                ],
                'environment': [
                    {
                        'name': 'string',
                        'value': 'string',
                    },
                ],
            }),
            retry_strategy=json.dumps({
                'attempts': 123,
            }),
            timeout=json.dumps({
                'attemptDurationSeconds': 123,
            }),
        )

    def test_submit_job(self):
        luigi.build([self.task], local_scheduler=True)

    def tearDown(self):
        self.mock_batch.stop()
        self.mock_ec2.stop()
        self.mock_iam.stop()
        self.mock_logs.stop()
