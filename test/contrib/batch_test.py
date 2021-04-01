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

from helpers import unittest

import luigi.contrib.batch as batch
from helpers import skipOnTravisAndGithubActions

import pytest

try:
    import boto3
    client = boto3.client('batch')
except ImportError:
    raise unittest.SkipTest('boto3 is not installed. BatchTasks require boto3')


class MockBotoBatchClient:

    def describe_job_queues(self):
        return {
            'jobQueues': [
                {
                    'jobQueueName': 'test_queue',
                    'state': 'ENABLED',
                    'status': 'VALID'
                }
            ]
        }

    def list_jobs(self, jobQueue='', jobStatus=''):
        return {
            'jobSummaryList': [
                {
                    'jobName': 'test_job',
                    'jobId': 'abcd'
                }
            ]
        }

    def describe_jobs(self, jobs=[]):
        return {
            'ResponseMetadata': {
                'HTTPStatusCode': 200
            },
            'jobs': [
                {
                    'status': 'SUCCEEDED',
                    'attempts': [
                        {
                            'container': {
                                'logStreamName': 'test_job_abcd_log_stream'
                            }
                        }
                    ]
                }
            ]
        }

    def submit_job(self, jobDefinition='', jobName='', jobQueue='', parameters={}):
        return {'jobId': 'abcd'}

    def register_job_definition(self, **kwargs):
        return {
            'ResponseMetadata': {
                'HTTPStatusCode': 200
            }
        }


class MockBotoLogsClient:

    def get_log_events(self, logGroupName='', logStreamName='', startFromHead=True):
        return {
            'events': [
                {
                    'message': 'log line 1'
                },
                {
                    'message': 'log line 2'
                },
                {
                    'message': 'log line 3'
                }
            ]
        }


@pytest.mark.aws
@skipOnTravisAndGithubActions("boto3 now importable. These tests need mocked")
class BatchClientTest(unittest.TestCase):

    def setUp(self):
        self.bc = batch.BatchClient(poll_time=10)
        self.bc._client = MockBotoBatchClient()
        self.bc._log_client = MockBotoLogsClient()

    def test_get_active_queue(self):
        self.assertEqual(self.bc.get_active_queue(), 'test_queue')

    def test_get_job_id_from_name(self):
        self.assertEqual(self.bc.get_job_id_from_name('test_job'), 'abcd')

    def test_get_job_status(self):
        self.assertEqual(self.bc.get_job_status('abcd'), 'SUCCEEDED')

    def test_get_logs(self):
        log_str = 'log line 1\nlog line 2\nlog line 3'
        self.assertEqual(self.bc.get_logs('test_job_abcd_log_stream'), log_str)

    def test_submit_job(self):
        job_id = self.bc.submit_job(
            'test_job_def',
            {'param1': 'foo', 'param2': 'bar'},
            job_name='test_job')
        self.assertEqual(job_id, 'abcd')

    def test_submit_job_specific_queue(self):
        job_id = self.bc.submit_job(
            'test_job_def',
            {'param1': 'foo', 'param2': 'bar'},
            job_name='test_job',
            queue='test_queue')
        self.assertEqual(job_id, 'abcd')

    def test_submit_job_non_existant_queue(self):
        with self.assertRaises(Exception):
            self.bc.submit_job(
                'test_job_def',
                {'param1': 'foo', 'param2': 'bar'},
                job_name='test_job',
                queue='non_existant_queue')

    def test_wait_on_job(self):
        job_id = self.bc.submit_job(
            'test_job_def',
            {'param1': 'foo', 'param2': 'bar'},
            job_name='test_job')
        self.assertTrue(self.bc.wait_on_job(job_id))

    def test_wait_on_job_failed(self):
        job_id = self.bc.submit_job(
            'test_job_def',
            {'param1': 'foo', 'param2': 'bar'},
            job_name='test_job')
        self.bc.get_job_status = lambda x: 'FAILED'
        with self.assertRaises(batch.BatchJobException) as context:
            self.bc.wait_on_job(job_id)
            self.assertTrue('log line 1' in context.exception)


@pytest.mark.aws
@skipOnTravisAndGithubActions("boto3 now importable. These tests need mocked")
class BatchTaskTest(unittest.TestCase):

    def setUp(self):
        self.task = batch.BatchTask(
            job_definition='test_job_def',
            job_name='test_job',
            poll_time=10)
