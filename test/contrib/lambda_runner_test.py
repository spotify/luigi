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
import unittest
import luigi
import subprocess
import zipfile
import os
import io
import json
from luigi.contrib.lambda_runner import LambdaRunnerTask
from moto import mock_lambda, mock_s3
from tempfile import mkdtemp
from nose.plugins.attrib import attr

try:
    import boto3
    boto3.setup_default_session()
    client = boto3.client('lambda')
except ImportError:
    raise unittest.SkipTest('boto3 is required for lambda_runner_test but not installed')

# trying to get config working
os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
os.environ['AWS_SECURITY_TOKEN'] = 'testing'
os.environ['AWS_SESSION_TOKEN'] = 'testing'
boto3.setup_default_session()
client = boto3.client('lambda')

# must have docker running for moto.mock_lambda
try:
    subprocess.check_output('docker info', shell=True)
except subprocess.subprocess.CalledProcessError:
    raise unittest.SkipTest('docker must be running for lambda_runner_test')

# _process_lambda and get_test_zip_file are adapted from
# spulec/moto tests/test_awslambda/test_lambda.py
def _process_lambda(func_str):
    """
    Zip input function string and return read value
    """
    zip_output = io.BytesIO()
    zip_file = zipfile.ZipFile(zip_output, 'w', zipfile.ZIP_DEFLATED)
    zip_file.writestr('lambda_function.py', func_str)
    zip_file.close()
    zip_output.seek(0)
    return zip_output.read()


def get_test_zip_file():
    pfunc = (
        """
        def lambda_handler(event, context):
        return event
        """
    )
    return _process_lambda(pfunc)


@mock_lambda
def test_invoke_requestresponse_function():


    in_data = {'msg': 'So long and thanks for all the fish'}
    success_result = conn.invoke(FunctionName='testFunction', InvocationType='RequestResponse',
                                 Payload=json.dumps(in_data))

    success_result["StatusCode"].should.equal(202)
    result_obj = json.loads(
        base64.b64decode(success_result["LogResult"]).decode('utf-8'))

    result_obj.should.equal(in_data)

    payload = success_result["Payload"].read().decode('utf-8')
    json.loads(payload).should.equal(in_data)


@attr('aws')
class TestLambdaRunnerTask(unittest.TestCase):
    setup_res = None
    tmp_dir = mkdtemp()

    @mock_lambda()
    def setUp(self):
        # Create a Lambda function, mocked by moto
        self.setup_res = client.create_function(
            FunctionName='test-lambda',
            Runtime='python3.6',
            Role='test-iam-role',
            Handler='lambda_function.lambda_handler',
            Code={
                'ZipFile': get_test_zip_file(),
            },
            Description='test lambda function',
            Timeout=3,
            MemorySize=128,
            Publish=True,
        )

    def tearDown(self):
        os.removedirs(self.tmp_dir)

    @mock_lambda()
    def test_task_lambda_no_payload(self):
        client.invoke(
            FunctionName=self.setup_res['FunctionArn'],
            InvocationType='RequestResponse',
            Payload=json.dumps({})
        )
        test_task = LambdaRunnerTask(lambda_name=self.setup_res['FunctionArn'],
                                     output_directory=self.tmp_dir)
        luigi.build([test_task], local_scheduler=True)
        out_fp = test_task.make_output_filepath()
        self.assertTrue(os.path.exists(out_fp))
        with open(out_fp) as f:
            out_json = json.loads(f.read())
        self.assertEqual(out_json, {})

    @mock_lambda()
    def test_task_lambda_dry_run(self):
        test_task = LambdaRunnerTask(lambda_name=self.setup_res['FunctionArn'],
                                     output_directory=self.tmp_dir,
                                     dry_run=True)
        luigi.build([test_task], local_scheduler=True)
        out_fp = test_task.make_output_filepath()
        self.assertTrue(os.path.exists(out_fp))
        with open(out_fp) as f:
            out_json = json.loads(f.read())
        # Setting dry_run results in an empty payload
        self.assertEqual(out_json, '')

    @mock_lambda()
    def test_task_lambda_dict_payload(self):
        test_task = LambdaRunnerTask(lambda_name=self.setup_res['FunctionArn'],
                                     output_directory=self.tmp_dir,
                                     payload={'abc': 123})
        luigi.build([test_task], local_scheduler=True)
        out_fp = test_task.make_output_filepath()
        self.assertTrue(os.path.exists(out_fp))
        with open(out_fp) as f:
            out_json = json.loads(f.read())
        self.assertEqual(out_json, {'abc': 123})
