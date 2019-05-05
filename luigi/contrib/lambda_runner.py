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
"""
AWS Lambda Runner for Luigi
===========================

Lambda is a serverless compute service from Amazon Web Services (AWS) that
facilitates stateless, scalable code. These functions can be written in Python
or a variety of other supported languages, and are easily interfaced with
other AWS services.

This module requires the `boto3` package to be installed.
You must also have AWS credentials set up in way boto3 can detect them. See:
https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html

`LambdaRunnerTask` invokes a pre-existing Lambda function given a name/ARN.
"""

import logging
import luigi
import json
import os
import uuid

logger = logging.getLogger('luigi-interface')

try:
    import boto3
    client = boto3.client('lambda')
                          # aws_access_key_id=os.environ['X'],
                          # aws_secret_access_key=os.environ['TEST_AWS_SECRET'])
except ImportError:
    logger.warning('boto3 is required for lambda_runner but not installed')


class LambdaRunnerTask(luigi.Task):
    """
    Base Task for invoking AWS Lambda functions. Must provide a Lambda name or
    ARN. Optionally, specify input payload to the Lambda, which is passed as
    event JSON to the invoked function. If no payload is specified, use an
    empty JSON object. Payload can be a Python dictionary or filepath; either
    way, it must be JSON seriablizable.

    Writes the Lambda response payload as a plaintext JSON file using a
    randomly generated uuid associated with the task. Stores the payload as the
    `response_payload` field and other of the response metadata as `response`.

    :param str lambda_name: Name or ARN of AWS Lambda function
    :param str output_directory: Directory to write output to. Default is '.'
    :param bool dry_run:
        If True, invoke the Lambda function in dry_run mode, which will result
        in an empty string payload written to output. Default is False
    :param payload:
        Object contents to pass as event JSON for the Lambda.
        Cannot specify `filepath_payload` if using this. Default is None
    :type payload: dict or None
    :param filepath_payload:
        Path for plaintext JSON file to use as event JSON for the Lambda.
        Cannot specify `payload` if using this. Default is None
    :type filepath_payload: str or None

    :raises TypeError: if specified payload is not JSON serializable
    """
    # Parameters
    lambda_name = luigi.Parameter()
    output_directory = luigi.Parameter(default='.')
    dry_run = luigi.BoolParameter(default=False)
    payload = luigi.OptionalParameter(default=None)
    filepath_payload = luigi.OptionalParameter(default=None)
    # Fields set by the task
    response = None
    response_payload = None
    run_uuid = str(uuid.uuid4())

    def make_output_filepath(self):
        """
        Make output filepath based on the Lambda function name, self.run_uuid,
        and self.output_directory.
        """
        # Get filename from lambda name. Handle ARN, if provided
        if 'arn:aws:' in self.lambda_name:
            name = self.lambda_name.split(':function:')[-1]
        else:
            name = self.lambda_name
        filename = '{name}-{uuid}.json'.format(name=name, uuid=self.run_uuid)
        return os.path.join(self.output_directory, filename)

    def output(self):
        return luigi.LocalTarget(self.make_output_filepath())

    def run(self):
        """
        Determine the payload for the Lambda and ensure it is JSON serializable.
        Then synchronous invoke the Lambda and write the response payload as
        plain JSON to the task output. If self.dry_run is True, the payload
        will be an empty string.
        """
        payload = None
        if self.payload and self.filepath_payload:
            raise ValueError('Cannot define both payload and filepath_payload.')
        elif self.payload:
            try:
                payload = json.dumps(self.payload)
            except TypeError as exc:
                raise TypeError('"payload" must be JSON serializable. Error %s' % exc)
        elif self.filepath_payload:
            with open(self.filepath_payload) as in_file:
                try:
                    payload = json.dumps(json.loads(in_file.read()))
                except TypeError as exc:
                    raise TypeError('"filepath_payload" %s contents must be '
                                    'JSON serializable. Error %s'
                                    % (self.filepath_payload, exc))
        else:
            payload = json.dumps({})  # No payload provided; use empty object

        invocation_type = 'DryRun' if self.dry_run is True else 'RequestResponse'
        self.response = client.invoke(
            FunctionName=self.lambda_name,
            InvocationType=invocation_type,
            Payload=payload
        )

        # Response Payload is a StreamingBody. Read and decode, then write to
        # output file as plain JSON
        resp_streaming_body = self.response.pop('Payload')
        self.response_payload = resp_streaming_body.read().decode()
        with self.output().open('w') as out_file:
            out_file.write(self.response_payload)
