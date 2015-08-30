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

import json
from helpers import unittest

import sys

import luigi
import luigi.notifications

from luigi.contrib import redshift
from moto import mock_s3
from boto.s3.key import Key
from luigi.s3 import S3Client


if (3, 4, 0) <= sys.version_info[:3] < (3, 4, 3):
    # spulec/moto#308
    mock_s3 = unittest.skip('moto mock doesn\'t work with python3.4')  # NOQA


luigi.notifications.DEBUG = True

AWS_ACCESS_KEY = 'key'
AWS_SECRET_KEY = 'secret'

BUCKET = 'bucket'
KEY = 'key'
KEY_2 = 'key2'
FILES = ['file1', 'file2', 'file3']


def generate_manifest_json(path_to_folders, file_names):
    entries = []
    for path_to_folder in path_to_folders:
        for file_name in file_names:
            entries.append({
                'url': '%s/%s' % (path_to_folder, file_name),
                'mandatory': True
            })
    return {'entries': entries}


class TestRedshiftManifestTask(unittest.TestCase):

    @mock_s3
    def test_run(self):
        client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        bucket = client.s3.create_bucket(BUCKET)
        for key in FILES:
            k = Key(bucket)
            k.key = '%s/%s' % (KEY, key)
            k.set_contents_from_string('')
        folder_path = 's3://%s/%s' % (BUCKET, KEY)
        k = Key(bucket)
        k.key = 'manifest'
        path = 's3://%s/%s/%s' % (BUCKET, k.key, 'test.manifest')
        folder_paths = [folder_path]
        t = redshift.RedshiftManifestTask(path, folder_paths)
        luigi.build([t], local_scheduler=True)

        output = t.output().open('r').read()
        expected_manifest_output = json.dumps(generate_manifest_json(folder_paths, FILES))
        self.assertEqual(output, expected_manifest_output)

    @mock_s3
    def test_run_multiple_paths(self):
        client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        bucket = client.s3.create_bucket(BUCKET)
        for parent in [KEY, KEY_2]:
            for key in FILES:
                k = Key(bucket)
                k.key = '%s/%s' % (parent, key)
                k.set_contents_from_string('')
        folder_path_1 = 's3://%s/%s' % (BUCKET, KEY)
        folder_path_2 = 's3://%s/%s' % (BUCKET, KEY_2)
        folder_paths = [folder_path_1, folder_path_2]
        k = Key(bucket)
        k.key = 'manifest'
        path = 's3://%s/%s/%s' % (BUCKET, k.key, 'test.manifest')
        t = redshift.RedshiftManifestTask(path, folder_paths)
        luigi.build([t], local_scheduler=True)

        output = t.output().open('r').read()
        expected_manifest_output = json.dumps(generate_manifest_json(folder_paths, FILES))
        self.assertEqual(output, expected_manifest_output)
