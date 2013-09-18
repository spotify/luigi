# Copyright (c) 2013 Mortar Data
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import gzip
import gc
import tempfile
import os
import unittest

from luigi import configuration
from luigi.s3 import S3Target, S3Client, InvalidDeleteException
import luigi.format

import boto
from boto.s3 import bucket
from boto.exception import S3ResponseError
from moto import mock_s3

AWS_ACCESS_KEY = "XXXXXXXXXXXXXXXXXXXX"
AWS_SECRET_KEY = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

class TestS3Target(unittest.TestCase):

    
    @mock_s3
    def test_close(self):
        client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        client.s3.create_bucket('mybucket')
        t = S3Target('s3://mybucket/test_file', client=client)
        p = t.open('w')
        print >> p, 'test'
        self.assertFalse(t.exists())
        p.close()
        self.assertTrue(t.exists())

    @mock_s3
    def test_del(self):
        client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        client.s3.create_bucket('mybucket')
        t = S3Target('s3://mybucket/test_del', client=client)
        p = t.open('w')
        print >> p, 'test'
        del p
        self.assertFalse(t.exists())
        
    @mock_s3
    def test_write_cleanup_no_close(self):
        client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        client.s3.create_bucket('mybucket')
        t = S3Target('s3://mybucket/test_cleanup', client=client)
        def context():
            f = t.open('w')
            f.write('stuff')

        context()
        gc.collect()
        self.assertFalse(t.exists())
        
    @mock_s3
    def test_write_cleanup_with_error(self):
        client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        client.s3.create_bucket('mybucket')
        t = S3Target('s3://mybucket/test_cleanup2', client=client)
        try:
            with t.open('w'):
                raise Exception('something broke')
        except:
            pass
        self.assertFalse(t.exists())

    @mock_s3
    def test_gzip(self):
        client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        client.s3.create_bucket('mybucket')
        t = S3Target('s3://mybucket/gzip_test', luigi.format.Gzip, client=client)
        p = t.open('w')
        test_data = 'test'
        p.write(test_data)
        self.assertFalse(t.exists())
        p.close()
        self.assertTrue(t.exists())

class TestS3Client(unittest.TestCase):

    def setUp(self):
        f = tempfile.NamedTemporaryFile(mode='wb', delete=False)
        self.tempFilePath = f.name
        f.write("I'm a temporary file for testing\n")
        f.close()

    def tearDown(self):
        os.remove(self.tempFilePath)

    @mock_s3
    def test_put(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.s3.create_bucket('mybucket')
        s3_client.put(self.tempFilePath, 's3://mybucket/putMe')
        self.assertTrue(s3_client.exists('s3://mybucket/putMe'))

    @mock_s3
    def test_exists(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.s3.create_bucket('mybucket')
        
        self.assertTrue(s3_client.exists('s3://mybucket/'))
        self.assertTrue(s3_client.exists('s3://mybucket'))
        self.assertFalse(s3_client.exists('s3://mybucket/nope'))
        self.assertFalse(s3_client.exists('s3://mybucket/nope/'))

        s3_client.put(self.tempFilePath, 's3://mybucket/tempfile')
        self.assertTrue(s3_client.exists('s3://mybucket/tempfile'))
        self.assertFalse(s3_client.exists('s3://mybucket/temp'))
        
        s3_client.put(self.tempFilePath, 's3://mybucket/tempdir0_$folder$')
        self.assertTrue(s3_client.exists('s3://mybucket/tempdir0'))
        
        s3_client.put(self.tempFilePath, 's3://mybucket/tempdir1/')
        self.assertTrue(s3_client.exists('s3://mybucket/tempdir1'))
        
        s3_client.put(self.tempFilePath, 's3://mybucket/tempdir2/subdir')
        self.assertTrue(s3_client.exists('s3://mybucket/tempdir2'))
        self.assertFalse(s3_client.exists('s3://mybucket/tempdir'))
    
    @mock_s3
    def test_remove(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.s3.create_bucket('mybucket')
        
        with self.assertRaises(S3ResponseError):
            s3_client.remove('s3://bucketdoesnotexist/file')
        
        self.assertFalse(s3_client.remove('s3://mybucket/doesNotExist'))
        
        s3_client.put(self.tempFilePath, 's3://mybucket/existingFile0')        
        self.assertTrue(s3_client.remove('s3://mybucket/existingFile0'))
        self.assertFalse(s3_client.exists('s3://mybucket/existingFile0'))
        
        with self.assertRaises(InvalidDeleteException):
            s3_client.remove('s3://mybucket/')
        with self.assertRaises(InvalidDeleteException):
            s3_client.remove('s3://mybucket')

        s3_client.put(self.tempFilePath, 's3://mybucket/removemedir/file')
        with self.assertRaises(InvalidDeleteException):
            s3_client.remove('s3://mybucket/removemedir', recursive=False)
        
        