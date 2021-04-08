# -*- coding: utf-8 -*-
#
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
#

import os
import sys
import tempfile

import boto3
from boto.s3 import key
from botocore.exceptions import ClientError
from mock import patch

from helpers import skipOnTravisAndGithubActions, unittest, with_config
from luigi.contrib.s3 import (DeprecatedBotoClientException, FileNotFoundException,
                              InvalidDeleteException, S3Client, S3Target)
from luigi.target import MissingParentDirectory
from moto import mock_s3, mock_sts
from target_test import FileSystemTargetTestMixin

import pytest

if (3, 4, 0) <= sys.version_info[:3] < (3, 4, 3):
    # spulec/moto#308
    raise unittest.SkipTest('moto mock doesn\'t work with python3.4')


AWS_ACCESS_KEY = "XXXXXXXXXXXXXXXXXXXX"
AWS_SECRET_KEY = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
AWS_SESSION_TOKEN = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"


def create_bucket():
    conn = boto3.resource('s3', region_name='us-east-1')
    # We need to create the bucket since this is all in Moto's 'virtual' AWS account
    conn.create_bucket(Bucket='mybucket')
    return conn


@pytest.mark.aws
class TestS3Target(unittest.TestCase, FileSystemTargetTestMixin):

    def setUp(self):
        f = tempfile.NamedTemporaryFile(mode='wb', delete=False)
        self.tempFileContents = (
            b"I'm a temporary file for testing\nAnd this is the second line\n"
            b"This is the third.")
        self.tempFilePath = f.name
        f.write(self.tempFileContents)
        f.close()
        self.addCleanup(os.remove, self.tempFilePath)

        self.mock_s3 = mock_s3()
        self.mock_s3.start()
        self.addCleanup(self.mock_s3.stop)

    def create_target(self, format=None, **kwargs):
        client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        create_bucket()
        return S3Target('s3://mybucket/test_file', client=client, format=format, **kwargs)

    def create_target_with_session(self, format=None, **kwargs):
        client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_SESSION_TOKEN)
        create_bucket()
        return S3Target('s3://mybucket/test_file', client=client, format=format, **kwargs)

    def test_read(self):
        client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        create_bucket()
        client.put(self.tempFilePath, 's3://mybucket/tempfile')
        t = S3Target('s3://mybucket/tempfile', client=client)
        read_file = t.open()
        file_str = read_file.read()
        self.assertEqual(self.tempFileContents, file_str.encode('utf-8'))

    def test_read_with_session(self):
        client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_SESSION_TOKEN)
        create_bucket()
        client.put(self.tempFilePath, 's3://mybucket/tempfile-with-session')
        t = S3Target('s3://mybucket/tempfile-with-session', client=client)
        read_file = t.open()
        file_str = read_file.read()
        self.assertEqual(self.tempFileContents, file_str.encode('utf-8'))

    def test_read_no_file(self):
        t = self.create_target()
        self.assertRaises(FileNotFoundException, t.open)

    def test_read_no_file_with_session(self):
        t = self.create_target_with_session()
        self.assertRaises(FileNotFoundException, t.open)

    def test_read_no_file_sse(self):
        t = self.create_target(encrypt_key=True)
        self.assertRaises(FileNotFoundException, t.open)

    def test_read_iterator_long(self):
        # write a file that is 5X the boto buffersize
        # to test line buffering
        old_buffer = key.Key.BufferSize
        key.Key.BufferSize = 2
        try:
            tempf = tempfile.NamedTemporaryFile(mode='wb', delete=False)
            temppath = tempf.name
            firstline = ''.zfill(key.Key.BufferSize * 5) + os.linesep
            secondline = 'line two' + os.linesep
            thirdline = 'line three' + os.linesep
            contents = firstline + secondline + thirdline
            tempf.write(contents.encode('utf-8'))
            tempf.close()

            client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
            create_bucket()
            remote_path = 's3://mybucket/largetempfile'
            client.put(temppath, remote_path)
            t = S3Target(remote_path, client=client)
            with t.open() as read_file:
                lines = [line for line in read_file]
        finally:
            key.Key.BufferSize = old_buffer

        self.assertEqual(3, len(lines))
        self.assertEqual(firstline, lines[0])
        self.assertEqual(secondline, lines[1])
        self.assertEqual(thirdline, lines[2])

    def test_get_path(self):
        t = self.create_target()
        path = t.path
        self.assertEqual('s3://mybucket/test_file', path)

    def test_get_path_sse(self):
        t = self.create_target(encrypt_key=True)
        path = t.path
        self.assertEqual('s3://mybucket/test_file', path)


@pytest.mark.aws
class TestS3Client(unittest.TestCase):
    def setUp(self):
        f = tempfile.NamedTemporaryFile(mode='wb', delete=False)
        self.tempFilePath = f.name
        self.tempFileContents = b"I'm a temporary file for testing\n"
        f.write(self.tempFileContents)
        f.close()
        self.addCleanup(os.remove, self.tempFilePath)

        self.mock_s3 = mock_s3()
        self.mock_s3.start()
        self.mock_sts = mock_sts()
        self.mock_sts.start()
        self.addCleanup(self.mock_s3.stop)
        self.addCleanup(self.mock_sts.stop)

    @patch('boto3.resource')
    def test_init_without_init_or_config(self, mock):
        """If no config or arn provided, boto3 client
           should be called with default parameters.
           Delegating ENV or Task Role credential handling
           to boto3 itself.
        """
        S3Client().s3
        mock.assert_called_with('s3', aws_access_key_id=None,
                                aws_secret_access_key=None, aws_session_token=None)

    @with_config({'s3': {'aws_access_key_id': 'foo', 'aws_secret_access_key': 'bar'}})
    @patch('boto3.resource')
    def test_init_with_config(self, mock):
        S3Client().s3
        mock.assert_called_with(
            's3', aws_access_key_id='foo',
            aws_secret_access_key='bar',
            aws_session_token=None)

    @patch('boto3.resource')
    @patch('boto3.client')
    @with_config({'s3': {'aws_role_arn': 'role', 'aws_role_session_name': 'name'}})
    def test_init_with_config_and_roles(self, sts_mock, s3_mock):
        S3Client().s3
        sts_mock.client.assume_role.called_with(
            RoleArn='role', RoleSessionName='name')

    @patch('boto3.client')
    def test_init_with_host_deprecated(self, mock):
        with self.assertRaises(DeprecatedBotoClientException):
            S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY, host='us-east-1').s3

    def test_put(self):
        create_bucket()
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.put(self.tempFilePath, 's3://mybucket/putMe')
        self.assertTrue(s3_client.exists('s3://mybucket/putMe'))

        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_SESSION_TOKEN)
        s3_client.put(self.tempFilePath, 's3://mybucket/putMe')
        self.assertTrue(s3_client.exists('s3://mybucket/putMe'))

    def test_put_no_such_bucket(self):
        # intentionally don't create bucket
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        with self.assertRaises(s3_client.s3.meta.client.exceptions.NoSuchBucket):
            s3_client.put(self.tempFilePath, 's3://mybucket/putMe')

    def test_put_sse_deprecated(self):
        create_bucket()
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        with self.assertRaises(DeprecatedBotoClientException):
            s3_client.put(self.tempFilePath,
                          's3://mybucket/putMe', encrypt_key=True)

    def test_put_host_deprecated(self):
        create_bucket()
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        with self.assertRaises(DeprecatedBotoClientException):
            s3_client.put(self.tempFilePath,
                          's3://mybucket/putMe', host='us-east-1')

    def test_put_string(self):
        create_bucket()
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.put_string("SOMESTRING", 's3://mybucket/putString')
        self.assertTrue(s3_client.exists('s3://mybucket/putString'))

    def test_put_string_no_such_bucket(self):
        # intentionally don't create bucket
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        with self.assertRaises(s3_client.s3.meta.client.exceptions.NoSuchBucket):
            s3_client.put_string("SOMESTRING", 's3://mybucket/putString')

    def test_put_string_sse_deprecated(self):
        create_bucket()
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        with self.assertRaises(DeprecatedBotoClientException):
            s3_client.put('SOMESTRING',
                          's3://mybucket/putMe', encrypt_key=True)

    def test_put_string_host_deprecated(self):
        create_bucket()
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        with self.assertRaises(DeprecatedBotoClientException):
            s3_client.put('SOMESTRING',
                          's3://mybucket/putMe', host='us-east-1')

    @skipOnTravisAndGithubActions("passes and fails intermitantly, suspecting it's a race condition not handled by moto")
    def test_put_multipart_multiple_parts_non_exact_fit(self):
        """
        Test a multipart put with two parts, where the parts are not exactly the split size.
        """
        # 5MB is minimum part size
        part_size = 8388608
        file_size = (part_size * 2) - 1000
        return self._run_multipart_test(part_size, file_size)

    @skipOnTravisAndGithubActions("passes and fails intermitantly, suspecting it's a race condition not handled by moto")
    def test_put_multipart_multiple_parts_exact_fit(self):
        """
        Test a multipart put with multiple parts, where the parts are exactly the split size.
        """
        # 5MB is minimum part size
        part_size = 8388608
        file_size = part_size * 2
        return self._run_multipart_test(part_size, file_size)

    def test_put_multipart_multiple_parts_with_sse_deprecated(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        with self.assertRaises(DeprecatedBotoClientException):
            s3_client.put_multipart('path', 'path', encrypt_key=True)

    def test_put_multipart_multiple_parts_with_host_deprecated(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        with self.assertRaises(DeprecatedBotoClientException):
            s3_client.put_multipart('path', 'path', host='us-east-1')

    def test_put_multipart_empty_file(self):
        """
        Test a multipart put with an empty file.
        """
        # 5MB is minimum part size
        part_size = 8388608
        file_size = 0
        return self._run_multipart_test(part_size, file_size)

    def test_put_multipart_less_than_split_size(self):
        """
        Test a multipart put with a file smaller than split size; should revert to regular put.
        """
        # 5MB is minimum part size
        part_size = 8388608
        file_size = 5000
        return self._run_multipart_test(part_size, file_size)

    def test_put_multipart_no_such_bucket(self):
        # intentionally don't create bucket
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        with self.assertRaises(s3_client.s3.meta.client.exceptions.NoSuchBucket):
            s3_client.put_multipart(self.tempFilePath, 's3://mybucket/putMe')

    def test_exists(self):
        create_bucket()
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)

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

    def test_get(self):
        create_bucket()
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.put(self.tempFilePath, 's3://mybucket/putMe')

        tmp_file = tempfile.NamedTemporaryFile(delete=True)
        tmp_file_path = tmp_file.name

        s3_client.get('s3://mybucket/putMe', tmp_file_path)
        with open(tmp_file_path, 'r') as f:
            content = f.read()
        self.assertEquals(content, self.tempFileContents.decode("utf-8"))
        tmp_file.close()

    def test_get_as_bytes(self):
        create_bucket()
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.put(self.tempFilePath, 's3://mybucket/putMe')

        contents = s3_client.get_as_bytes('s3://mybucket/putMe')

        self.assertEquals(contents, self.tempFileContents)

    def test_get_as_string(self):
        create_bucket()
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.put(self.tempFilePath, 's3://mybucket/putMe2')

        contents = s3_client.get_as_string('s3://mybucket/putMe2')

        self.assertEquals(contents, self.tempFileContents.decode('utf-8'))

    def test_get_as_string_latin1(self):
        create_bucket()
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.put(self.tempFilePath, 's3://mybucket/putMe3')

        contents = s3_client.get_as_string('s3://mybucket/putMe3', encoding='ISO-8859-1')

        self.assertEquals(contents, self.tempFileContents.decode('ISO-8859-1'))

    def test_get_key(self):
        create_bucket()
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.put(self.tempFilePath, 's3://mybucket/key_to_find')
        self.assertTrue(s3_client.get_key('s3://mybucket/key_to_find').key)
        self.assertFalse(s3_client.get_key('s3://mybucket/does_not_exist'))

    def test_isdir(self):
        create_bucket()
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        self.assertTrue(s3_client.isdir('s3://mybucket'))

        s3_client.put(self.tempFilePath, 's3://mybucket/tempdir0_$folder$')
        self.assertTrue(s3_client.isdir('s3://mybucket/tempdir0'))

        s3_client.put(self.tempFilePath, 's3://mybucket/tempdir1/')
        self.assertTrue(s3_client.isdir('s3://mybucket/tempdir1'))

        s3_client.put(self.tempFilePath, 's3://mybucket/key')
        self.assertFalse(s3_client.isdir('s3://mybucket/key'))

    def test_mkdir(self):
        create_bucket()
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        self.assertTrue(s3_client.isdir('s3://mybucket'))
        s3_client.mkdir('s3://mybucket')

        s3_client.mkdir('s3://mybucket/dir')
        self.assertTrue(s3_client.isdir('s3://mybucket/dir'))

        self.assertRaises(MissingParentDirectory,
                          s3_client.mkdir, 's3://mybucket/dir/foo/bar', parents=False)

        self.assertFalse(s3_client.isdir('s3://mybucket/dir/foo/bar'))

    def test_listdir(self):
        create_bucket()
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)

        s3_client.put_string("", 's3://mybucket/hello/frank')
        s3_client.put_string("", 's3://mybucket/hello/world')

        self.assertEqual(['s3://mybucket/hello/frank', 's3://mybucket/hello/world'],
                         list(s3_client.listdir('s3://mybucket/hello')))

    def test_list(self):
        create_bucket()
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)

        s3_client.put_string("", 's3://mybucket/hello/frank')
        s3_client.put_string("", 's3://mybucket/hello/world')

        self.assertEqual(['frank', 'world'],
                         list(s3_client.list('s3://mybucket/hello')))

    def test_listdir_key(self):
        create_bucket()
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)

        s3_client.put_string("", 's3://mybucket/hello/frank')
        s3_client.put_string("", 's3://mybucket/hello/world')

        self.assertEqual([True, True],
                         [s3_client.exists('s3://' + x.bucket_name + '/' + x.key) for x in s3_client.listdir('s3://mybucket/hello', return_key=True)])

    def test_list_key(self):
        create_bucket()
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)

        s3_client.put_string("", 's3://mybucket/hello/frank')
        s3_client.put_string("", 's3://mybucket/hello/world')

        self.assertEqual([True, True],
                         [s3_client.exists('s3://' + x.bucket_name + '/' + x.key) for x in s3_client.listdir('s3://mybucket/hello', return_key=True)])

    def test_remove_bucket_dne(self):
        create_bucket()
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)

        self.assertRaises(
            ClientError,
            lambda: s3_client.remove('s3://bucketdoesnotexist/file')
        )

    def test_remove_file_dne(self):
        create_bucket()
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)

        self.assertFalse(s3_client.remove('s3://mybucket/doesNotExist'))

    def test_remove_file(self):
        create_bucket()
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)

        s3_client.put(self.tempFilePath, 's3://mybucket/existingFile0')
        self.assertTrue(s3_client.remove('s3://mybucket/existingFile0'))
        self.assertFalse(s3_client.exists('s3://mybucket/existingFile0'))

    def test_remove_invalid(self):
        create_bucket()
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)

        self.assertRaises(
            InvalidDeleteException,
            lambda: s3_client.remove('s3://mybucket/')
        )

    def test_remove_invalid_no_slash(self):
        create_bucket()
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)

        self.assertRaises(
            InvalidDeleteException,
            lambda: s3_client.remove('s3://mybucket')
        )

    def test_remove_dir_not_recursive(self):
        create_bucket()
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)

        s3_client.put(self.tempFilePath, 's3://mybucket/removemedir/file')
        self.assertRaises(
            InvalidDeleteException,
            lambda: s3_client.remove('s3://mybucket/removemedir', recursive=False)
        )

    def test_remove_dir(self):
        create_bucket()
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)

        # test that the marker file created by Hadoop S3 Native FileSystem is removed
        s3_client.put(self.tempFilePath, 's3://mybucket/removemedir/file')
        s3_client.put_string("", 's3://mybucket/removemedir_$folder$')
        self.assertTrue(s3_client.remove('s3://mybucket/removemedir'))
        self.assertFalse(s3_client.exists('s3://mybucket/removemedir_$folder$'))

    def test_remove_dir_batch(self):
        create_bucket()
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)

        for i in range(0, 2000):
            s3_client.put(self.tempFilePath, 's3://mybucket/removemedir/file{i}'.format(i=i))
        self.assertTrue(s3_client.remove('s3://mybucket/removemedir/'))
        self.assertFalse(s3_client.exists('s3://mybucket/removedir/'))

    @skipOnTravisAndGithubActions("passes and fails intermitantly, suspecting it's a race condition not handled by moto")
    def test_copy_multiple_parts_non_exact_fit(self):
        """
        Test a multipart put with two parts, where the parts are not exactly the split size.
        """
        # First, put a file into S3
        self._run_copy_test(self.test_put_multipart_multiple_parts_non_exact_fit)

    @skipOnTravisAndGithubActions("passes and fails intermitantly, suspecting it's a race condition not handled by moto")
    def test_copy_multiple_parts_exact_fit(self):
        """
        Test a copy multiple parts, where the parts are exactly the split size.
        """
        self._run_copy_test(self.test_put_multipart_multiple_parts_exact_fit)

    def test_copy_less_than_split_size(self):
        """
        Test a copy with a file smaller than split size; should revert to regular put.
        """
        self._run_copy_test(self.test_put_multipart_less_than_split_size)

    def test_copy_empty_file(self):
        """
        Test a copy with an empty file.
        """
        self._run_copy_test(self.test_put_multipart_empty_file)

    @mock_s3
    def test_copy_empty_dir(self):
        """
        Test copying an empty dir
        """
        create_bucket()

        s3_dir = 's3://mybucket/copydir/'

        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)

        s3_client.mkdir(s3_dir)
        self.assertTrue(s3_client.exists(s3_dir))

        s3_dest = 's3://mybucket/copydir_new/'
        response = s3_client.copy(s3_dir, s3_dest)

        self._run_copy_response_test(response, expected_num=0, expected_size=0)

    @mock_s3
    @skipOnTravisAndGithubActions('https://travis-ci.org/spotify/luigi/jobs/145895385')
    def test_copy_dir(self):
        """
        Test copying 20 files from one folder to another
        """
        create_bucket()
        n = 20
        copy_part_size = (1024 ** 2) * 5

        # Note we can't test the multipart copy due to moto issue #526
        # so here I have to keep the file size smaller than the copy_part_size
        file_size = 5000

        s3_dir = 's3://mybucket/copydir/'
        file_contents = b"a" * file_size
        tmp_file = tempfile.NamedTemporaryFile(mode='wb', delete=True)
        tmp_file_path = tmp_file.name
        tmp_file.write(file_contents)
        tmp_file.flush()

        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)

        for i in range(n):
            file_path = s3_dir + str(i)
            s3_client.put_multipart(tmp_file_path, file_path)
            self.assertTrue(s3_client.exists(file_path))

        s3_dest = 's3://mybucket/copydir_new/'
        response = s3_client.copy(s3_dir, s3_dest, threads=10, part_size=copy_part_size)

        self._run_copy_response_test(response, expected_num=n, expected_size=(n * file_size))

        for i in range(n):
            original_size = s3_client.get_key(s3_dir + str(i)).size
            copy_size = s3_client.get_key(s3_dest + str(i)).size
            self.assertEqual(original_size, copy_size)

    def test__path_to_bucket_and_key(self):
        self.assertEqual(('bucket', 'key'), S3Client._path_to_bucket_and_key('s3://bucket/key'))

    def test__path_to_bucket_and_key_with_question_mark(self):
        self.assertEqual(('bucket', 'key?blade'), S3Client._path_to_bucket_and_key('s3://bucket/key?blade'))

    @mock_s3
    def _run_copy_test(self, put_method, is_multipart=False):
        create_bucket()
        # Run the method to put the file into s3 into the first place
        expected_num, expected_size = put_method()

        # As all the multipart put methods use `self._run_multipart_test`
        # we can just use this key
        original = 's3://mybucket/putMe'
        copy = 's3://mybucket/putMe_copy'

        # Copy the file from old location to new
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        if is_multipart:
            # 5MB is minimum part size, use it here so we don't have to generate huge files to test
            # the multipart upload in moto
            part_size = (1024 ** 2) * 5
            response = s3_client.copy(original, copy, part_size=part_size, threads=4)
        else:
            response = s3_client.copy(original, copy, threads=4)

        self._run_copy_response_test(response, expected_num=expected_num, expected_size=expected_size)

        # We can't use etags to compare between multipart and normal keys,
        # so we fall back to using the file size
        original_size = s3_client.get_key(original).size
        copy_size = s3_client.get_key(copy).size
        self.assertEqual(original_size, copy_size)

    @mock_s3
    def _run_multipart_test(self, part_size, file_size, **kwargs):
        create_bucket()
        file_contents = b"a" * file_size

        s3_path = 's3://mybucket/putMe'
        tmp_file = tempfile.NamedTemporaryFile(mode='wb', delete=True)
        tmp_file_path = tmp_file.name
        tmp_file.write(file_contents)
        tmp_file.flush()

        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)

        s3_client.put_multipart(tmp_file_path, s3_path,
                                part_size=part_size, **kwargs)
        self.assertTrue(s3_client.exists(s3_path))
        file_size = os.path.getsize(tmp_file.name)
        key_size = s3_client.get_key(s3_path).size
        self.assertEqual(file_size, key_size)
        tmp_file.close()

        return 1, key_size

    def _run_copy_response_test(self, response, expected_num=None, expected_size=None):
        num, size = response
        self.assertIsInstance(response, tuple)

        # only check >= minimum possible value if not provided expected value
        self.assertEqual(num, expected_num) if expected_num is not None else self.assertGreaterEqual(num, 1)
        self.assertEqual(size, expected_size) if expected_size is not None else self.assertGreaterEqual(size, 0)
