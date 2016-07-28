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
from __future__ import print_function

import os
import sys
import tempfile

from target_test import FileSystemTargetTestMixin
from helpers import with_config, unittest, skipOnTravis

from boto.exception import S3ResponseError
from boto.s3 import key
from moto import mock_s3
from moto import mock_sts

from luigi import configuration
from luigi.s3 import FileNotFoundException, InvalidDeleteException, S3Client, S3Target
from luigi.target import MissingParentDirectory

if (3, 4, 0) <= sys.version_info[:3] < (3, 4, 3):
    # spulec/moto#308
    raise unittest.SkipTest('moto mock doesn\'t work with python3.4')


AWS_ACCESS_KEY = "XXXXXXXXXXXXXXXXXXXX"
AWS_SECRET_KEY = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"


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
        client.s3.create_bucket('mybucket')
        return S3Target('s3://mybucket/test_file', client=client, format=format, **kwargs)

    def test_read(self):
        client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        client.s3.create_bucket('mybucket')
        client.put(self.tempFilePath, 's3://mybucket/tempfile')
        t = S3Target('s3://mybucket/tempfile', client=client)
        read_file = t.open()
        file_str = read_file.read()
        self.assertEqual(self.tempFileContents, file_str.encode('utf-8'))

    def test_read_no_file(self):
        t = self.create_target()
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
            contents = firstline + 'line two' + os.linesep + 'line three'
            tempf.write(contents.encode('utf-8'))
            tempf.close()

            client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
            client.s3.create_bucket('mybucket')
            client.put(temppath, 's3://mybucket/largetempfile')
            t = S3Target('s3://mybucket/largetempfile', client=client)
            with t.open() as read_file:
                lines = [line for line in read_file]
        finally:
            key.Key.BufferSize = old_buffer

        self.assertEqual(3, len(lines))
        self.assertEqual(firstline, lines[0])
        self.assertEqual("line two" + os.linesep, lines[1])
        self.assertEqual("line three", lines[2])

    def test_get_path(self):
        t = self.create_target()
        path = t.path
        self.assertEqual('s3://mybucket/test_file', path)

    def test_get_path_sse(self):
        t = self.create_target(encrypt_key=True)
        path = t.path
        self.assertEqual('s3://mybucket/test_file', path)


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

    def test_init_with_environment_variables(self):
        os.environ['AWS_ACCESS_KEY_ID'] = 'foo'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'bar'
        # Don't read any exsisting config
        old_config_paths = configuration.LuigiConfigParser._config_paths
        configuration.LuigiConfigParser._config_paths = [tempfile.mktemp()]

        s3_client = S3Client()
        configuration.LuigiConfigParser._config_paths = old_config_paths

        self.assertEqual(s3_client.s3.gs_access_key_id, 'foo')
        self.assertEqual(s3_client.s3.gs_secret_access_key, 'bar')

    @with_config({'s3': {'aws_access_key_id': 'foo', 'aws_secret_access_key': 'bar'}})
    def test_init_with_config(self):
        s3_client = S3Client()
        self.assertEqual(s3_client.s3.access_key, 'foo')
        self.assertEqual(s3_client.s3.secret_key, 'bar')

    @with_config({'s3': {'aws_role_arn': 'role', 'aws_role_session_name': 'name'}})
    def test_init_with_config_and_roles(self):
        s3_client = S3Client()
        self.assertEqual(s3_client.s3.access_key, 'AKIAIOSFODNN7EXAMPLE')
        self.assertEqual(s3_client.s3.secret_key, 'aJalrXUtnFEMI/K7MDENG/bPxRfiCYzEXAMPLEKEY')

    def test_put(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.s3.create_bucket('mybucket')
        s3_client.put(self.tempFilePath, 's3://mybucket/putMe')
        self.assertTrue(s3_client.exists('s3://mybucket/putMe'))

    def test_put_sse(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.s3.create_bucket('mybucket')
        s3_client.put(self.tempFilePath, 's3://mybucket/putMe', encrypt_key=True)
        self.assertTrue(s3_client.exists('s3://mybucket/putMe'))

    def test_put_string(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.s3.create_bucket('mybucket')
        s3_client.put_string("SOMESTRING", 's3://mybucket/putString')
        self.assertTrue(s3_client.exists('s3://mybucket/putString'))

    def test_put_string_sse(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.s3.create_bucket('mybucket')
        s3_client.put_string("SOMESTRING", 's3://mybucket/putString', encrypt_key=True)
        self.assertTrue(s3_client.exists('s3://mybucket/putString'))

    def test_put_multipart_multiple_parts_non_exact_fit(self):
        """
        Test a multipart put with two parts, where the parts are not exactly the split size.
        """
        # 5MB is minimum part size
        part_size = (1024 ** 2) * 5
        file_size = (part_size * 2) - 5000
        self._run_multipart_test(part_size, file_size)

    def test_put_multipart_multiple_parts_non_exact_fit_with_sse(self):
        """
        Test a multipart put with two parts, where the parts are not exactly the split size.
        """
        # 5MB is minimum part size
        part_size = (1024 ** 2) * 5
        file_size = (part_size * 2) - 5000
        self._run_multipart_test(part_size, file_size, encrypt_key=True)

    def test_put_multipart_multiple_parts_exact_fit(self):
        """
        Test a multipart put with multiple parts, where the parts are exactly the split size.
        """
        # 5MB is minimum part size
        part_size = (1024 ** 2) * 5
        file_size = part_size * 2
        self._run_multipart_test(part_size, file_size)

    def test_put_multipart_multiple_parts_exact_fit_wit_sse(self):
        """
        Test a multipart put with multiple parts, where the parts are exactly the split size.
        """
        # 5MB is minimum part size
        part_size = (1024 ** 2) * 5
        file_size = part_size * 2
        self._run_multipart_test(part_size, file_size, encrypt_key=True)

    def test_put_multipart_less_than_split_size(self):
        """
        Test a multipart put with a file smaller than split size; should revert to regular put.
        """
        # 5MB is minimum part size
        part_size = (1024 ** 2) * 5
        file_size = 5000
        self._run_multipart_test(part_size, file_size)

    def test_put_multipart_less_than_split_size_with_sse(self):
        """
        Test a multipart put with a file smaller than split size; should revert to regular put.
        """
        # 5MB is minimum part size
        part_size = (1024 ** 2) * 5
        file_size = 5000
        self._run_multipart_test(part_size, file_size, encrypt_key=True)

    def test_put_multipart_empty_file(self):
        """
        Test a multipart put with an empty file.
        """
        # 5MB is minimum part size
        part_size = (1024 ** 2) * 5
        file_size = 0
        self._run_multipart_test(part_size, file_size)

    def test_put_multipart_empty_file_with_sse(self):
        """
        Test a multipart put with an empty file.
        """
        # 5MB is minimum part size
        part_size = (1024 ** 2) * 5
        file_size = 0
        self._run_multipart_test(part_size, file_size, encrypt_key=True)

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

    def test_get(self):
        # put a file on s3 first
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.s3.create_bucket('mybucket')
        s3_client.put(self.tempFilePath, 's3://mybucket/putMe')

        tmp_file = tempfile.NamedTemporaryFile(delete=True)
        tmp_file_path = tmp_file.name

        s3_client.get('s3://mybucket/putMe', tmp_file_path)
        self.assertEquals(tmp_file.read(), self.tempFileContents)

        tmp_file.close()

    def test_get_as_string(self):
        # put a file on s3 first
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.s3.create_bucket('mybucket')
        s3_client.put(self.tempFilePath, 's3://mybucket/putMe')

        contents = s3_client.get_as_string('s3://mybucket/putMe')

        self.assertEquals(contents, self.tempFileContents)

    def test_get_key(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.s3.create_bucket('mybucket')
        s3_client.put(self.tempFilePath, 's3://mybucket/key_to_find')
        self.assertTrue(s3_client.get_key('s3://mybucket/key_to_find'))
        self.assertFalse(s3_client.get_key('s3://mybucket/does_not_exist'))

    def test_isdir(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.s3.create_bucket('mybucket')
        self.assertTrue(s3_client.isdir('s3://mybucket'))

        s3_client.put(self.tempFilePath, 's3://mybucket/tempdir0_$folder$')
        self.assertTrue(s3_client.isdir('s3://mybucket/tempdir0'))

        s3_client.put(self.tempFilePath, 's3://mybucket/tempdir1/')
        self.assertTrue(s3_client.isdir('s3://mybucket/tempdir1'))

        s3_client.put(self.tempFilePath, 's3://mybucket/key')
        self.assertFalse(s3_client.isdir('s3://mybucket/key'))

    def test_mkdir(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.s3.create_bucket('mybucket')
        self.assertTrue(s3_client.isdir('s3://mybucket'))
        s3_client.mkdir('s3://mybucket')

        s3_client.mkdir('s3://mybucket/dir')
        self.assertTrue(s3_client.isdir('s3://mybucket/dir'))

        self.assertRaises(MissingParentDirectory,
                          s3_client.mkdir, 's3://mybucket/dir/foo/bar', parents=False)
        self.assertFalse(s3_client.isdir('s3://mybucket/dir/foo/bar'))

    def test_listdir(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.s3.create_bucket('mybucket')

        s3_client.put_string("", 's3://mybucket/hello/frank')
        s3_client.put_string("", 's3://mybucket/hello/world')

        self.assertEqual(['s3://mybucket/hello/frank', 's3://mybucket/hello/world'],
                         list(s3_client.listdir('s3://mybucket/hello')))

    def test_list(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.s3.create_bucket('mybucket')

        s3_client.put_string("", 's3://mybucket/hello/frank')
        s3_client.put_string("", 's3://mybucket/hello/world')

        self.assertEqual(['frank', 'world'],
                         list(s3_client.list('s3://mybucket/hello')))

    def test_listdir_key(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.s3.create_bucket('mybucket')

        s3_client.put_string("", 's3://mybucket/hello/frank')
        s3_client.put_string("", 's3://mybucket/hello/world')

        self.assertEqual([True, True],
                         [x.exists() for x in s3_client.listdir('s3://mybucket/hello', return_key=True)])

    def test_list_key(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.s3.create_bucket('mybucket')

        s3_client.put_string("", 's3://mybucket/hello/frank')
        s3_client.put_string("", 's3://mybucket/hello/world')

        self.assertEqual([True, True],
                         [x.exists() for x in s3_client.list('s3://mybucket/hello', return_key=True)])

    def test_remove(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.s3.create_bucket('mybucket')

        self.assertRaises(
            S3ResponseError,
            lambda: s3_client.remove('s3://bucketdoesnotexist/file')
        )

        self.assertFalse(s3_client.remove('s3://mybucket/doesNotExist'))

        s3_client.put(self.tempFilePath, 's3://mybucket/existingFile0')
        self.assertTrue(s3_client.remove('s3://mybucket/existingFile0'))
        self.assertFalse(s3_client.exists('s3://mybucket/existingFile0'))

        self.assertRaises(
            InvalidDeleteException,
            lambda: s3_client.remove('s3://mybucket/')
        )

        self.assertRaises(
            InvalidDeleteException,
            lambda: s3_client.remove('s3://mybucket')
        )

        s3_client.put(self.tempFilePath, 's3://mybucket/removemedir/file')
        self.assertRaises(
            InvalidDeleteException,
            lambda: s3_client.remove('s3://mybucket/removemedir', recursive=False)
        )

        # test that the marker file created by Hadoop S3 Native FileSystem is removed
        s3_client.put(self.tempFilePath, 's3://mybucket/removemedir/file')
        s3_client.put_string("", 's3://mybucket/removemedir_$folder$')
        self.assertTrue(s3_client.remove('s3://mybucket/removemedir'))
        self.assertFalse(s3_client.exists('s3://mybucket/removemedir_$folder$'))

    def test_copy_multiple_parts_non_exact_fit(self):
        """
        Test a multipart put with two parts, where the parts are not exactly the split size.
        """
        # First, put a file into S3
        self._run_copy_test(self.test_put_multipart_multiple_parts_non_exact_fit)

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

    @unittest.skip("bug in moto 0.4.21 causing failure: https://github.com/spulec/moto/issues/526")
    def test_copy_multipart_multiple_parts_non_exact_fit(self):
        """
        Test a multipart copy with two parts, where the parts are not exactly the split size.
        """
        # First, put a file into S3
        self._run_multipart_copy_test(self.test_put_multipart_multiple_parts_non_exact_fit)

    @unittest.skip("bug in moto 0.4.21 causing failure: https://github.com/spulec/moto/issues/526")
    def test_copy_multipart_multiple_parts_exact_fit(self):
        """
        Test a multipart copy with multiple parts, where the parts are exactly the split size.
        """
        self._run_multipart_copy_test(self.test_put_multipart_multiple_parts_exact_fit)

    def test_copy_multipart_less_than_split_size(self):
        """
        Test a multipart copy with a file smaller than split size; should revert to regular put.
        """
        self._run_multipart_copy_test(self.test_put_multipart_less_than_split_size)

    def test_copy_multipart_empty_file(self):
        """
        Test a multipart copy with an empty file.
        """
        self._run_multipart_copy_test(self.test_put_multipart_empty_file)

    @skipOnTravis('https://travis-ci.org/spotify/luigi/jobs/145895385')
    def test_copy_dir(self):
        """
        Test copying 20 files from one folder to another
        """

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
        s3_client.s3.create_bucket('mybucket')

        for i in range(n):
            file_path = s3_dir + str(i)
            s3_client.put_multipart(tmp_file_path, file_path)
            self.assertTrue(s3_client.exists(file_path))

        s3_dest = 's3://mybucket/copydir_new/'
        s3_client.copy(s3_dir, s3_dest, threads=10, part_size=copy_part_size)

        for i in range(n):
            original_size = s3_client.get_key(s3_dir + str(i)).size
            copy_size = s3_client.get_key(s3_dest + str(i)).size
            self.assertEqual(original_size, copy_size)

    def _run_multipart_copy_test(self, put_method):
        # Run the method to put the file into s3 into the first place
        put_method()

        # As all the multipart put methods use `self._run_multipart_test`
        # we can just use this key
        original = 's3://mybucket/putMe'
        copy = 's3://mybucket/putMe_copy'

        # 5MB is minimum part size, use it here so we don't have to generate huge files to test
        # the multipart upload in moto
        part_size = (1024 ** 2) * 5

        # Copy the file from old location to new
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.copy(original, copy, part_size=part_size, threads=4)

        # We can't use etags to compare between multipart and normal keys,
        # so we fall back to using the size instead
        original_size = s3_client.get_key(original).size
        copy_size = s3_client.get_key(copy).size
        self.assertEqual(original_size, copy_size)

    def _run_copy_test(self, put_method):
        # Run the method to put the file into s3 into the first place
        put_method()

        # As all the multipart put methods use `self._run_multipart_test`
        # we can just use this key
        original = 's3://mybucket/putMe'
        copy = 's3://mybucket/putMe_copy'

        # Copy the file from old location to new
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.copy(original, copy, threads=4)

        # We can't use etags to compare between multipart and normal keys,
        # so we fall back to using the file size
        original_size = s3_client.get_key(original).size
        copy_size = s3_client.get_key(copy).size
        self.assertEqual(original_size, copy_size)

    def _run_multipart_test(self, part_size, file_size, **kwargs):
        file_contents = b"a" * file_size

        s3_path = 's3://mybucket/putMe'
        tmp_file = tempfile.NamedTemporaryFile(mode='wb', delete=True)
        tmp_file_path = tmp_file.name
        tmp_file.write(file_contents)
        tmp_file.flush()

        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.s3.create_bucket('mybucket')
        s3_client.put_multipart(tmp_file_path, s3_path, part_size=part_size, **kwargs)
        self.assertTrue(s3_client.exists(s3_path))
        file_size = os.path.getsize(tmp_file.name)
        key_size = s3_client.get_key(s3_path).size
        self.assertEqual(file_size, key_size)
        tmp_file.close()
