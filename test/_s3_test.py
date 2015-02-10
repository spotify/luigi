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

import gc
import os
import sys
import tempfile
import unittest

import luigi.format
from boto.exception import S3ResponseError
from boto.s3 import key
from luigi import configuration
from luigi.s3 import FileNotFoundException, InvalidDeleteException, S3Client, S3Target

# moto does not yet work with
# python 2.6. Until it does,
# disable these tests in python2.6
try:
    from moto import mock_s3
except ImportError:
    # https://github.com/spulec/moto/issues/29
    print('Skipping %s because moto does not install properly before '
          'python2.7' % __file__)
    from luigi.mock import skip
    mock_s3 = skip

AWS_ACCESS_KEY = "XXXXXXXXXXXXXXXXXXXX"
AWS_SECRET_KEY = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"


class TestS3Target(unittest.TestCase):

    def setUp(self):
        f = tempfile.NamedTemporaryFile(mode='wb', delete=False)
        self.tempFileContents = (
            "I'm a temporary file for testing\nAnd this is the second line\n"
            "This is the third.")
        self.tempFilePath = f.name
        f.write(self.tempFileContents)
        f.close()

    def tearDown(self):
        os.remove(self.tempFilePath)

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
    def test_read(self):
        client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        client.s3.create_bucket('mybucket')
        client.put(self.tempFilePath, 's3://mybucket/tempfile')
        t = S3Target('s3://mybucket/tempfile', client=client)
        read_file = t.open()
        file_str = read_file.read()
        self.assertEqual(self.tempFileContents, file_str)

    @mock_s3
    def test_read_no_file(self):
        client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        client.s3.create_bucket('mybucket')
        t = S3Target('s3://mybucket/tempfile', client=client)
        with self.assertRaises(FileNotFoundException):
            t.open()

    @mock_s3
    def test_read_iterator(self):
        # write a file that is 5X the boto buffersize
        # to test line buffering
        tempf = tempfile.NamedTemporaryFile(mode='wb', delete=False)
        temppath = tempf.name
        firstline = ''.zfill(key.Key.BufferSize * 5) + os.linesep
        contents = firstline + 'line two' + os.linesep + 'line three'
        tempf.write(contents)
        tempf.close()

        client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        client.s3.create_bucket('mybucket')
        client.put(temppath, 's3://mybucket/largetempfile')
        t = S3Target('s3://mybucket/largetempfile', client=client)
        with t.open() as read_file:
            lines = [line for line in read_file]
        self.assertEqual(3, len(lines))
        self.assertEqual(firstline, lines[0])
        self.assertEqual("line two" + os.linesep, lines[1])
        self.assertEqual("line three", lines[2])

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
        t = S3Target('s3://mybucket/gzip_test', luigi.format.Gzip,
                     client=client)
        p = t.open('w')
        test_data = 'test'
        p.write(test_data)
        self.assertFalse(t.exists())
        p.close()
        self.assertTrue(t.exists())

    @mock_s3
    def test_gzip_works_and_cleans_up(self):
        client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        client.s3.create_bucket('mybucket')
        t = S3Target('s3://mybucket/gzip_test', luigi.format.Gzip,
                     client=client)
        test_data = '123testing'
        with t.open('w') as f:
            f.write(test_data)

        with t.open() as f:
            result = f.read()

        self.assertEqual(test_data, result)
        self.assertFalse(os.path.exists(t._tmp_extract_path))


class TestS3Client(unittest.TestCase):

    def setUp(self):
        f = tempfile.NamedTemporaryFile(mode='wb', delete=False)
        self.tempFilePath = f.name
        f.write("I'm a temporary file for testing\n")
        f.close()

        self.s3_config = dict(aws_access_key_id='foo',
                              aws_secret_access_key='bar')
        with open(tempfile.mktemp(prefix='luigi_s3_test_'), 'w') as f:
            self._s3_config_path = f.name
            f.write('[s3]\n{}\n'.format(
                '\n'.join(['{}: {}'.format(k, v)
                           for k, v in self.s3_config.iteritems()])))
        self._old_config_paths = configuration.LuigiConfigParser._config_paths
        configuration.LuigiConfigParser._config_paths = self._s3_config_path

    def tearDown(self):
        os.remove(self.tempFilePath)
        os.remove(self._s3_config_path)
        configuration.LuigiConfigParser._config_paths = self._old_config_paths

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

    @mock_s3
    def test_init_with_config(self):
        s3_client = S3Client()
        self.assertEqual(s3_client.s3.access_key,
                         self.s3_config['aws_access_key_id'])
        self.assertEqual(s3_client.s3.secret_key,
                         self.s3_config['aws_secret_access_key'])

    @mock_s3
    def test_put(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.s3.create_bucket('mybucket')
        s3_client.put(self.tempFilePath, 's3://mybucket/putMe')
        self.assertTrue(s3_client.exists('s3://mybucket/putMe'))

    @mock_s3
    def test_put_string(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.s3.create_bucket('mybucket')
        s3_client.put_string("SOMESTRING", 's3://mybucket/putString')
        self.assertTrue(s3_client.exists('s3://mybucket/putString'))

    @mock_s3
    def test_put_multipart_multiple_parts_non_exact_fit(self):
        """
        Test a multipart put with two parts, where the parts are not exactly the split size.
        """
        # 5MB is minimum part size
        part_size = (1024 ** 2) * 5
        file_size = (part_size * 2) - 5000
        self._run_multipart_test(part_size, file_size)

    @mock_s3
    def test_put_multipart_multiple_parts_exact_fit(self):
        """
        Test a multipart put with multiple parts, where the parts are exactly the split size.
        """
        # 5MB is minimum part size
        part_size = (1024 ** 2) * 5
        file_size = part_size * 2
        self._run_multipart_test(part_size, file_size)

    @mock_s3
    def test_put_multipart_less_than_split_size(self):
        """
        Test a multipart put with a file smaller than split size; should revert to regular put.
        """
        # 5MB is minimum part size
        part_size = (1024 ** 2) * 5
        file_size = 5000
        self._run_multipart_test(part_size, file_size)

    @mock_s3
    def test_put_multipart_empty_file(self):
        """
        Test a multipart put with an empty file.
        """
        # 5MB is minimum part size
        part_size = (1024 ** 2) * 5
        file_size = 0
        self._run_multipart_test(part_size, file_size)

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
    def test_get_key(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.s3.create_bucket('mybucket')
        s3_client.put(self.tempFilePath, 's3://mybucket/key_to_find')
        self.assertTrue(s3_client.get_key('s3://mybucket/key_to_find'))
        self.assertFalse(s3_client.get_key('s3://mybucket/does_not_exist'))

    @mock_s3
    def test_is_dir(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.s3.create_bucket('mybucket')
        self.assertTrue(s3_client.is_dir('s3://mybucket'))

        s3_client.put(self.tempFilePath, 's3://mybucket/tempdir0_$folder$')
        self.assertTrue(s3_client.is_dir('s3://mybucket/tempdir0'))

        s3_client.put(self.tempFilePath, 's3://mybucket/tempdir1/')
        self.assertTrue(s3_client.is_dir('s3://mybucket/tempdir1'))

        s3_client.put(self.tempFilePath, 's3://mybucket/key')
        self.assertFalse(s3_client.is_dir('s3://mybucket/key'))

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

    def _run_multipart_test(self, part_size, file_size):
        file_contents = "a" * file_size

        s3_path = 's3://mybucket/putMe'
        tmp_file = tempfile.NamedTemporaryFile(mode='wb', delete=True)
        tmp_file_path = tmp_file.name
        tmp_file.write(file_contents)
        tmp_file.flush()

        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.s3.create_bucket('mybucket')
        s3_client.put_multipart(tmp_file_path, s3_path, part_size=part_size)
        self.assertTrue(s3_client.exists(s3_path))
        # b/c of https://github.com/spulec/moto/issues/131 have to
        # get contents to check size
        key_contents = s3_client.get_key(s3_path).get_contents_as_string()
        self.assertEqual(len(file_contents), len(key_contents))

        tmp_file.close()

if __name__ == '__main__':
    unittest.main()
