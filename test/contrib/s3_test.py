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

from luigi import configuration
from luigi.contrib._luigi1_compat import IS_LUIGI1_DEPRECATED
from luigi.contrib.s3 import (
    FileNotFoundException,
    InvalidDeleteException,
    ReadableS3File,
    ReadableS3FileBoto1,
    ReadableS3FileBoto3,
    S3Client,
    S3ClientBoto1,
    S3ClientBoto3,
    S3EmrTask,
    S3FlagTask,
    S3PathTask,
    S3Target,
)
from luigi.target import MissingParentDirectory

# `S3Client` is the flag-resolved alias from luigi.contrib.s3:
#   IS_LUIGI1_DEPRECATED is False  → S3Client is S3ClientBoto1 (legacy boto1 stack)
#   IS_LUIGI1_DEPRECATED is True   → S3Client is S3ClientBoto3 (modern boto3 stack)
USING_BOTO1 = S3Client is S3ClientBoto1

# Try to import boto1 unconditionally so the file can collect cleanly even
# in degenerate configs (e.g. luigi1 installed but boto missing). The
# resulting symbol bindings let us pick the right exception class to match
# whichever stack is actually live.
try:
    from boto.exception import S3ResponseError as _Boto1ResponseError
    from boto.s3 import key
    HAS_BOTO_PKG = True
except ImportError:
    _Boto1ResponseError = None
    key = None
    HAS_BOTO_PKG = False

from botocore.exceptions import ClientError as _Boto3ResponseError

# ``S3ResponseError`` must match whatever the active stack actually raises,
# not just whichever package happens to be installed. e.g. boto can be on
# the path while the flag still selects boto3 — in that case errors come
# from botocore, not boto.
if USING_BOTO1 and HAS_BOTO_PKG:
    S3ResponseError = _Boto1ResponseError
else:
    S3ResponseError = _Boto3ResponseError

# Tests that exercise boto1-only behaviour (encrypt_key kwarg,
# key.Key.BufferSize, boto-specific credential attrs) need both the flag
# pointing at boto1 AND the boto package to be importable.
BOTO1_RUNNABLE = USING_BOTO1 and HAS_BOTO_PKG

# True when ``S3Client()`` (the flag-resolved default) can actually be
# instantiated in this env. ``S3ClientBoto1.__init__`` does ``from boto.s3.key
# import Key``, so when the flag selects boto1 but the boto package is missing,
# instantiation fails. Tests that construct ``S3Client()`` (or any class that
# transitively does, like ``S3Target('s3://...')`` with no explicit client)
# must skip in that degenerate case.
S3CLIENT_INSTANTIABLE = HAS_BOTO_PKG if USING_BOTO1 else True

try:
    from moto import mock_s3, mock_sts
except ImportError:
    # moto >= 4.0 renamed mock_s3/mock_sts to mock_aws
    from moto import mock_aws as mock_s3, mock_aws as mock_sts

if (3, 4, 0) <= sys.version_info[:3] < (3, 4, 3):
    # spulec/moto#308
    raise unittest.SkipTest('moto mock doesn\'t work with python3.4')


AWS_ACCESS_KEY = "XXXXXXXXXXXXXXXXXXXX"
AWS_SECRET_KEY = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"


@unittest.skipUnless(S3CLIENT_INSTANTIABLE,
                     'flag selects boto1 stack but boto package is not installed')
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

    def _create_bucket(self, client):
        if USING_BOTO1:
            client.s3.create_bucket('mybucket')
        else:
            import boto3
            conn = boto3.resource('s3', region_name='us-east-1')
            conn.create_bucket(Bucket='mybucket')

    def create_target(self, format=None, **kwargs):
        client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        self._create_bucket(client)
        return S3Target('s3://mybucket/test_file', client=client, format=format, **kwargs)

    def test_read(self):
        client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        self._create_bucket(client)
        client.put(self.tempFilePath, 's3://mybucket/tempfile')
        t = S3Target('s3://mybucket/tempfile', client=client)
        read_file = t.open()
        file_str = read_file.read()
        self.assertEqual(self.tempFileContents, file_str.encode('utf-8'))

    def test_read_no_file(self):
        t = self.create_target()
        self.assertRaises(FileNotFoundException, t.open)

    @unittest.skipIf(not BOTO1_RUNNABLE, 'encrypt_key is boto-only')
    def test_read_no_file_sse(self):
        t = self.create_target(encrypt_key=True)
        self.assertRaises(FileNotFoundException, t.open)

    @unittest.skipIf(not BOTO1_RUNNABLE, 'boto Key.BufferSize not available with boto3')
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
            self._create_bucket(client)
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

    @unittest.skipIf(not BOTO1_RUNNABLE, 'encrypt_key is boto-only')
    def test_get_path_sse(self):
        t = self.create_target(encrypt_key=True)
        path = t.path
        self.assertEqual('s3://mybucket/test_file', path)


@unittest.skipUnless(S3CLIENT_INSTANTIABLE,
                     'flag selects boto1 stack but boto package is not installed')
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

    def _create_bucket(self, client=None, name='mybucket'):
        if USING_BOTO1:
            (client or S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)).s3.create_bucket(name)
        else:
            import boto3
            conn = boto3.resource('s3', region_name='us-east-1')
            conn.create_bucket(Bucket=name)

    @unittest.skipIf(not BOTO1_RUNNABLE, 'boto-specific credential attribute gs_access_key_id')
    def test_init_with_environment_variables(self):
        os.environ['AWS_ACCESS_KEY_ID'] = 'foo'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'bar'
        # Don't read any existing config
        old_config_paths = configuration.LuigiConfigParser._config_paths
        configuration.LuigiConfigParser._config_paths = [tempfile.mktemp()]

        s3_client = S3Client()
        configuration.LuigiConfigParser._config_paths = old_config_paths

        self.assertEqual(s3_client.s3.gs_access_key_id, 'foo')
        self.assertEqual(s3_client.s3.gs_secret_access_key, 'bar')

    @unittest.skipIf(not BOTO1_RUNNABLE, 'boto-specific credential attributes access_key/secret_key')
    @with_config({'s3': {'aws_access_key_id': 'foo', 'aws_secret_access_key': 'bar'}})
    def test_init_with_config(self):
        s3_client = S3Client()
        self.assertEqual(s3_client.s3.access_key, 'foo')
        self.assertEqual(s3_client.s3.secret_key, 'bar')

    @unittest.skipIf(not BOTO1_RUNNABLE, 'boto-specific STS credential attributes')
    @with_config({'s3': {'aws_role_arn': 'role', 'aws_role_session_name': 'name'}})
    def test_init_with_config_and_roles(self):
        s3_client = S3Client()
        self.assertEqual(s3_client.s3.access_key, 'AKIAIOSFODNN7EXAMPLE')
        self.assertEqual(s3_client.s3.secret_key, 'aJalrXUtnFEMI/K7MDENG/bPxRfiCYzEXAMPLEKEY')

    def test_put(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        self._create_bucket(s3_client)
        s3_client.put(self.tempFilePath, 's3://mybucket/putMe')
        self.assertTrue(s3_client.exists('s3://mybucket/putMe'))

    @unittest.skipIf(not BOTO1_RUNNABLE, 'encrypt_key is boto-only')
    def test_put_sse(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        self._create_bucket(s3_client)
        s3_client.put(self.tempFilePath, 's3://mybucket/putMe', encrypt_key=True)
        self.assertTrue(s3_client.exists('s3://mybucket/putMe'))

    def test_put_string(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        self._create_bucket(s3_client)
        s3_client.put_string("SOMESTRING", 's3://mybucket/putString')
        self.assertTrue(s3_client.exists('s3://mybucket/putString'))

    @unittest.skipIf(not BOTO1_RUNNABLE, 'encrypt_key is boto-only')
    def test_put_string_sse(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        self._create_bucket(s3_client)
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

    @unittest.skipIf(not BOTO1_RUNNABLE, 'encrypt_key is boto-only')
    def test_put_multipart_multiple_parts_non_exact_fit_with_sse(self):
        part_size = (1024 ** 2) * 5
        file_size = (part_size * 2) - 5000
        self._run_multipart_test(part_size, file_size, encrypt_key=True)

    def test_put_multipart_multiple_parts_exact_fit(self):
        """
        Test a multipart put with multiple parts, where the parts are exactly the split size.
        """
        part_size = (1024 ** 2) * 5
        file_size = part_size * 2
        self._run_multipart_test(part_size, file_size)

    @unittest.skipIf(not BOTO1_RUNNABLE, 'encrypt_key is boto-only')
    def test_put_multipart_multiple_parts_exact_fit_wit_sse(self):
        part_size = (1024 ** 2) * 5
        file_size = part_size * 2
        self._run_multipart_test(part_size, file_size, encrypt_key=True)

    def test_put_multipart_less_than_split_size(self):
        """
        Test a multipart put with a file smaller than split size; should revert to regular put.
        """
        part_size = (1024 ** 2) * 5
        file_size = 5000
        self._run_multipart_test(part_size, file_size)

    @unittest.skipIf(not BOTO1_RUNNABLE, 'encrypt_key is boto-only')
    def test_put_multipart_less_than_split_size_with_sse(self):
        part_size = (1024 ** 2) * 5
        file_size = 5000
        self._run_multipart_test(part_size, file_size, encrypt_key=True)

    def test_put_multipart_empty_file(self):
        """
        Test a multipart put with an empty file.
        """
        part_size = (1024 ** 2) * 5
        file_size = 0
        self._run_multipart_test(part_size, file_size)

    @unittest.skipIf(not BOTO1_RUNNABLE, 'encrypt_key is boto-only')
    def test_put_multipart_empty_file_with_sse(self):
        part_size = (1024 ** 2) * 5
        file_size = 0
        self._run_multipart_test(part_size, file_size, encrypt_key=True)

    def test_exists(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        self._create_bucket(s3_client)

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
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        self._create_bucket(s3_client)
        s3_client.put(self.tempFilePath, 's3://mybucket/putMe')

        tmp_file = tempfile.NamedTemporaryFile(delete=True)
        tmp_file_path = tmp_file.name

        s3_client.get('s3://mybucket/putMe', tmp_file_path)
        self.assertEqual(tmp_file.read(), self.tempFileContents)

        tmp_file.close()

    def test_get_as_string(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        self._create_bucket(s3_client)
        s3_client.put(self.tempFilePath, 's3://mybucket/putMe')

        contents = s3_client.get_as_string('s3://mybucket/putMe')

        self.assertEqual(contents, self.tempFileContents)

    def test_get_key(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        self._create_bucket(s3_client)
        s3_client.put(self.tempFilePath, 's3://mybucket/key_to_find')
        self.assertTrue(s3_client.get_key('s3://mybucket/key_to_find'))
        self.assertFalse(s3_client.get_key('s3://mybucket/does_not_exist'))

    def test_isdir(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        self._create_bucket(s3_client)
        self.assertTrue(s3_client.isdir('s3://mybucket'))

        s3_client.put(self.tempFilePath, 's3://mybucket/tempdir0_$folder$')
        self.assertTrue(s3_client.isdir('s3://mybucket/tempdir0'))

        s3_client.put(self.tempFilePath, 's3://mybucket/tempdir1/')
        self.assertTrue(s3_client.isdir('s3://mybucket/tempdir1'))

        s3_client.put(self.tempFilePath, 's3://mybucket/key')
        self.assertFalse(s3_client.isdir('s3://mybucket/key'))

    def test_mkdir(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        self._create_bucket(s3_client)
        self.assertTrue(s3_client.isdir('s3://mybucket'))
        s3_client.mkdir('s3://mybucket')

        s3_client.mkdir('s3://mybucket/dir')
        self.assertTrue(s3_client.isdir('s3://mybucket/dir'))

        self.assertRaises(MissingParentDirectory,
                          s3_client.mkdir, 's3://mybucket/dir/foo/bar', parents=False)
        self.assertFalse(s3_client.isdir('s3://mybucket/dir/foo/bar'))

    def test_listdir(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        self._create_bucket(s3_client)

        s3_client.put_string("", 's3://mybucket/hello/frank')
        s3_client.put_string("", 's3://mybucket/hello/world')

        self.assertEqual(['s3://mybucket/hello/frank', 's3://mybucket/hello/world'],
                         list(s3_client.listdir('s3://mybucket/hello')))

    def test_list(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        self._create_bucket(s3_client)

        s3_client.put_string("", 's3://mybucket/hello/frank')
        s3_client.put_string("", 's3://mybucket/hello/world')

        self.assertEqual(['frank', 'world'],
                         list(s3_client.list('s3://mybucket/hello')))

    def test_listdir_key(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        self._create_bucket(s3_client)

        s3_client.put_string("", 's3://mybucket/hello/frank')
        s3_client.put_string("", 's3://mybucket/hello/world')

        self.assertEqual([True, True],
                         [x.exists() for x in s3_client.listdir('s3://mybucket/hello', return_key=True)])

    def test_list_key(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        self._create_bucket(s3_client)

        s3_client.put_string("", 's3://mybucket/hello/frank')
        s3_client.put_string("", 's3://mybucket/hello/world')

        self.assertEqual([True, True],
                         [x.exists() for x in s3_client.list('s3://mybucket/hello', return_key=True)])

    def test_remove(self):
        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        self._create_bucket(s3_client)

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
        self._run_copy_test(self.test_put_multipart_multiple_parts_non_exact_fit)

    def test_copy_multiple_parts_exact_fit(self):
        self._run_copy_test(self.test_put_multipart_multiple_parts_exact_fit)

    def test_copy_less_than_split_size(self):
        self._run_copy_test(self.test_put_multipart_less_than_split_size)

    def test_copy_empty_file(self):
        self._run_copy_test(self.test_put_multipart_empty_file)

    def test_copy_multipart_multiple_parts_non_exact_fit(self):
        self._run_multipart_copy_test(self.test_put_multipart_multiple_parts_non_exact_fit)

    def test_copy_multipart_multiple_parts_exact_fit(self):
        self._run_multipart_copy_test(self.test_put_multipart_multiple_parts_exact_fit)

    def test_copy_multipart_less_than_split_size(self):
        self._run_multipart_copy_test(self.test_put_multipart_less_than_split_size)

    def test_copy_multipart_empty_file(self):
        self._run_multipart_copy_test(self.test_put_multipart_empty_file)

    @skipOnTravis('https://travis-ci.org/spotify/luigi/jobs/145895385')
    def test_copy_dir(self):
        n = 20
        copy_part_size = (1024 ** 2) * 5
        file_size = 5000

        s3_dir = 's3://mybucket/copydir/'
        file_contents = b"a" * file_size
        tmp_file = tempfile.NamedTemporaryFile(mode='wb', delete=True)
        tmp_file_path = tmp_file.name
        tmp_file.write(file_contents)
        tmp_file.flush()

        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        self._create_bucket(s3_client)

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
        put_method()

        original = 's3://mybucket/putMe'
        copy = 's3://mybucket/putMe_copy'

        part_size = (1024 ** 2) * 5

        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.copy(original, copy, part_size=part_size, threads=4)

        original_size = s3_client.get_key(original).size
        copy_size = s3_client.get_key(copy).size
        self.assertEqual(original_size, copy_size)

    def _run_copy_test(self, put_method):
        put_method()

        original = 's3://mybucket/putMe'
        copy = 's3://mybucket/putMe_copy'

        s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        s3_client.copy(original, copy, threads=4)

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
        self._create_bucket(s3_client)
        s3_client.put_multipart(tmp_file_path, s3_path, part_size=part_size, **kwargs)
        self.assertTrue(s3_client.exists(s3_path))
        file_size = os.path.getsize(tmp_file.name)
        key_size = s3_client.get_key(s3_path).size
        self.assertEqual(file_size, key_size)
        tmp_file.close()


class TestFlagDrivenS3Dispatch(unittest.TestCase):
    """``S3Client`` / ``ReadableS3File`` and the default client injected by
    ``S3Target`` / ``S3PathTask`` / ``S3EmrTask`` / ``S3FlagTask`` are all
    chosen by ``IS_LUIGI1_DEPRECATED``. These tests pin that contract."""

    def test_alias_matches_current_flag(self):
        if IS_LUIGI1_DEPRECATED:
            self.assertIs(S3Client, S3ClientBoto3)
            self.assertIs(ReadableS3File, ReadableS3FileBoto3)
        else:
            self.assertIs(S3Client, S3ClientBoto1)
            self.assertIs(ReadableS3File, ReadableS3FileBoto1)

    def test_readable_file_class_wired_to_each_client(self):
        # Independently of which is currently aliased as the default, each
        # client class must point at its matching readable-file impl so that
        # S3Target.open() resolves correctly regardless of which client was
        # injected.
        self.assertIs(S3ClientBoto1._readable_file_cls, ReadableS3FileBoto1)
        self.assertIs(S3ClientBoto3._readable_file_cls, ReadableS3FileBoto3)

    @unittest.skipUnless(S3CLIENT_INSTANTIABLE, 'flag selects boto1 but boto is not installed')
    def test_default_client_injection_in_s3_target(self):
        target = S3Target('s3://mybucket/key')
        self.assertIsInstance(target.fs, S3Client)

    @unittest.skipUnless(S3CLIENT_INSTANTIABLE, 'flag selects boto1 but boto is not installed')
    def test_default_client_injection_in_s3_path_task(self):
        task = S3PathTask(path='s3://mybucket/key')
        self.assertIsInstance(task.output().fs, S3Client)

    @unittest.skipUnless(S3CLIENT_INSTANTIABLE, 'flag selects boto1 but boto is not installed')
    def test_default_client_injection_in_s3_emr_task(self):
        # S3EmrTarget (via S3FlagTarget) requires path to end with '/'
        task = S3EmrTask(path='s3://mybucket/dir/')
        self.assertIsInstance(task.output().fs, S3Client)

    @unittest.skipUnless(S3CLIENT_INSTANTIABLE, 'flag selects boto1 but boto is not installed')
    def test_default_client_injection_in_s3_flag_task(self):
        task = S3FlagTask(path='s3://mybucket/dir/')
        self.assertIsInstance(task.output().fs, S3Client)

    @unittest.skipUnless(S3CLIENT_INSTANTIABLE, 'flag selects boto1 but boto is not installed')
    def test_explicit_client_overrides_flag_default(self):
        # Same-stack override — always runs (when S3Client is instantiable).
        # Confirms the ``client or S3Client()`` branch in S3Target.__init__
        # uses the injected instance verbatim, regardless of which stack the
        # flag selected as the default.
        same_stack = S3Client()
        target = S3Target('s3://mybucket/key', client=same_stack)
        self.assertIs(target.fs, same_stack)

    def test_explicit_client_cross_stack_override(self):
        # Cross-stack override — pick the OTHER stack from the flag-resolved
        # default and confirm S3Target uses the injected instance. Skipped
        # when the other stack's backing package is not installed.
        other_cls = S3ClientBoto1 if S3Client is S3ClientBoto3 else S3ClientBoto3
        try:
            cross_stack = other_cls()
        except ImportError:
            self.skipTest(
                '{} backing package not installed; cross-stack override path '
                'not exercised in this env'.format(other_cls.__name__)
            )
        target = S3Target('s3://mybucket/key', client=cross_stack)
        self.assertIs(target.fs, cross_stack)
        self.assertIsInstance(target.fs, other_cls)

    def test_aliases_flip_when_flag_flipped(self):
        # Reload luigi.contrib.s3 with luigi1 forcibly importable (flag → False)
        # and confirm the aliases flip to the boto1 stack; then with luigi1
        # forcibly unimportable (flag → True) and confirm they flip back to
        # boto3. Both directions must be simulated actively because luigi1
        # may or may not be genuinely installed in this env — popping
        # ``sys.modules['luigi1']`` alone is not enough when luigi1 is on
        # disk, since Python would re-import it from there.
        import importlib
        import sys
        import types

        class _BlockLuigi1Finder:
            """A meta_path finder that forces ``import luigi1`` to fail."""
            def find_spec(self, fullname, path, target=None):
                if fullname == 'luigi1':
                    raise ImportError("luigi1 blocked by test")
                return None

        saved_luigi1 = sys.modules.get('luigi1', None)
        had_luigi1 = 'luigi1' in sys.modules
        saved_meta_path = list(sys.meta_path)
        blocker = _BlockLuigi1Finder()
        try:
            # Flag → False: stub luigi1 in sys.modules so import succeeds.
            sys.modules['luigi1'] = types.ModuleType('luigi1')
            sys.modules.pop('luigi.contrib._luigi1_compat', None)
            sys.modules.pop('luigi.contrib.s3', None)
            s3mod = importlib.import_module('luigi.contrib.s3')
            self.assertIs(s3mod.S3Client, s3mod.S3ClientBoto1)
            self.assertIs(s3mod.ReadableS3File, s3mod.ReadableS3FileBoto1)

            # Flag → True: install a meta_path finder that raises ImportError
            # for ``luigi1``, so ``import luigi1`` fails even if the package
            # is installed on disk.
            sys.modules.pop('luigi1', None)
            sys.meta_path.insert(0, blocker)
            sys.modules.pop('luigi.contrib._luigi1_compat', None)
            sys.modules.pop('luigi.contrib.s3', None)
            s3mod = importlib.import_module('luigi.contrib.s3')
            self.assertIs(s3mod.S3Client, s3mod.S3ClientBoto3)
            self.assertIs(s3mod.ReadableS3File, s3mod.ReadableS3FileBoto3)
        finally:
            sys.meta_path[:] = saved_meta_path
            if had_luigi1:
                sys.modules['luigi1'] = saved_luigi1
            else:
                sys.modules.pop('luigi1', None)
            sys.modules.pop('luigi.contrib._luigi1_compat', None)
            sys.modules.pop('luigi.contrib.s3', None)
            importlib.import_module('luigi.contrib._luigi1_compat')
            importlib.import_module('luigi.contrib.s3')
