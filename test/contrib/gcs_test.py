# -*- coding: utf-8 -*-
#
# Copyright 2015 Twitter Inc
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

"""This is an integration test for the GCS-luigi binding.

This test requires credentials that can access GCS & access to a bucket below.
Follow the directions in the gcloud tools to set up local credentials.
"""

from helpers import unittest
try:
    import googleapiclient.errors
    import google.auth
except ImportError:
    raise unittest.SkipTest('Unable to load googleapiclient module')
import os
import tempfile
import unittest
from unittest import mock

from luigi.contrib import gcs
from target_test import FileSystemTargetTestMixin
import pytest

# In order to run this test, you should set these to your GCS project/bucket.
# Unfortunately there's no mock
PROJECT_ID = os.environ.get('GCS_TEST_PROJECT_ID', 'your_project_id_here')
BUCKET_NAME = os.environ.get('GCS_TEST_BUCKET', 'your_test_bucket_here')
TEST_FOLDER = os.environ.get('TRAVIS_BUILD_ID', 'gcs_test_folder')

CREDENTIALS, _ = google.auth.default()
ATTEMPTED_BUCKET_CREATE = False


def bucket_url(suffix):
    """
    Actually it's bucket + test folder name
    """
    return 'gs://{}/{}/{}'.format(BUCKET_NAME, TEST_FOLDER, suffix)


class _GCSBaseTestCase(unittest.TestCase):
    def setUp(self):
        self.client = gcs.GCSClient(CREDENTIALS)

        global ATTEMPTED_BUCKET_CREATE
        if not ATTEMPTED_BUCKET_CREATE:
            try:
                self.client.client.buckets().insert(
                    project=PROJECT_ID, body={'name': BUCKET_NAME}).execute()
            except googleapiclient.errors.HttpError as ex:
                if ex.resp.status != 409:  # bucket already exists
                    raise

            ATTEMPTED_BUCKET_CREATE = True

        self.client.remove(bucket_url(''), recursive=True)
        self.client.mkdir(bucket_url(''))

    def tearDown(self):
        self.client.remove(bucket_url(''), recursive=True)


@pytest.mark.gcloud
class GCSClientTest(_GCSBaseTestCase):

    def test_not_exists(self):
        self.assertFalse(self.client.exists(bucket_url('does_not_exist')))
        self.assertFalse(self.client.isdir(bucket_url('does_not_exist')))

    def test_exists(self):
        self.client.put_string('hello', bucket_url('exists_test'))
        self.assertTrue(self.client.exists(bucket_url('exists_test')))
        self.assertFalse(self.client.isdir(bucket_url('exists_test')))

    def test_mkdir(self):
        self.client.mkdir(bucket_url('exists_dir_test'))
        self.assertTrue(self.client.exists(bucket_url('exists_dir_test')))
        self.assertTrue(self.client.isdir(bucket_url('exists_dir_test')))

    def test_mkdir_by_upload(self):
        self.client.put_string('hello', bucket_url('test_dir_recursive/yep/file'))
        self.assertTrue(self.client.exists(bucket_url('test_dir_recursive')))
        self.assertTrue(self.client.isdir(bucket_url('test_dir_recursive')))

    def test_download(self):
        self.client.put_string('hello', bucket_url('test_download'))
        fp = self.client.download(bucket_url('test_download'))
        self.assertEqual(b'hello', fp.read())

    def test_rename(self):
        self.client.put_string('hello', bucket_url('test_rename_1'))
        self.client.rename(bucket_url('test_rename_1'), bucket_url('test_rename_2'))
        self.assertFalse(self.client.exists(bucket_url('test_rename_1')))
        self.assertTrue(self.client.exists(bucket_url('test_rename_2')))

    def test_rename_recursive(self):
        self.client.mkdir(bucket_url('test_rename_recursive'))
        self.client.put_string('hello', bucket_url('test_rename_recursive/1'))
        self.client.put_string('hello', bucket_url('test_rename_recursive/2'))
        self.client.rename(bucket_url('test_rename_recursive'), bucket_url('test_rename_recursive_dest'))
        self.assertFalse(self.client.exists(bucket_url('test_rename_recursive')))
        self.assertFalse(self.client.exists(bucket_url('test_rename_recursive/1')))
        self.assertTrue(self.client.exists(bucket_url('test_rename_recursive_dest')))
        self.assertTrue(self.client.exists(bucket_url('test_rename_recursive_dest/1')))

    def test_remove(self):
        self.client.put_string('hello', bucket_url('test_remove'))
        self.client.remove(bucket_url('test_remove'))
        self.assertFalse(self.client.exists(bucket_url('test_remove')))

    def test_remove_recursive(self):
        self.client.mkdir(bucket_url('test_remove_recursive'))
        self.client.put_string('hello', bucket_url('test_remove_recursive/1'))
        self.client.put_string('hello', bucket_url('test_remove_recursive/2'))
        self.client.remove(bucket_url('test_remove_recursive'))

        self.assertFalse(self.client.exists(bucket_url('test_remove_recursive')))
        self.assertFalse(self.client.exists(bucket_url('test_remove_recursive/1')))
        self.assertFalse(self.client.exists(bucket_url('test_remove_recursive/2')))

    def test_listdir(self):
        self.client.put_string('hello', bucket_url('test_listdir/1'))
        self.client.put_string('hello', bucket_url('test_listdir/2'))

        self.assertEqual([bucket_url('test_listdir/1'), bucket_url('test_listdir/2')],
                         list(self.client.listdir(bucket_url('test_listdir/'))))
        self.assertEqual([bucket_url('test_listdir/1'), bucket_url('test_listdir/2')],
                         list(self.client.listdir(bucket_url('test_listdir'))))

    def test_put_file(self):
        with tempfile.NamedTemporaryFile() as fp:
            lorem = b'Lorem ipsum dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh euismod tincidunt\n'
            # Larger file than chunk size, fails with incorrect progress set up
            big = lorem * 41943
            fp.write(big)
            fp.flush()

            self.client.put(fp.name, bucket_url('test_put_file'))
            self.assertTrue(self.client.exists(bucket_url('test_put_file')))
            self.assertEqual(big, self.client.download(bucket_url('test_put_file')).read())

    def test_put_file_multiproc(self):
        temporary_fps = []
        for _ in range(2):
            fp = tempfile.NamedTemporaryFile(mode='wb')

            lorem = b'Lorem ipsum dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh euismod tincidunt\n'
            # Larger file than chunk size, fails with incorrect progress set up
            big = lorem * 41943
            fp.write(big)
            fp.flush()
            temporary_fps.append(fp)

        filepaths = [f.name for f in temporary_fps]
        self.client.put_multiple(filepaths, bucket_url(''), num_process=2)

        for fp in temporary_fps:
            basename = os.path.basename(fp.name)
            self.assertTrue(self.client.exists(bucket_url(basename)))
            self.assertEqual(big, self.client.download(bucket_url(basename)).read())
            fp.close()


@pytest.mark.gcloud
class GCSTargetTest(_GCSBaseTestCase, FileSystemTargetTestMixin):

    def create_target(self, format=None):
        return gcs.GCSTarget(bucket_url(self.id()), format=format, client=self.client)

    def test_close_twice(self):
        # Ensure gcs._DeleteOnCloseFile().close() can be called multiple times
        tgt = self.create_target()

        with tgt.open('w') as dst:
            dst.write('data')
        assert dst.closed
        dst.close()
        assert dst.closed

        with tgt.open() as src:
            assert src.read().strip() == 'data'
        assert src.closed
        src.close()
        assert src.closed


class RetryTest(unittest.TestCase):
    def test_success_with_retryable_error(self):
        m = mock.MagicMock(side_effect=[IOError, IOError, 'test_func_output'])

        @gcs.gcs_retry
        def mock_func():
            return m()

        actual = mock_func()
        expected = 'test_func_output'
        self.assertEqual(expected, actual)

    def test_fail_with_retry_limit_exceed(self):
        m = mock.MagicMock(side_effect=[IOError, IOError, IOError, IOError, IOError])

        @gcs.gcs_retry
        def mock_func():
            return m()

        with self.assertRaises(IOError):
            mock_func()
