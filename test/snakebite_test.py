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

import datetime
import getpass
import os
import posixpath
import time
import unittest

import luigi.target
from luigi import six
from nose.plugins.attrib import attr

raise unittest.SkipTest("snakebite doesn't work on Python 3 yet.")

try:
    from luigi.contrib.hdfs import SnakebiteHdfsClient
    from minicluster import MiniClusterTestCase
except ImportError:
    raise unittest.SkipTest('Snakebite not installed')


@attr('minicluster')
class TestSnakebiteClient(MiniClusterTestCase):

    """This test requires a snakebite -- it finds it from your
    luigi.cfg"""
    snakebite = None

    def get_client(self):
        return SnakebiteHdfsClient()

    """
    This hdfs client is used for writing file to hdfs and
    then getting merge of it using snakebite
    """
    def get_hdfs_client(self):
        return luigi.contrib.hdfs.create_hadoopcli_client()

    def setUp(self):
        """ We override setUp because we want to also use snakebite for
        creating the testing directory.  """
        self.testDir = "/tmp/luigi-test-{0}-{1}".format(
            os.environ["USER"],
            time.mktime(datetime.datetime.now().timetuple())
        )
        self.snakebite = self.get_client()
        self.assertTrue(self.snakebite.mkdir(self.testDir))

    def tearDown(self):
        if self.snakebite.exists(self.testDir):
            self.snakebite.remove(self.testDir, True)

    def test_get_merge(self):
        hdfs_client = self.get_hdfs_client()
        local_filename = "file2.dat"

        target_dir = '/tmp/luigi_tmp_testdir_%s' % getpass.getuser()
        local_target1 = luigi.LocalTarget(local_filename)
        target1 = hdfs_client.put_file(local_target1, local_filename, target_dir)
        self.assertTrue(target1.exists())

        local_dir = "test/data"
        local_copy_path = "%s/file.dat.cp" % (local_dir)
        local_copy = luigi.LocalTarget(local_copy_path)
        if local_copy.exists():
            local_copy.remove()
        self.snakebite.get_merge(target_dir, local_copy_path)
        self.assertTrue(local_copy.exists())
        local_copy.remove()

    def test_exists(self):
        self.assertTrue(self.snakebite.exists(self.testDir))

    def test_rename(self):
        foo = posixpath.join(self.testDir, "foo")
        bar = posixpath.join(self.testDir, "bar")
        self.assertTrue(self.snakebite.mkdir(foo))
        self.assertTrue(self.snakebite.rename(foo, bar))
        self.assertTrue(self.snakebite.exists(bar))

    def test_rename_trailing_slash(self):
        foo = posixpath.join(self.testDir, "foo")
        bar = posixpath.join(self.testDir, "bar/")
        self.assertTrue(self.snakebite.mkdir(foo))
        self.assertTrue(self.snakebite.rename(foo, bar))
        self.assertTrue(self.snakebite.exists(bar))
        self.assertFalse(self.snakebite.exists(posixpath.join(bar, 'foo')))

    def test_relativepath(self):
        rel_test_dir = "." + os.path.split(self.testDir)[1]
        try:
            self.assertFalse(self.snakebite.exists(rel_test_dir))
            self.snakebite.mkdir(rel_test_dir)
            self.assertTrue(self.snakebite.exists(rel_test_dir))
        finally:
            if self.snakebite.exists(rel_test_dir):
                self.snakebite.remove(rel_test_dir, True)

    def test_rename_dont_move(self):
        foo = posixpath.join(self.testDir, "foo")
        bar = posixpath.join(self.testDir, "bar")
        self.assertTrue(self.snakebite.mkdir(foo))
        self.assertTrue(self.snakebite.mkdir(bar))
        self.assertTrue(self.snakebite.exists(foo))  # For sanity
        self.assertTrue(self.snakebite.exists(bar))  # For sanity

        self.assertRaises(luigi.target.FileAlreadyExists,
                          lambda: self.snakebite.rename_dont_move(foo, bar))
        self.assertTrue(self.snakebite.exists(foo))
        self.assertTrue(self.snakebite.exists(bar))

        self.snakebite.rename_dont_move(foo, foo + '2')
        self.assertFalse(self.snakebite.exists(foo))
        self.assertTrue(self.snakebite.exists(foo + '2'))
