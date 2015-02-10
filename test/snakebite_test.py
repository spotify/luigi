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
import os
import posixpath
import time

from snakebite.client import AutoConfigClient as SnakebiteAutoConfigClient
from snakebite.minicluster import MiniCluster

import luigi.hdfs
import luigi.interface
from luigi.hdfs import SnakebiteHdfsClient
from minicluster import MiniClusterTestCase
from nose.plugins.attrib import attr

try:
    import unittest2 as unittest
except ImportError:
    import unittest


@attr('minicluster')
class TestSnakebiteClient(MiniClusterTestCase):

    """This test requires a snakebite -- it finds it from your
    client.cfg"""
    snakebite = None

    def get_client(self):
        return SnakebiteHdfsClient()

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

        self.assertFalse(self.snakebite.rename_dont_move(foo, bar))
        self.assertTrue(self.snakebite.exists(foo))
        self.assertTrue(self.snakebite.exists(bar))

        self.assertTrue(self.snakebite.rename_dont_move(foo, foo + '2'))
        self.assertFalse(self.snakebite.exists(foo))
        self.assertTrue(self.snakebite.exists(foo + '2'))
