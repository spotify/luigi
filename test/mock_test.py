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

import unittest

from luigi.mock import MockFile, MockFileSystem


class MockFileTest(unittest.TestCase):

    def test_1(self):
        t = MockFile('test')
        p = t.open('w')
        print >> p, 'test'
        p.close()

        q = t.open('r')
        self.assertEqual(list(q), ['test\n'])
        q.close()

    def test_with(self):
        t = MockFile("foo")
        with t.open('w') as b:
            b.write("bar")

        with t.open('r') as b:
            self.assertEqual(list(b), ['bar'])


class MockFileSystemTest(unittest.TestCase):
    fs = MockFileSystem()

    def _touch(self, path):
        t = MockFile(path)
        with t.open('w'):
            pass

    def setUp(self):
        self.fs.clear()
        self.path = "/tmp/foo"
        self.path2 = "/tmp/bar"
        self._touch(self.path)
        self._touch(self.path2)

    def test_exists(self):
        self.assertTrue(self.fs.exists(self.path))

    def test_remove(self):
        self.fs.remove(self.path)
        self.assertFalse(self.fs.exists(self.path))

    def test_remove_recursive(self):
        self.fs.remove("/tmp", recursive=True)
        self.assertFalse(self.fs.exists(self.path))
        self.assertFalse(self.fs.exists(self.path2))

    def test_listdir(self):
        self.assertEqual(sorted([self.path, self.path2]), sorted(self.fs.listdir("/tmp")))
