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
from __future__ import print_function

import bz2
import gzip
import os
import random
import shutil

import mock

# python 3 support
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

from helpers import unittest
import luigi.format
from luigi import LocalTarget
from luigi.file import LocalFileSystem
from luigi.target import FileAlreadyExists, MissingParentDirectory
from target_test import FileSystemTargetTestMixin


class LocalTargetTest(unittest.TestCase, FileSystemTargetTestMixin):
    path = '/tmp/test.txt'
    copy = '/tmp/test.copy.txt'

    def _cleanup_files(self):
        if os.path.exists(self.path):
            if os.path.isdir(self.path):
                shutil.rmtree(self.path)
            else:
                os.remove(self.path)
        if os.path.exists(self.copy):
            os.remove(self.copy)

    def setUp(self):
        self._cleanup_files()

    def tearDown(self):
        self._cleanup_files()

    def create_target(self, format=None):
        return LocalTarget(self.path, format=format)

    def assertCleanUp(self, tmp_path=''):
        self.assertFalse(os.path.exists(tmp_path))

    def test_exists(self):
        t = self.create_target()
        p = t.open('w')
        self.assertEqual(t.exists(), os.path.exists(self.path))
        p.close()
        self.assertEqual(t.exists(), os.path.exists(self.path))

    def test_gzip_with_module(self):
        t = LocalTarget(self.path, luigi.format.Gzip)
        p = t.open('w')
        test_data = b'test'
        p.write(test_data)
        print(self.path)
        self.assertFalse(os.path.exists(self.path))
        p.close()
        self.assertTrue(os.path.exists(self.path))

        # Using gzip module as validation
        f = gzip.open(self.path, 'r')
        self.assertTrue(test_data == f.read())
        f.close()

        # Verifying our own gzip reader
        f = LocalTarget(self.path, luigi.format.Gzip).open('r')
        self.assertTrue(test_data == f.read())
        f.close()

    def test_bzip2(self):
        t = LocalTarget(self.path, luigi.format.Bzip2)
        p = t.open('w')
        test_data = b'test'
        p.write(test_data)
        print(self.path)
        self.assertFalse(os.path.exists(self.path))
        p.close()
        self.assertTrue(os.path.exists(self.path))

        # Using bzip module as validation
        f = bz2.BZ2File(self.path, 'r')
        self.assertTrue(test_data == f.read())
        f.close()

        # Verifying our own bzip2 reader
        f = LocalTarget(self.path, luigi.format.Bzip2).open('r')
        self.assertTrue(test_data == f.read())
        f.close()

    def test_copy(self):
        t = LocalTarget(self.path)
        f = t.open('w')
        test_data = 'test'
        f.write(test_data)
        f.close()
        self.assertTrue(os.path.exists(self.path))
        self.assertFalse(os.path.exists(self.copy))
        t.copy(self.copy)
        self.assertTrue(os.path.exists(self.path))
        self.assertTrue(os.path.exists(self.copy))
        self.assertEqual(t.open('r').read(), LocalTarget(self.copy).open('r').read())

    def test_move(self):
        t = LocalTarget(self.path)
        f = t.open('w')
        test_data = 'test'
        f.write(test_data)
        f.close()
        self.assertTrue(os.path.exists(self.path))
        self.assertFalse(os.path.exists(self.copy))
        t.move(self.copy)
        self.assertFalse(os.path.exists(self.path))
        self.assertTrue(os.path.exists(self.copy))

    def test_format_chain(self):
        UTF8WIN = luigi.format.TextFormat(encoding='utf8', newline='\r\n')
        t = LocalTarget(self.path, UTF8WIN >> luigi.format.Gzip)
        a = u'我é\nçф'

        with t.open('w') as f:
            f.write(a)

        f = gzip.open(self.path, 'rb')
        b = f.read()
        f.close()

        self.assertEqual(b'\xe6\x88\x91\xc3\xa9\r\n\xc3\xa7\xd1\x84', b)

    def test_format_chain_reverse(self):
        t = LocalTarget(self.path, luigi.format.UTF8 >> luigi.format.Gzip)

        f = gzip.open(self.path, 'wb')
        f.write(b'\xe6\x88\x91\xc3\xa9\r\n\xc3\xa7\xd1\x84')
        f.close()

        with t.open('r') as f:
            b = f.read()

        self.assertEqual(u'我é\nçф', b)

    @mock.patch('os.linesep', '\r\n')
    def test_format_newline(self):
        t = LocalTarget(self.path, luigi.format.SysNewLine)

        with t.open('w') as f:
            f.write(b'a\rb\nc\r\nd')

        with t.open('r') as f:
            b = f.read()

        with open(self.path, 'rb') as f:
            c = f.read()

        self.assertEqual(b'a\nb\nc\nd', b)
        self.assertEqual(b'a\r\nb\r\nc\r\nd', c)

    def test_directory_with_split(self):
        directory_format = luigi.format.DirectoryFormat(max_part_size=3)
        t = LocalTarget(self.path, is_dir=True, format=directory_format)
        p = t.open('w')
        test_data = b'test'
        p.write(test_data)
        print(self.path)
        self.assertFalse(os.path.exists(self.path))
        p.close()
        self.assertTrue(os.path.exists(self.path))

        # Validate split
        self.assertEquals(os.listdir(self.path), ['part-00', 'part-01'])

        # Verifying our own directory reader
        f = LocalTarget(self.path, is_dir=True, format=directory_format).open('r')
        self.assertTrue(test_data == f.read())
        f.close()

    def test_directory_with_gzip_split(self):
        directory_format = luigi.format.Gzip >> luigi.format.DirectoryFormat(max_part_size=10, suffix=".gz")
        t = LocalTarget(self.path, is_dir=True, format=directory_format)
        p = t.open('w')
        test_data = b'test'
        p.write(test_data)
        print(self.path)
        self.assertFalse(os.path.exists(self.path))
        p.close()
        self.assertTrue(os.path.exists(self.path))

        # Validate split
        self.assertEquals(os.listdir(self.path), ['part-00.gz', 'part-01.gz', 'part-02.gz'])

        # Verifying our own directory reader
        f = LocalTarget(self.path, is_dir=True, format=directory_format).open('r')
        self.assertTrue(test_data == f.read())
        f.close()

        # Verifying using gzip
        with LocalTarget(self.path, is_dir=True, format=luigi.format.DirectoryFormat(suffix=".gz")).open('r') as fp:
            v = StringIO(fp.read())
            with gzip.GzipFile(fileobj=v, mode='rb') as gp:
                self.assertEqual(test_data, gp.read())

    def test_atomicity_dir_simple(self):
        test_data = 'test'
        target = LocalTarget(self.path, is_dir=True, format=luigi.format.DirectoryFormat(max_part_size=3))
        with target.open("w") as f:
            self.assertFalse(target.exists())
            f.write(test_data)
        self.assertTrue(target.exists())
        self.assertTrue(target.open().read(), test_data)

    def test_atomicity_dir_with_error(self):
        test_data = 'test'
        target = LocalTarget(self.path, is_dir=True, format=luigi.format.DirectoryFormat(max_part_size=3))

        def raises_error():
            with target.open("w") as f:
                self.assertFalse(target.exists())
                f.write(test_data)
                raise Exception("My Error")

        self.assertRaisesRegexp(Exception, "My Error", raises_error)
        self.assertFalse(target.exists())


class LocalTargetCreateDirectoriesTest(LocalTargetTest):
    path = '/tmp/%s/xyz/test.txt' % random.randint(0, 999999999)
    copy = '/tmp/%s/xyz_2/copy.txt' % random.randint(0, 999999999)


class LocalTargetRelativeTest(LocalTargetTest):
    # We had a bug that caused relative file paths to fail, adding test for it
    path = 'test.txt'
    copy = 'copy.txt'


class TmpFileTest(unittest.TestCase):

    def test_tmp(self):
        t = LocalTarget(is_tmp=True)
        self.assertFalse(t.exists())
        self.assertFalse(os.path.exists(t.path))
        p = t.open('w')
        print('test', file=p)
        self.assertFalse(t.exists())
        self.assertFalse(os.path.exists(t.path))
        p.close()
        self.assertTrue(t.exists())
        self.assertTrue(os.path.exists(t.path))

        q = t.open('r')
        self.assertEqual(q.readline(), 'test\n')
        q.close()
        path = t.path
        del t  # should remove the underlying file
        self.assertFalse(os.path.exists(path))


class FileSystemTest(unittest.TestCase):
    path = '/tmp/luigi-test-dir'
    fs = LocalFileSystem()

    def setUp(self):
        if os.path.exists(self.path):
            shutil.rmtree(self.path)

    def tearDown(self):
        self.setUp()

    def test_mkdir(self):
        testpath = os.path.join(self.path, 'foo/bar')

        self.assertRaises(MissingParentDirectory, self.fs.mkdir, testpath, parents=False)

        self.fs.mkdir(testpath)
        self.assertTrue(os.path.exists(testpath))
        self.assertTrue(self.fs.isdir(testpath))

        self.assertRaises(FileAlreadyExists, self.fs.mkdir, testpath, raise_if_exists=True)

    def test_exists(self):
        self.assertFalse(self.fs.exists(self.path))
        os.mkdir(self.path)
        self.assertTrue(self.fs.exists(self.path))
        self.assertTrue(self.fs.isdir(self.path))

    def test_listdir(self):
        os.mkdir(self.path)
        with open(self.path + '/file', 'w') as fp:
            pass
        self.assertTrue([self.path + '/file'], list(self.fs.listdir(self.path + '/')))


class TestImportFile(unittest.TestCase):

    def test_file(self):
        from luigi.file import File
        self.assertTrue(isinstance(File('foo'), LocalTarget))
