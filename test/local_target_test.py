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

import bz2
import gzip
import os
import random
import shutil
import sys
from helpers import unittest
import mock

import luigi.format
from luigi import LocalTarget
from luigi.local_target import LocalFileSystem
from luigi.target import FileAlreadyExists, MissingParentDirectory
from target_test import FileSystemTargetTestMixin

import itertools
import io
from errno import EEXIST, EXDEV


class LocalTargetTest(unittest.TestCase, FileSystemTargetTestMixin):
    PATH_PREFIX = '/tmp/test.txt'

    def setUp(self):
        self.path = self.PATH_PREFIX + '-' + str(self.id())
        self.copy = self.PATH_PREFIX + '-copy-' + str(self.id())
        if os.path.exists(self.path):
            os.remove(self.path)
        if os.path.exists(self.copy):
            os.remove(self.copy)

    def tearDown(self):
        if os.path.exists(self.path):
            os.remove(self.path)
        if os.path.exists(self.copy):
            os.remove(self.copy)

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

    @unittest.skipIf(tuple(sys.version_info) < (3, 4), 'only for Python>=3.4')
    def test_pathlib(self):
        """Test work with pathlib.Path"""
        import pathlib
        path = pathlib.Path(self.path)
        self.assertFalse(path.exists())
        target = LocalTarget(path)
        self.assertFalse(target.exists())
        with path.open('w') as stream:
            stream.write('test me')
        self.assertTrue(target.exists())

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

    def test_move_across_filesystems(self):
        t = LocalTarget(self.path)
        with t.open('w') as f:
            f.write('test_data')

        def rename_across_filesystems(src, dst):
            err = OSError()
            err.errno = EXDEV
            raise err

        real_rename = os.rename

        def mockrename(src, dst):
            if '-across-fs' in src:
                real_rename(src, dst)
            else:
                rename_across_filesystems(src, dst)

        copy = '%s-across-fs' % self.copy
        with mock.patch('os.rename', mockrename):
            t.move(copy)

        self.assertFalse(os.path.exists(self.path))
        self.assertTrue(os.path.exists(copy))
        self.assertEqual('test_data', LocalTarget(copy).open('r').read())

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

    def theoretical_io_modes(
            self,
            rwax='rwax',
            bt=['', 'b', 't'],
            plus=['', '+']):
        p = itertools.product(rwax, plus, bt)
        return {''.join(c) for c in list(
            itertools.chain.from_iterable(
                [itertools.permutations(m) for m in p]))}

    def valid_io_modes(self, *a, **kw):
        modes = set()
        t = LocalTarget(is_tmp=True)
        t.open('w').close()
        for mode in self.theoretical_io_modes(*a, **kw):
            try:
                io.FileIO(t.path, mode).close()
            except ValueError:
                pass
            except IOError as err:
                if err.errno == EEXIST:
                    modes.add(mode)
                else:
                    raise
            else:
                modes.add(mode)
        return modes

    def valid_write_io_modes_for_luigi(self):
        return self.valid_io_modes('w', plus=[''])

    def valid_read_io_modes_for_luigi(self):
        return self.valid_io_modes('r', plus=[''])

    def invalid_io_modes_for_luigi(self):
        return self.valid_io_modes().difference(
            self.valid_write_io_modes_for_luigi(),
            self.valid_read_io_modes_for_luigi())

    def test_open_modes(self):
        t = LocalTarget(is_tmp=True)
        print('Valid write mode:', end=' ')
        for mode in self.valid_write_io_modes_for_luigi():
            print(mode, end=' ')
            p = t.open(mode)
            p.close()
        print()
        print('Valid read mode:', end=' ')
        for mode in self.valid_read_io_modes_for_luigi():
            print(mode, end=' ')
            p = t.open(mode)
            p.close()
        print()
        print('Invalid mode:', end=' ')
        for mode in self.invalid_io_modes_for_luigi():
            print(mode, end=' ')
            self.assertRaises(Exception, t.open, mode)
        print()


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

    def test_copy(self):
        src = os.path.join(self.path, 'src.txt')
        dest = os.path.join(self.path, 'newdir', 'dest.txt')

        LocalTarget(src).open('w').close()
        self.fs.copy(src, dest)
        self.assertTrue(os.path.exists(src))
        self.assertTrue(os.path.exists(dest))

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
        with open(self.path + '/file', 'w'):
            pass
        self.assertTrue([self.path + '/file'], list(self.fs.listdir(self.path + '/')))

    def test_move_to_new_dir(self):
        # Regression test for a bug in LocalFileSystem.move
        src = os.path.join(self.path, 'src.txt')
        dest = os.path.join(self.path, 'newdir', 'dest.txt')

        LocalTarget(src).open('w').close()
        self.fs.move(src, dest)
        self.assertTrue(os.path.exists(dest))
