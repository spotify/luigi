# Copyright (c) 2012 Spotify AB
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

from luigi import File
from luigi.file import LocalFileSystem 
import unittest
import os
import gzip
import luigi.format
import random
import gc
import shutil


class FileTest(unittest.TestCase):
    path = '/tmp/test.txt'
    copy = '/tmp/test.copy.txt'

    def setUp(self):
        if os.path.exists(self.path):
            os.remove(self.path)
        if os.path.exists(self.copy):
            os.remove(self.copy)

    def tearDown(self):
        if os.path.exists(self.path):
            os.remove(self.path)
        if os.path.exists(self.copy):
            os.remove(self.copy)

    def test_close(self):
        t = File(self.path)
        p = t.open('w')
        print >> p, 'test'
        self.assertFalse(os.path.exists(self.path))
        p.close()
        self.assertTrue(os.path.exists(self.path))

    def test_del(self):
        t = File(self.path)
        p = t.open('w')
        print >> p, 'test'
        tp = p.tmp_path
        del p

        self.assertFalse(os.path.exists(tp))
        self.assertFalse(os.path.exists(self.path))

    def test_write_cleanup_no_close(self):
        t = File(self.path)

        def context():
            f = t.open('w')
            f.write('stuff')
        context()
        gc.collect()  # force garbage collection of f variable
        self.assertFalse(t.exists())

    def test_write_cleanup_with_error(self):
        t = File(self.path)
        try:
            with t.open('w'):
                raise Exception('something broke')
        except:
            pass
        self.assertFalse(t.exists())

    def test_gzip(self):
        t = File(self.path, luigi.format.Gzip)
        p = t.open('w')
        test_data = 'test'
        p.write(test_data)
        print self.path
        self.assertFalse(os.path.exists(self.path))
        p.close()
        self.assertTrue(os.path.exists(self.path))

        # Using gzip module as validation
        f = gzip.open(self.path, 'rb')
        self.assertTrue(test_data == f.read())
        f.close()

        # Verifying our own gzip reader
        f = File(self.path, luigi.format.Gzip).open('r')
        self.assertTrue(test_data == f.read())
        f.close()

    def test_copy(self):
        t = File(self.path)
        f = t.open('w')
        test_data = 'test'
        f.write(test_data)
        f.close()
        self.assertTrue(os.path.exists(self.path))
        self.assertFalse(os.path.exists(self.copy))
        t.copy(self.copy)
        self.assertTrue(os.path.exists(self.path))
        self.assertTrue(os.path.exists(self.copy))
        self.assertEqual(t.open('r').read(), File(self.copy).open('r').read())

    def test_format_injection(self):
        class CustomFormat(luigi.format.Format):
            def pipe_reader(self, input_pipe):
                input_pipe.foo = "custom read property"
                return input_pipe

            def pipe_writer(self, output_pipe):
                output_pipe.foo = "custom write property"
                return output_pipe

        t = File(self.path, format=CustomFormat())
        with t.open("w") as f:
            self.assertEqual(f.foo, "custom write property")

        with t.open("r") as f:
            self.assertEqual(f.foo, "custom read property")


class FileCreateDirectoriesTest(FileTest):
    path = '/tmp/%s/xyz/test.txt' % random.randint(0, 999999999)


class FileRelativeTest(FileTest):
    # We had a bug that caused relative file paths to fail, adding test for it
    path = 'test.txt'


class TmpFileTest(unittest.TestCase):
    def test_tmp(self):
        t = File(is_tmp=True)
        self.assertFalse(t.exists())
        self.assertFalse(os.path.exists(t.path))
        p = t.open('w')
        print >> p, 'test'
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


class TestFileSystem(unittest.TestCase):
    path = '/tmp/luigi-test-dir'
    fs = LocalFileSystem()

    def setUp(self):
        if os.path.exists(self.path):
            shutil.rmtree(self.path)

    def tearDown(self):
        self.setUp()

    def test_mkdir(self):
        testpath = os.path.join(self.path, 'foo/bar')
        self.fs.mkdir(testpath)
        self.assertTrue(os.path.exists(testpath))

    def test_exists(self):
        self.assertFalse(self.fs.exists(self.path))
        os.mkdir(self.path)
        self.assertTrue(self.fs.exists(self.path))
