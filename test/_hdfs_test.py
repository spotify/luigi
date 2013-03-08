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

import unittest
from luigi import hdfs


class TestException(Exception):
    pass


class AtomicHdfsOutputPipeTests(unittest.TestCase):
    def test_atomicity(self):
        testpath = "luigi_hdfs_testfile"
        if hdfs.exists(testpath):
            hdfs.remove(testpath)

        pipe = hdfs.HdfsAtomicWritePipe(testpath)
        self.assertFalse(hdfs.exists(testpath))
        pipe.close()
        self.assertTrue(hdfs.exists(testpath))

    def test_with_close(self):
        testpath = "luigi_hdfs_testfile"
        if hdfs.exists(testpath):
            hdfs.remove(testpath)

        with hdfs.HdfsAtomicWritePipe(testpath) as fobj:
            fobj.write('hej')

        self.assertTrue(hdfs.exists(testpath))

    def test_with_noclose(self):
        testpath = "luigi_hdfs_testfile"
        if hdfs.exists(testpath):
            hdfs.remove(testpath)

        def foo():
            with hdfs.HdfsAtomicWritePipe(testpath) as fobj:
                fobj.write('hej')
                raise TestException('Test triggered exception')
        self.assertRaises(TestException, foo)
        self.assertFalse(hdfs.exists(testpath))


class HdfsAtomicWriteDirPipeTests(unittest.TestCase):
    def setUp(self):
        self.path = "luigi_hdfs_testfile"
        if hdfs.exists(self.path):
            hdfs.remove(self.path)

    def test_atomicity(self):
        pipe = hdfs.HdfsAtomicWriteDirPipe(self.path)
        self.assertFalse(hdfs.exists(self.path))
        pipe.close()
        self.assertTrue(hdfs.exists(self.path))

    def test_readback(self):
        pipe = hdfs.HdfsAtomicWriteDirPipe(self.path)
        self.assertFalse(hdfs.exists(self.path))
        pipe.write("foo\nbar")
        pipe.close()
        self.assertTrue(hdfs.exists(self.path))
        dirlist = tuple(hdfs.listdir(self.path))
        datapath = '%s/data' % self.path
        self.assertEquals(dirlist, (datapath,))
        pipe = hdfs.HdfsReadPipe(datapath)
        self.assertEqual(pipe.read(), "foo\nbar")

    def test_with_close(self):
        with hdfs.HdfsAtomicWritePipe(self.path) as fobj:
            fobj.write('hej')

        self.assertTrue(hdfs.exists(self.path))

    def test_with_noclose(self):
        def foo():
            with hdfs.HdfsAtomicWritePipe(self.path) as fobj:
                fobj.write('hej')
                raise TestException('Test triggered exception')
        self.assertRaises(TestException, foo)
        self.assertFalse(hdfs.exists(self.path))


class _HdfsFormatTest(unittest.TestCase):
    format = None  # override with luigi.format.Format subclass

    def setUp(self):
        self.target = hdfs.HdfsTarget("luigi_hdfs_testfile", format=self.format)
        if self.target.exists():
            self.target.remove()

    def test_with_write_success(self):
        with self.target.open('w') as fobj:
            fobj.write('foo')
        self.assertTrue(self.target.exists())

    def test_with_write_failure(self):
        def dummy():
            with self.target.open('w') as fobj:
                fobj.write('foo')
                raise TestException()

        self.assertRaises(TestException, dummy)
        self.assertFalse(self.target.exists())


class PlainFormatTest(_HdfsFormatTest):
    format = hdfs.Plain


class PlainDirFormatTest(_HdfsFormatTest):
    format = hdfs.PlainDir

    def test_multifile(self):
        with self.target.open('w') as fobj:
            fobj.write('foo\n')
        second = hdfs.HdfsTarget(self.target.path + '/data2', format=hdfs.Plain)

        with second.open('w') as fobj:
            fobj.write('bar\n')
        invisible = hdfs.HdfsTarget(self.target.path + '/_SUCCESS', format=hdfs.Plain)
        with invisible.open('w') as fobj:
            fobj.write('b0rk\n')
        self.assertTrue(second.exists())
        self.assertTrue(invisible.exists())
        self.assertTrue(self.target.exists())
        with self.target.open('r') as fobj:
            parts = fobj.read().strip('\n').split('\n')
            parts.sort()
        self.assertEqual(tuple(parts), ('bar', 'foo'))


class HdfsTargetTests(unittest.TestCase):

    def test_slow_exists(self):
        target = hdfs.HdfsTarget("luigi_hdfs_testfile")
        try:
            target.remove()
        except:
            pass

        self.assertFalse(hdfs.exists(target.path))
        target.open("w").close()
        self.assertTrue(hdfs.exists(target.path))

        def should_raise():
            hdfs.exists("hdfs://doesnotexist/foo")
        self.assertRaises(RuntimeError, should_raise)

        def should_raise_2():
            hdfs.exists("hdfs://_doesnotexist_/foo")
        self.assertRaises(RuntimeError, should_raise_2)

    def test_atomicity(self):
        target = hdfs.HdfsTarget("luigi_hdfs_testfile")
        if target.exists():
            target.remove()

        fobj = target.open("w")
        self.assertFalse(target.exists())
        fobj.close()
        self.assertTrue(target.exists())

    def test_readback(self):
        target = hdfs.HdfsTarget("luigi_hdfs_testfile")
        if target.exists():
            target.remove()

        origdata = 'lol\n'
        fobj = target.open("w")
        fobj.write(origdata)
        fobj.close()

        fobj = target.open('r')
        data = fobj.read()
        self.assertEqual(origdata, data)

    def test_with_close(self):
        target = hdfs.HdfsTarget("luigi_hdfs_testfile")
        if target.exists():
            target.remove()

        with target.open('w') as fobj:
            fobj.write('hej\n')

        self.assertTrue(target.exists())

    def test_with_exception(self):
        target = hdfs.HdfsTarget("luigi_hdfs_testfile")
        if target.exists():
            target.remove()

        def foo():
            with target.open('w') as fobj:
                fobj.write('hej\n')
                raise TestException('Test triggered exception')
        self.assertRaises(TestException, foo)
        self.assertFalse(target.exists())

    def test_create_parents(self):
        parent = "luigi_hdfs_testdir"
        target = hdfs.HdfsTarget("%s/testfile" % parent)
        if hdfs.exists(parent):
            hdfs.remove(parent)
        self.assertFalse(hdfs.exists(parent))
        fobj = target.open('w')
        fobj.write('lol\n')
        fobj.close()
        self.assertTrue(hdfs.exists(parent))
        self.assertTrue(target.exists())

    def test_tmp_cleanup(self):
        path = "luigi_hdfs_testfile"
        target = hdfs.HdfsTarget(path, is_tmp=True)
        if target.exists():
            target.remove()
        with target.open('w') as fobj:
            fobj.write('lol\n')
        self.assertTrue(target.exists())
        del target
        import gc
        gc.collect()
        self.assertFalse(hdfs.exists(path))

    def test_luigi_tmp(self):
        target = hdfs.HdfsTarget(is_tmp=True)
        self.assertFalse(target.exists())
        with target.open('w'):
            pass
        self.assertTrue(target.exists())

    def test_tmp_move(self):
        target = hdfs.HdfsTarget(is_tmp=True)
        target2 = hdfs.HdfsTarget("luigi_hdfs_testdir")
        if target2.exists():
            target2.remove()
        with target.open('w'):
            pass
        self.assertTrue(target.exists())
        target.move(target2.path)
        self.assertFalse(target.exists())
        self.assertTrue(target2.exists())

    def test_rename_no_parent(self):
        if hdfs.exists("foo"):
            hdfs.remove("foo")

        target1 = hdfs.HdfsTarget(is_tmp=True)
        target2 = hdfs.HdfsTarget("foo/bar")
        with target1.open('w'):
            pass
        self.assertTrue(target1.exists())
        target1.move(target2.path)
        self.assertFalse(target1.exists())
        self.assertTrue(target2.exists())

    def test_glob_exists(self):
        target = hdfs.HdfsTarget("luigi_hdfs_testdir")
        if target.exists():
            target.remove()
        hdfs.mkdir(target.path)
        t1 = hdfs.HdfsTarget(target.path + "/part-00001")
        t2 = hdfs.HdfsTarget(target.path + "/part-00002")
        t3 = hdfs.HdfsTarget(target.path + "/another")

        with t1.open('w') as f:
            f.write('foo\n')
        with t2.open('w') as f:
            f.write('bar\n')
        with t3.open('w') as f:
            f.write('biz\n')

        files = hdfs.HdfsTarget("luigi_hdfs_testdir/part-0000*")

        self.assertEqual(files.glob_exists(2), True)
        self.assertEqual(files.glob_exists(3), False)
        self.assertEqual(files.glob_exists(1), False)


class TouchTests(unittest.TestCase):
    def setUp(self):
        target_dir = hdfs.HdfsTarget(is_tmp=True)
        self.touch_file = target_dir.path + "/touchz"

    def testUnsafe(self):
        hdfs.touch(self.touch_file)

        def shouldRaise():
            hdfs.touch(self.touch_file)
        self.assertRaises(RuntimeError, shouldRaise)

    def testSafe(self):
        hdfs.touch(self.touch_file)
        hdfs.touch(self.touch_file, True)


if __name__ == "__main__":
    unittest.main()
