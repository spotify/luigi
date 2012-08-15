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

if __name__ == "__main__":
    unittest.main()
