import unittest
from spotify.luigi import hdfs

class TestException(Exception):
    pass

class AtomicHdfsOutputPipeTests(unittest.TestCase):
    def test_atomicity(self):
        testpath = "luigi_hdfs_testfile"
        if hdfs.exists(testpath):
            hdfs.remove(testpath)

        pipe = hdfs.AtomicHdfsOutputPipe(testpath)
        self.assertFalse(hdfs.exists(testpath))
        pipe.close()
        self.assertTrue(hdfs.exists(testpath))

    def test_with_close(self):
        testpath = "luigi_hdfs_testfile"
        if hdfs.exists(testpath):
            hdfs.remove(testpath)

        with hdfs.AtomicHdfsOutputPipe(testpath) as fobj:
            fobj.write('hej')

        self.assertTrue(hdfs.exists(testpath))

    def test_with_noclose(self):
        testpath = "luigi_hdfs_testfile"
        if hdfs.exists(testpath):
            hdfs.remove(testpath)

        def foo():
            with hdfs.AtomicHdfsOutputPipe(testpath) as fobj:
                fobj.write('hej')
                raise TestException('Test triggered exception')
        self.assertRaises(TestException, foo)
        self.assertFalse(hdfs.exists(testpath))


class HdfsTargetTests(unittest.TestCase):
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

    def test_with_subprocess_error(self):
        target = hdfs.HdfsTarget("luigi_hdfs_testfile", format=hdfs.AVRO)
        if target.exists():
            target.remove()

        def foo():
            with target.open('w') as fobj:
                fobj.write('hej')  # writing avro without line ending should break avro-write
        self.assertRaises(RuntimeError, foo)
        self.assertFalse(target.exists())

    def test_dir_atomicity(self):
        target = hdfs.HdfsTarget("luigi_hdfs_testdir", is_dir=True)
        if target.exists():
            target.remove()
        self.assertFalse(target.exists())
        with target.open('w') as fobj:
            fobj.write('hej\n')
            self.assertFalse(target.exists())
        self.assertTrue(target.exists())

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

if __name__ == "__main__":
    unittest.main()
