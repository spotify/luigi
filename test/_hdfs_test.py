import unittest
from spotify.luigi import hdfs


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
    def test_atomicity(self):
        target = hdfs.HdfsTarget("luigi_hdfs_testfile.avro", format=hdfs.Avro)
        if target.exists():
            target.remove()

        fobj = target.open("w")
        self.assertFalse(target.exists())
        fobj.close()
        self.assertTrue(target.exists())

    def test_dir_atomicity(self):
        target = hdfs.HdfsTarget("luigi_hdfs_testdir", format=hdfs.AvroDir)
        if target.exists():
            target.remove()
        self.assertFalse(target.exists())
        with target.open('w') as fobj:
            fobj.write('hej\n')
            self.assertFalse(target.exists())
        self.assertTrue(target.exists())

    def test_readback(self):
        target = hdfs.HdfsTarget("luigi_hdfs_testfile.avro", format=hdfs.Avro)
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
        target = hdfs.HdfsTarget("luigi_hdfs_testfile.avro", format=hdfs.Avro)
        if target.exists():
            target.remove()

        with target.open('w') as fobj:
            fobj.write('hej\n')

        self.assertTrue(target.exists())

    def test_with_exception(self):
        target = hdfs.HdfsTarget("luigi_hdfs_testfile.avro", format=hdfs.Avro)
        if target.exists():
            target.remove()

        def foo():
            with target.open('w') as fobj:
                fobj.write('hej\n')
                raise TestException('Test triggered exception')
        self.assertRaises(TestException, foo)
        self.assertFalse(target.exists())

    def test_with_subprocess_error(self):
        target = hdfs.HdfsTarget("luigi_hdfs_testfile.avro", format=hdfs.Avro)
        if target.exists():
            target.remove()

        def foo():
            with target.open('w') as fobj:
                fobj.write('hej')  # writing avro without line ending should break avro-write
        self.assertRaises(RuntimeError, foo)
        self.assertFalse(target.exists())

    def test_create_parents(self):
        parent = "luigi_hdfs_testdir"
        target = hdfs.HdfsTarget("%s/testfile" % parent, format=hdfs.Avro)
        if hdfs.exists(parent):
            hdfs.remove(parent)
        self.assertFalse(hdfs.exists(parent))
        fobj = target.open('w')
        fobj.write('lol\n')
        fobj.close()
        self.assertTrue(hdfs.exists(parent))
        self.assertTrue(target.exists())

    def test_avro_iteration(self):
        target = hdfs.HdfsTarget("luigi_hdfs_testfile.avro", format=hdfs.Avro)
        if target.exists():
            target.remove()
        with target.open('w') as fobj:
            print >> fobj, "lol"
        for line in target.open('r'):
            self.assertEqual(line, "lol\n")

    def test_avro_multifile_read(self):
        target = hdfs.HdfsTarget("luigi_hdfs_testdir", format=hdfs.AvroDir)
        if target.exists():
            target.remove()
        hdfs.mkdir(target.path)
        t1 = hdfs.HdfsTarget(target.path + "/part-00001.avro", format=hdfs.Avro)
        t2 = hdfs.HdfsTarget(target.path + "/part-00002.avro", format=hdfs.Avro)

        with t1.open('w') as f:
            f.write('foo\n')
        with t2.open('w') as f:
            f.write('bar\n')

        with target.open('r') as f:
            self.assertEqual(list(f), ['foo\n', 'bar\n'])

if __name__ == "__main__":
    unittest.main()
