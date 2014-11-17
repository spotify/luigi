import datetime
import os
import posixpath
import time
import unittest2
import luigi.hdfs
import luigi.interface
from luigi.hdfs import SnakebiteHdfsClient
from snakebite.client import AutoConfigClient as SnakebiteAutoConfigClient
from nose.plugins.attrib import attr
from snakebite.minicluster import MiniCluster


@attr('minicluster')
class TestSnakebiteClient(unittest2.TestCase):
    """This test requires a snakebite -- it finds it from your
    client.cfg"""
    snakebite = None
    cluster = None

    @classmethod
    def setupClass(cls):
        if not cls.cluster:
            cls.cluster = MiniCluster(None, nnport=50030)

    @classmethod
    def tearDownClass(cls):
        if cls.cluster:
            cls.cluster.terminate()

    def get_client(self):
        return SnakebiteHdfsClient()

    def setUp(self):
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
