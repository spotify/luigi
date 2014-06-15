import datetime
import os
import posixpath
import time
import unittest
import luigi.hdfs
import luigi.interface
from luigi.hdfs import SnakebiteHdfsClient
from snakebite.client import AutoConfigClient as SnakebiteAutoConfigClient


class TestSnakebiteClient(unittest.TestCase):
    """This test requires a snakebite -- it finds it from your
    client.cfg"""
    snakebite = None

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

    def test_relativepath(self):
        rel_test_dir = "." + os.path.split(self.testDir)[1]
        try:
            self.assertFalse(self.snakebite.exists(rel_test_dir))
            self.snakebite.mkdir(rel_test_dir)
            self.assertTrue(self.snakebite.exists(rel_test_dir))
        finally:
            if self.snakebite.exists(rel_test_dir):
                self.snakebite.remove(rel_test_dir, True)


class SnakebiteHdfsClientMock(SnakebiteHdfsClient):
    """ A pure python HDFS client that support HA and is auto configured through the ``HADOOP_PATH`` environment variable.
  
      This is fully backwards compatible with the vanilla Client and can be used for a non HA cluster as well.
      This client tries to read ``${HADOOP_PATH}/conf/hdfs-site.xml`` to get the address of the namenode.
      The behaviour is the same as Client.
    """
    def get_bite(self):
        self._bite = SnakebiteAutoConfigClient()
        return self._bite


class TestSnakebiteAutoConfigClient(TestSnakebiteClient):
    """This test requires a snakebite -- it finds it from your
    client.cfg"""

    def get_client(self):
        return SnakebiteHdfsClientMock()
