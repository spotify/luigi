import os
import datetime
import posixpath
import time
import unittest

from luigi.contrib import webhdfs


class TestWebHdfsTarget(unittest.TestCase):
    '''
    This test requires a running Hadoop cluster with WebHdfs enabled
    This test requires the client.cfg file to have a `hdfs` section
    with the namenode_host, namenode_port and user settings.
    '''

    def setUp(self):
        self.testDir = "/tmp/luigi-test".format()
        self.path = os.path.join(self.testDir, 'out.txt')
        self.client = webhdfs.WebHdfsClient()
        self.target = webhdfs.WebHdfsTarget(self.path)

    def tearDown(self):
        if self.client.exists(self.testDir):
            self.client.remove(self.testDir, recursive=True)

    def test_write(self):
        self.assertFalse(self.client.exists(self.path))
        output = self.target.open('w')
        output.write('this is line 1\n')
        output.write('this is line #2\n')
        output.close()
        self.assertTrue(self.client.exists(self.path))

    def test_read(self):
        self.test_write()
        input_ = self.target.open('r')
        all_test = 'this is line 1\nthis is line #2\n'
        self.assertEqual(all_test, input_.read())
        input_.close()

    def test_read_lines(self):
        self.test_write()
        input_ = self.target.open('r')
        lines = list(input_.readlines())
        self.assertEqual(lines[0], 'this is line 1')
        self.assertEqual(lines[1], 'this is line #2')
        input_.close()
