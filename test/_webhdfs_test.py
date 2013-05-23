import datetime
import os
import posixpath
import time
import unittest
import luigi.hdfs
import luigi.interface
import luigi.webhdfs
import whoops


class TestConfig(unittest.TestCase):
    def test_no_config(self):
        class TestConfigParser(luigi.interface.LuigiConfigParser):
            _config_paths = []
            _instance = None

        config = TestConfigParser.instance()
        self.assertRaises(
            RuntimeError,
            luigi.webhdfs.get_whoops_defaults,
            config)

    def test_config(self):
        class TestConfigParser(luigi.interface.LuigiConfigParser):
            _config_paths = []
            _instance = None
        host = "webhdfs-test"
        port = "9999"
        config = TestConfigParser.instance()
        config.add_section("hdfs")
        config.set("hdfs", "namenode_host", host)
        config.set("hdfs", "namenode_port", port)
        self.assertEquals({"host": host, "port": port},
                          luigi.webhdfs.get_whoops_defaults(config))


class TestGetWhoops(unittest.TestCase):
    host = "webhdfs-test"
    port = "9999"

    def setUp(self):
        class TestConfigParser(luigi.interface.LuigiConfigParser):
            _config_paths = []
            _instance = None
        self.config = TestConfigParser.instance()
        self.config.add_section("hdfs")
        self.config.set("hdfs", "namenode_host", self.host)
        self.config.set("hdfs", "namenode_port", self.port)

    def test_get_relative_path(self):
        w = luigi.webhdfs.get_whoops("/tmp", self.config)
        self.assertEquals(self.host, w.host)
        self.assertEquals(self.port, w.port)

    def test_get_abs_path(self):
        w = luigi.webhdfs.get_whoops("hdfs://footesthdfs:8020/bar",
                                     self.config)
        self.assertEquals("footesthdfs", w.host)
        self.assertEquals("8020", w.port)

    def test_get_malformed_path(self):
        self.assertRaises(
            RuntimeError,
            luigi.webhdfs.get_whoops,
            "hdfs://foo/bar")

    def test_get_wrong_scheme(self):
        self.assertRaises(
            RuntimeError,
            luigi.webhdfs.get_whoops,
            "s3n://foo:80/bar"
        )


class TestWebHDFSClient(unittest.TestCase):
    """This test requires a running webhdfs -- it finds it from your
    client.cfg"""

    def setUp(self):
        self.testDir = "/tmp/luigi-test-{0}-{1}".format(
            os.environ["USER"],
            time.mktime(datetime.datetime.now().timetuple())
        )

    def tearDown(self):
        if luigi.webhdfs.exists(self.testDir):
            luigi.webhdfs.remove(self.testDir, True)

    def test_exists(self):
        self.assertFalse(luigi.webhdfs.exists(self.testDir))
        self.assertTrue(luigi.webhdfs.mkdir(self.testDir))
        self.assertTrue(luigi.webhdfs.exists(self.testDir))

    def test_rename(self):
        foo = posixpath.join(self.testDir, "foo")
        bar = posixpath.join(self.testDir, "bar")
        self.assertTrue(luigi.webhdfs.mkdir(foo))
        self.assertTrue(luigi.webhdfs.rename(foo, bar))
        self.assertTrue(luigi.webhdfs.exists(bar))

    def test_remove(self):
        foo = posixpath.join(self.testDir, "foo")
        foobar = posixpath.join(foo, "bar")
        self.assertTrue(luigi.webhdfs.mkdir(foo))
        self.assertTrue(luigi.webhdfs.mkdir(foobar))
        self.assertRaises(whoops.WebHDFSError,
                          luigi.webhdfs.remove,
                          foo, recursive=False)
        self.assertTrue(luigi.webhdfs.remove(foo, recursive=True))

    def test_listdir(self):
        foo = posixpath.join(self.testDir, "foo")
        foobar = posixpath.join(foo, "bar")
        # whoops' put() support seems to be broken.
        foobaz = luigi.hdfs.HdfsTarget(posixpath.join(foo, "baz"))

        self.assertTrue(luigi.webhdfs.mkdir(foo))
        self.assertTrue(luigi.webhdfs.mkdir(foobar))

        with foobaz.open('w') as t:
            t.write("testing")

        results = luigi.webhdfs.listdir(foo)
        self.assertEquals(set([foobar, foobaz.path]), set(results))
        results = luigi.webhdfs.listdir(foo, ignore_directories=True)
        self.assertEquals([foobaz.path], list(results))
        results = luigi.webhdfs.listdir(foo, ignore_files=True)
        self.assertEquals([foobar], list(results))
        results = luigi.webhdfs.listdir(foo,
                                        include_size=True, include_type=True)
        self.assertEquals(set([(foobar, 0, 'd'), (foobaz.path, 7, '-')]),
                          set(results))

    def test_relativepath(self):
        rel_test_dir = "." + os.path.split(self.testDir)[1]
        try:
            self.assertFalse(luigi.webhdfs.exists(rel_test_dir))
            luigi.webhdfs.mkdir(rel_test_dir)
            self.assertTrue(luigi.webhdfs.exists(rel_test_dir))
        finally:
            if luigi.webhdfs.exists(rel_test_dir):
                luigi.webhdfs.remove(rel_test_dir, True)
