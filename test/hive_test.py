import tempfile
import unittest

import luigi.hive

from luigi import LocalTarget
import os


class HiveTest(unittest.TestCase):
    count = 0

    def mock_hive_cmd(self, args):
        self.last_hive_cmd = args
        self.count += 1
        return "statement{0}".format(self.count)

    def setUp(self):
        self.run_hive_cmd_saved = luigi.hive.run_hive
        luigi.hive.run_hive = self.mock_hive_cmd

    def tearDown(self):
        luigi.hive.run_hive = self.run_hive_cmd_saved

    def testRunHiveCommand(self):
        pre_count = self.count
        res = luigi.hive.run_hive_cmd("foo")
        self.assertEquals(["-e", "foo"], self.last_hive_cmd)
        self.assertEquals("statement{0}".format(pre_count+1), res)

    def testRunHiveScriptNotExists(self):
        def test():
            luigi.hive.run_hive_script("/tmp/some-non-existant-file______")
        self.assertRaises(RuntimeError, test)

    def testRunHiveScriptExists(self):
        with tempfile.NamedTemporaryFile(delete=True) as f:
            pre_count = self.count
            res = luigi.hive.run_hive_script(f.name)
            self.assertEquals(["-f", f.name], self.last_hive_cmd)
            self.assertEquals("statement{0}".format(pre_count+1), res)

    def test_create_parent_dirs(self):
        dirname = "/tmp/hive_task_test_dir"

        class FooHiveTask(object):
            def output(self):
                return LocalTarget(os.path.join(dirname, "foo"))

        runner = luigi.hive.HiveQueryRunner()
        runner.prepare_outputs(FooHiveTask())
        self.assertTrue(os.path.exists(dirname))
