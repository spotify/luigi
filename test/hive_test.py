import tempfile
import unittest

import luigi.hive


class HiveTest(unittest.TestCase):

    def mock_hive_cmd(self, args):
        self.last_hive_cmd = args

    def setUp(self):
        self.run_hive_cmd_saved = luigi.hive.run_hive
        luigi.hive.run_hive = self.mock_hive_cmd

    def tearDown(self):
        luigi.hive.run_hive = self.run_hive_cmd_saved

    def testRunHiveCommand(self):
        luigi.hive.run_hive_cmd("foo")
        self.assertEquals(["-e", "foo"], self.last_hive_cmd)

    def testRunHiveScriptNotExists(self):
        def test():
            luigi.hive.run_hive_script("/tmp/some-non-existant-file______")
        self.assertRaises(RuntimeError, test)

    def testRunHiveScriptExists(self):
        with tempfile.NamedTemporaryFile(delete=True) as f:
            luigi.hive.run_hive_script(f.name)
            self.assertEquals(["-f", f.name], self.last_hive_cmd)
