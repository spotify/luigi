from unittest import TestCase
from luigi import hive


class TestHiveTask(TestCase):
    def test_error_cmd(self):
        self.assertRaises(hive.HiveCommandError, hive.run_hive_cmd, "this is a bogus command and should cause an error;")

    def test_ok_cmd(self):
        "Test that SHOW TABLES doesn't throw an error"
        hive.run_hive_cmd("SHOW TABLES;")
