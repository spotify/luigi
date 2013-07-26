import mock
import os
import sys
import tempfile
import unittest

import luigi.hive
from luigi import LocalTarget


class HiveTest(unittest.TestCase):
    count = 0

    def mock_hive_cmd(self, args, check_return=True):
        self.last_hive_cmd = args
        self.count += 1
        return "statement{0}".format(self.count)

    def setUp(self):
        self.run_hive_cmd_saved = luigi.hive.run_hive
        luigi.hive.run_hive = self.mock_hive_cmd

    def tearDown(self):
        luigi.hive.run_hive = self.run_hive_cmd_saved

    def test_run_hive_command(self):
        pre_count = self.count
        res = luigi.hive.run_hive_cmd("foo")
        self.assertEquals(["-e", "foo"], self.last_hive_cmd)
        self.assertEquals("statement{0}".format(pre_count+1), res)

    def test_run_hive_script_not_exists(self):
        def test():
            luigi.hive.run_hive_script("/tmp/some-non-existant-file______")
        self.assertRaises(RuntimeError, test)

    def test_run_hive_script_exists(self):
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

class HiveCommandClientTest(unittest.TestCase):
    """Note that some of these tests are really for the CDH releases of Hive, to which I do not currently have access.
    Hopefully there are no significant differences in the expected output"""

    def setUp(self):
        self.client = luigi.hive.HiveCommandClient()
        self.apacheclient = luigi.hive.ApacheHiveCommandClient()

    @mock.patch("luigi.hive.run_hive_cmd")
    def test_default_table_location(self, run_command):
        run_command.return_value = "Protect Mode:       	None                	 \n" \
                                   "Retention:          	0                   	 \n" \
                                   "Location:           	hdfs://localhost:9000/user/hive/warehouse/mytable	 \n" \
                                   "Table Type:         	MANAGED_TABLE       	 \n"

        returned = self.client.table_location("mytable")
        self.assertEquals('hdfs://localhost:9000/user/hive/warehouse/mytable', returned)

    @mock.patch("luigi.hive.run_hive_cmd")
    def test_table_exists(self, run_command):
        run_command.return_value = "FAILED: SemanticException [Error 10001]: blah does not exist\nSome other stuff"
        returned = self.client.table_exists("mytable")
        self.assertFalse(returned)

        run_command.return_value = "OK\n" \
                                   "col1       	string              	None                \n" \
                                   "col2            	string              	None                \n" \
                                   "col3         	string              	None  \n"
        returned = self.client.table_exists("mytable")
        self.assertTrue(returned)

        run_command.return_value = "day=2013-06-28/hour=3\n" \
                                   "day=2013-06-28/hour=4\n" \
                                   "day=2013-07-07/hour=2\n"
        self.client.partition_spec = mock.Mock(name="partition_spec")
        self.client.partition_spec.return_value = "somepart"
        returned = self.client.table_exists("mytable", partition={'a':'b'})
        self.assertTrue(returned)

        run_command.return_value = ""
        returned = self.client.table_exists("mytable", partition={'a':'b'})
        self.assertFalse(returned)

    @mock.patch("luigi.hive.run_hive_cmd")
    def test_table_schema(self, run_command):
        run_command.return_value = "FAILED: SemanticException [Error 10001]: blah does not exist\nSome other stuff"
        returned = self.client.table_schema("mytable")
        self.assertFalse(returned)

        run_command.return_value = "OK\n" \
                                   "col1       	string              	None                \n" \
                                   "col2            	string              	None                \n" \
                                   "col3         	string              	None                \n" \
                                   "day                 	string              	None                \n" \
                                   "hour                	smallint            	None                \n\n" \
                                   "# Partition Information	 	 \n" \
                                   "# col_name            	data_type           	comment             \n\n" \
                                   "day                 	string              	None                \n" \
                                   "hour                	smallint            	None                \n" \
                                   "Time taken: 2.08 seconds, Fetched: 34 row(s)\n"
        expected = [('OK',),
                    ('col1', 'string', 'None'),
                    ('col2', 'string', 'None'),
                    ('col3', 'string', 'None'),
                    ('day', 'string', 'None'),
                    ('hour', 'smallint', 'None'),
                    ('',),
                    ('# Partition Information',),
                    ('# col_name', 'data_type', 'comment'),
                    ('',),
                    ('day', 'string', 'None'),
                    ('hour', 'smallint', 'None'),
                    ('Time taken: 2.08 seconds, Fetched: 34 row(s)',)]
        returned = self.client.table_schema("mytable")
        self.assertEquals(expected, returned)

    def test_partition_spec(self):
        returned = self.client.partition_spec({'a': 'b', 'c': 'd'})
        self.assertEquals("a='b',c='d'", returned)

    @mock.patch("luigi.hive.run_hive_cmd")
    def test_apacheclient_table_exists(self, run_command):
        run_command.return_value = "FAILED: SemanticException [Error 10001]: Table not found mytable\nSome other stuff"
        returned = self.apacheclient.table_exists("mytable")
        self.assertFalse(returned)

        run_command.return_value = "OK\n" \
                                   "col1       	string              	None                \n" \
                                   "col2            	string              	None                \n" \
                                   "col3         	string              	None  \n"
        returned = self.apacheclient.table_exists("mytable")
        self.assertTrue(returned)

        run_command.return_value = "day=2013-06-28/hour=3\n" \
                                   "day=2013-06-28/hour=4\n" \
                                   "day=2013-07-07/hour=2\n"
        self.apacheclient.partition_spec = mock.Mock(name="partition_spec")
        self.apacheclient.partition_spec.return_value = "somepart"
        returned = self.apacheclient.table_exists("mytable", partition={'a':'b'})
        self.assertTrue(returned)

        run_command.return_value = ""
        returned = self.apacheclient.table_exists("mytable", partition={'a':'b'})
        self.assertFalse(returned)

    @mock.patch("luigi.hive.run_hive_cmd")
    def test_apacheclient_table_schema(self, run_command):
        run_command.return_value = "FAILED: SemanticException [Error 10001]: Table not found mytable\nSome other stuff"
        returned = self.apacheclient.table_schema("mytable")
        self.assertFalse(returned)

        run_command.return_value = "OK\n" \
                                   "col1       	string              	None                \n" \
                                   "col2            	string              	None                \n" \
                                   "col3         	string              	None                \n" \
                                   "day                 	string              	None                \n" \
                                   "hour                	smallint            	None                \n\n" \
                                   "# Partition Information	 	 \n" \
                                   "# col_name            	data_type           	comment             \n\n" \
                                   "day                 	string              	None                \n" \
                                   "hour                	smallint            	None                \n" \
                                   "Time taken: 2.08 seconds, Fetched: 34 row(s)\n"
        expected = [('OK',),
                    ('col1', 'string', 'None'),
                    ('col2', 'string', 'None'),
                    ('col3', 'string', 'None'),
                    ('day', 'string', 'None'),
                    ('hour', 'smallint', 'None'),
                    ('',),
                    ('# Partition Information',),
                    ('# col_name', 'data_type', 'comment'),
                    ('',),
                    ('day', 'string', 'None'),
                    ('hour', 'smallint', 'None'),
                    ('Time taken: 2.08 seconds, Fetched: 34 row(s)',)]
        returned = self.apacheclient.table_schema("mytable")
        self.assertEquals(expected, returned)

    @mock.patch("luigi.configuration")
    def test_client_def(self, hive_syntax):
        hive_syntax.get_config.return_value.get.return_value = "cdh4"

        del sys.modules['luigi.hive']
        import luigi.hive
        self.assertEquals(luigi.hive.HiveCommandClient, type(luigi.hive.client))

        hive_syntax.get_config.return_value.get.return_value = "cdh3"
        del sys.modules['luigi.hive']
        import luigi.hive
        self.assertEquals(luigi.hive.HiveCommandClient, type(luigi.hive.client))

        hive_syntax.get_config.return_value.get.return_value = "apache"
        del sys.modules['luigi.hive']
        import luigi.hive
        self.assertEquals(luigi.hive.ApacheHiveCommandClient, type(luigi.hive.client))

    @mock.patch('subprocess.Popen')
    def test_run_hive_command(self, popen):
        # I'm testing this again to check the return codes
        # I didn't want to tear up all the existing tests to change how run_hive is mocked
        comm = mock.Mock(name='communicate_mock')
        comm.return_value = "some return stuff", ""

        preturn = mock.Mock(name='open_mock')
        preturn.returncode = 0
        preturn.communicate = comm
        popen.return_value = preturn

        returned = luigi.hive.run_hive(["blah", "blah"])
        self.assertEquals("some return stuff", returned)

        preturn.returncode = 17
        self.assertRaises(luigi.hive.HiveCommandError, luigi.hive.run_hive, ["blah", "blah"])

        comm.return_value = "", "some stderr stuff"
        returned = luigi.hive.run_hive(["blah", "blah"], False)
        self.assertEquals("", returned)

if __name__ == '__main__':
    unittest.main()
