# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from __future__ import print_function

try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser
import mock
import os
import subprocess
from helpers import unittest

from luigi import six

import luigi
import luigi.cmdline
from luigi.mock import MockTarget


class SomeTask(luigi.Task):
    n = luigi.IntParameter()

    def output(self):
        return MockTarget('/tmp/test_%d' % self.n)

    def run(self):
        f = self.output().open('w')
        f.write('done')
        f.close()


class AmbiguousClass(luigi.Task):
    pass


class AmbiguousClass(luigi.Task):  # NOQA
    pass


class TaskWithSameName(luigi.Task):

    def run(self):
        self.x = 42


class TaskWithSameName(luigi.Task):  # NOQA
    # there should be no ambiguity

    def run(self):
        self.x = 43


class WriteToFile(luigi.Task):
    filename = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.filename)

    def run(self):
        f = self.output().open('w')
        print('foo', file=f)
        f.close()


class FooBaseClass(luigi.Task):
    x = luigi.Parameter(default='foo_base_default')


class FooSubClass(FooBaseClass):
    pass


class ATaskThatFails(luigi.Task):
    def run(self):
        raise ValueError()


class CmdlineTest(unittest.TestCase):

    def setUp(self):
        MockTarget.fs.clear()

    @mock.patch("logging.getLogger")
    def test_cmdline_main_task_cls(self, logger):
        luigi.run(['--local-scheduler', '--no-lock', '--n', '100'], main_task_cls=SomeTask)
        self.assertEqual(dict(MockTarget.fs.get_all_data()), {'/tmp/test_100': b'done'})

    @mock.patch("logging.getLogger")
    def test_cmdline_local_scheduler(self, logger):
        luigi.run(['SomeTask', '--no-lock', '--n', '101'], local_scheduler=True)
        self.assertEqual(dict(MockTarget.fs.get_all_data()), {'/tmp/test_101': b'done'})

    @mock.patch("logging.getLogger")
    def test_cmdline_other_task(self, logger):
        luigi.run(['--local-scheduler', '--no-lock', 'SomeTask', '--n', '1000'])
        self.assertEqual(dict(MockTarget.fs.get_all_data()), {'/tmp/test_1000': b'done'})

    @mock.patch("logging.getLogger")
    def test_cmdline_ambiguous_class(self, logger):
        self.assertRaises(Exception, luigi.run, ['--local-scheduler', '--no-lock', 'AmbiguousClass'])

    @mock.patch("logging.getLogger")
    @mock.patch("logging.StreamHandler")
    def test_setup_interface_logging(self, handler, logger):
        handler.return_value = mock.Mock(name="stream_handler")
        with mock.patch("luigi.interface.setup_interface_logging.has_run", new=False):
            luigi.interface.setup_interface_logging()
            self.assertEqual([mock.call(handler.return_value)], logger.return_value.addHandler.call_args_list)

        with mock.patch("luigi.interface.setup_interface_logging.has_run", new=False):
            if six.PY2:
                error = ConfigParser.NoSectionError
            else:
                error = KeyError
            self.assertRaises(error, luigi.interface.setup_interface_logging, '/blah')

    @mock.patch("warnings.warn")
    @mock.patch("luigi.interface.setup_interface_logging")
    def test_cmdline_logger(self, setup_mock, warn):
        with mock.patch("luigi.interface.core") as env_params:
            env_params.return_value.logging_conf_file = ''
            env_params.return_value.log_level = 'DEBUG'
            luigi.run(['SomeTask', '--n', '7', '--local-scheduler', '--no-lock'])
            self.assertEqual([mock.call('', 'DEBUG')], setup_mock.call_args_list)

        with mock.patch("luigi.configuration.get_config") as getconf:
            getconf.return_value.get.side_effect = ConfigParser.NoOptionError(section='foo', option='bar')
            getconf.return_value.getint.return_value = 0

            luigi.interface.setup_interface_logging.call_args_list = []
            luigi.run(['SomeTask', '--n', '42', '--local-scheduler', '--no-lock'])
            self.assertEqual([], setup_mock.call_args_list)

    @mock.patch('argparse.ArgumentParser.print_usage')
    def test_non_existent_class(self, print_usage):
        self.assertRaises(luigi.task_register.TaskClassNotFoundException,
                          luigi.run, ['--local-scheduler', '--no-lock', 'XYZ'])

    @mock.patch('argparse.ArgumentParser.print_usage')
    def test_no_task(self, print_usage):
        self.assertRaises(SystemExit, luigi.run, ['--local-scheduler', '--no-lock'])

    def test_luigid_logging_conf(self):
        with mock.patch('luigi.server.run') as server_run, \
                mock.patch('logging.config.fileConfig') as fileConfig:
            luigi.cmdline.luigid([])
            self.assertTrue(server_run.called)
            # the default test configuration specifies a logging conf file
            fileConfig.assert_called_with("test/testconfig/logging.cfg")

    def test_luigid_no_configure_logging(self):
        with mock.patch('luigi.server.run') as server_run, \
                mock.patch('logging.basicConfig') as basicConfig, \
                mock.patch('luigi.configuration.get_config') as get_config:
            get_config.return_value.getboolean.return_value = True  # no_configure_logging=True
            luigi.cmdline.luigid([])
            self.assertTrue(server_run.called)
            self.assertTrue(basicConfig.called)

    def test_luigid_no_logging_conf(self):
        with mock.patch('luigi.server.run') as server_run, \
                mock.patch('logging.basicConfig') as basicConfig, \
                mock.patch('luigi.configuration.get_config') as get_config:
            get_config.return_value.getboolean.return_value = False  # no_configure_logging=False
            get_config.return_value.get.return_value = None  # logging_conf_file=None
            luigi.cmdline.luigid([])
            self.assertTrue(server_run.called)
            self.assertTrue(basicConfig.called)

    def test_luigid_missing_logging_conf(self):
        with mock.patch('luigi.server.run') as server_run, \
                mock.patch('logging.basicConfig') as basicConfig, \
                mock.patch('luigi.configuration.get_config') as get_config:
            get_config.return_value.getboolean.return_value = False  # no_configure_logging=False
            get_config.return_value.get.return_value = "nonexistent.cfg"  # logging_conf_file=None
            self.assertRaises(Exception, luigi.cmdline.luigid, [])
            self.assertFalse(server_run.called)
            self.assertFalse(basicConfig.called)


class InvokeOverCmdlineTest(unittest.TestCase):

    def _run_cmdline(self, args):
        env = os.environ.copy()
        env['PYTHONPATH'] = env.get('PYTHONPATH', '') + ':.:test'
        print('Running: ' + ' '.join(args))  # To simplify rerunning failing tests
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        stdout, stderr = p.communicate()  # Unfortunately subprocess.check_output is 2.7+
        return p.returncode, stdout, stderr

    def test_bin_luigi(self):
        t = luigi.LocalTarget(is_tmp=True)
        args = ['./bin/luigi', '--module', 'cmdline_test', 'WriteToFile', '--filename', t.path, '--local-scheduler', '--no-lock']
        self._run_cmdline(args)
        self.assertTrue(t.exists())

    def test_direct_python(self):
        t = luigi.LocalTarget(is_tmp=True)
        args = ['python', 'test/cmdline_test.py', 'WriteToFile', '--filename', t.path, '--local-scheduler', '--no-lock']
        self._run_cmdline(args)
        self.assertTrue(t.exists())

    def test_python_module(self):
        t = luigi.LocalTarget(is_tmp=True)
        args = ['python', '-m', 'luigi', '--module', 'cmdline_test', 'WriteToFile', '--filename', t.path, '--local-scheduler', '--no-lock']
        self._run_cmdline(args)
        self.assertTrue(t.exists())

    def test_direct_python_help(self):
        returncode, stdout, stderr = self._run_cmdline(['python', 'test/cmdline_test.py', '--help-all'])
        self.assertTrue(stdout.find(b'--FooBaseClass-x') != -1)
        self.assertFalse(stdout.find(b'--x') != -1)

    def test_direct_python_help_class(self):
        returncode, stdout, stderr = self._run_cmdline(['python', 'test/cmdline_test.py', 'FooBaseClass', '--help'])
        self.assertTrue(stdout.find(b'--FooBaseClass-x') != -1)
        self.assertTrue(stdout.find(b'--x') != -1)

    def test_bin_luigi_help(self):
        returncode, stdout, stderr = self._run_cmdline(['./bin/luigi', '--module', 'cmdline_test', '--help-all'])
        self.assertTrue(stdout.find(b'--FooBaseClass-x') != -1)
        self.assertFalse(stdout.find(b'--x') != -1)

    def test_python_module_luigi_help(self):
        returncode, stdout, stderr = self._run_cmdline(['python', '-m', 'luigi', '--module', 'cmdline_test', '--help-all'])
        self.assertTrue(stdout.find(b'--FooBaseClass-x') != -1)
        self.assertFalse(stdout.find(b'--x') != -1)

    def test_bin_luigi_help_no_module(self):
        returncode, stdout, stderr = self._run_cmdline(['./bin/luigi', '--help'])
        self.assertTrue(stdout.find(b'usage:') != -1)

    def test_bin_luigi_help_not_spammy(self):
        """
        Test that `luigi --help` fits on one screen
        """
        returncode, stdout, stderr = self._run_cmdline(['./bin/luigi', '--help'])
        self.assertLessEqual(len(stdout.splitlines()), 15)

    def test_bin_luigi_all_help_spammy(self):
        """
        Test that `luigi --help-all` doesn't fit on a screen

        Naturally, I don't mind this test breaking, but it convinces me that
        the "not spammy" test is actually testing what it claims too.
        """
        returncode, stdout, stderr = self._run_cmdline(['./bin/luigi', '--help-all'])
        self.assertGreater(len(stdout.splitlines()), 15)

    def test_error_mesage_on_misspelled_task(self):
        returncode, stdout, stderr = self._run_cmdline(['./bin/luigi', 'RangeDaili'])
        self.assertTrue(stderr.find(b'RangeDaily') != -1)

    def test_bin_luigi_no_parameters(self):
        returncode, stdout, stderr = self._run_cmdline(['./bin/luigi'])
        self.assertTrue(stderr.find(b'No task specified') != -1)

    def test_python_module_luigi_no_parameters(self):
        returncode, stdout, stderr = self._run_cmdline(['python', '-m', 'luigi'])
        self.assertTrue(stderr.find(b'No task specified') != -1)

    def test_bin_luigi_help_class(self):
        returncode, stdout, stderr = self._run_cmdline(['./bin/luigi', '--module', 'cmdline_test', 'FooBaseClass', '--help'])
        self.assertTrue(stdout.find(b'--FooBaseClass-x') != -1)
        self.assertTrue(stdout.find(b'--x') != -1)

    def test_python_module_help_class(self):
        returncode, stdout, stderr = self._run_cmdline(['python', '-m', 'luigi', '--module', 'cmdline_test', 'FooBaseClass', '--help'])
        self.assertTrue(stdout.find(b'--FooBaseClass-x') != -1)
        self.assertTrue(stdout.find(b'--x') != -1)

    def test_bin_luigi_options_before_task(self):
        args = ['./bin/luigi', '--module', 'cmdline_test', '--no-lock', '--local-scheduler', '--FooBaseClass-x', 'hello', 'FooBaseClass']
        returncode, stdout, stderr = self._run_cmdline(args)
        self.assertEqual(0, returncode)

    def test_bin_fail_on_unrecognized_args(self):
        returncode, stdout, stderr = self._run_cmdline(['./bin/luigi', '--no-lock', '--local-scheduler', 'Task', '--unknown-param', 'hiiii'])
        self.assertNotEqual(0, returncode)

    def test_deps_py_script(self):
        """
        Test the deps.py script.
        """
        args = 'python luigi/tools/deps.py --module examples.top_artists ArtistToplistToDatabase --date-interval 2015-W10'.split()
        returncode, stdout, stderr = self._run_cmdline(args)
        self.assertEqual(0, returncode)
        self.assertTrue(stdout.find(b'[FileSystem] data/streams_2015_03_04_faked.tsv') != -1)
        self.assertTrue(stdout.find(b'[DB] localhost') != -1)

    def test_deps_tree_py_script(self):
        """
        Test the deps_tree.py script.
        """
        args = 'python luigi/tools/deps_tree.py --module examples.top_artists AggregateArtists --date-interval 2012-06'.split()
        returncode, stdout, stderr = self._run_cmdline(args)
        self.assertEqual(0, returncode)
        for i in range(1, 30):
            self.assertTrue(stdout.find(("-[Streams-{{'date': '2012-06-{0}'}}".format(str(i).zfill(2))).encode('utf-8')) != -1)

    def test_bin_mentions_misspelled_task(self):
        """
        Test that the error message is informative when a task is misspelled.

        In particular it should say that the task is misspelled and not that
        the local parameters do not exist.
        """
        returncode, stdout, stderr = self._run_cmdline(['./bin/luigi', '--module', 'cmdline_test', 'HooBaseClass', '--x 5'])
        self.assertTrue(stderr.find(b'FooBaseClass') != -1)
        self.assertTrue(stderr.find(b'--x') != 0)

    def test_stack_trace_has_no_inner(self):
        """
        Test that the stack trace for failing tasks are short

        The stack trace shouldn't contain unreasonably much implementation
        details of luigi In particular it should say that the task is
        misspelled and not that the local parameters do not exist.
        """
        returncode, stdout, stderr = self._run_cmdline(['./bin/luigi', '--module', 'cmdline_test', 'ATaskThatFails', '--local-scheduler', '--no-lock'])
        print(stdout)

        self.assertFalse(stdout.find(b"run() got an unexpected keyword argument 'tracking_url_callback'") != -1)
        self.assertFalse(stdout.find(b'During handling of the above exception, another exception occurred') != -1)


if __name__ == '__main__':
    # Needed for one of the tests
    luigi.run()
