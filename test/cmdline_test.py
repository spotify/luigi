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
from helpers import unittest, parsing

from luigi import six

import luigi
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
            env_params.return_value.logging_conf_file = None
            luigi.run(['SomeTask', '--n', '7', '--local-scheduler', '--no-lock'])
            self.assertEqual([mock.call(None)], setup_mock.call_args_list)

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


class InvokeOverCmdlineTest(unittest.TestCase):

    def _run_cmdline(self, args):
        env = os.environ.copy()
        env['PYTHONPATH'] = env.get('PYTHONPATH', '') + ':.:test'
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
        returncode, stdout, stderr = self._run_cmdline(['python', 'test/cmdline_test.py', '--help'])
        self.assertTrue(stdout.find(b'--FooBaseClass-x') != -1)
        self.assertFalse(stdout.find(b'--x') != -1)

    def test_direct_python_help_class(self):
        returncode, stdout, stderr = self._run_cmdline(['python', 'test/cmdline_test.py', 'FooBaseClass', '--help'])
        self.assertTrue(stdout.find(b'--FooBaseClass-x') != -1)
        self.assertTrue(stdout.find(b'--x') != -1)

    def test_bin_luigi_help(self):
        returncode, stdout, stderr = self._run_cmdline(['./bin/luigi', '--module', 'cmdline_test', '--help'])
        self.assertTrue(stdout.find(b'--FooBaseClass-x') != -1)
        self.assertFalse(stdout.find(b'--x') != -1)

    def test_python_module_luigi_help(self):
        returncode, stdout, stderr = self._run_cmdline(['python', '-m', 'luigi', '--module', 'cmdline_test', '--help'])
        self.assertTrue(stdout.find(b'--FooBaseClass-x') != -1)
        self.assertFalse(stdout.find(b'--x') != -1)

    def test_bin_luigi_help_no_module(self):
        returncode, stdout, stderr = self._run_cmdline(['./bin/luigi', '--help'])
        self.assertTrue(stdout.find(b'usage:') != -1)

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


class NewStyleParameters822Test(unittest.TestCase):
    # See https://github.com/spotify/luigi/issues/822

    @parsing(['FooSubClass', '--x', 'xyz', '--FooBaseClass-x', 'xyz'])
    def test_subclasses(self):
        self.assertEquals(FooSubClass().x, 'xyz')

    @parsing(['FooBaseClass', '--FooBaseClass-x', 'xyz'])
    def test_subclasses_2(self):
        # https://github.com/spotify/luigi/issues/822#issuecomment-77782714
        self.assertEquals(FooBaseClass().x, 'xyz')


if __name__ == '__main__':
    # Needed for one of the tests
    luigi.run()
