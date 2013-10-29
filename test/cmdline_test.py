# Copyright (c) 2012 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import ConfigParser
import logging
import luigi
from luigi.mock import MockFile
import mock
import unittest
import warnings


class SomeTask(luigi.Task):
    n = luigi.IntParameter()

    def output(self):
        return File('/tmp/test_%d' % self.n)

    def run(self):
        f = self.output().open('w')
        f.write('done')
        f.close()


class AmbiguousClass(luigi.Task):
    pass


class AmbiguousClass(luigi.Task):
    pass


class NonAmbiguousClass(luigi.ExternalTask):
    pass


class NonAmbiguousClass(luigi.Task):
    def run(self):
        NonAmbiguousClass.has_run = True


class CmdlineTest(unittest.TestCase):
    def setUp(self):
        global File
        File = MockFile
        MockFile._file_contents.clear()

    def test_expose_deprecated(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            luigi.expose(SomeTask)
            self.assertEqual(w[-1].category, DeprecationWarning)

    @mock.patch("logging.getLogger")
    def test_cmdline_main_task_cls(self, logger):
        luigi.run(['--local-scheduler', '--n', '100'], main_task_cls=SomeTask)
        self.assertEqual(MockFile._file_contents, {'/tmp/test_100': 'done'})

    @mock.patch("logging.getLogger")
    def test_cmdline_other_task(self, logger):
        luigi.run(['--local-scheduler', 'SomeTask', '--n', '1000'])
        self.assertEqual(MockFile._file_contents, {'/tmp/test_1000': 'done'})

    @mock.patch("logging.getLogger")
    def test_cmdline_ambiguous_class(self, logger):
        self.assertRaises(Exception, luigi.run, ['--local-scheduler', 'AmbiguousClass'])

    @mock.patch("logging.getLogger")
    @mock.patch("warnings.warn")
    def test_cmdline_non_ambiguous_class(self, warn, logger):
        luigi.run(['--local-scheduler', 'NonAmbiguousClass'])
        self.assertTrue(NonAmbiguousClass.has_run)

    @mock.patch("logging.getLogger")
    @mock.patch("logging.StreamHandler")
    def test_setup_interface_logging(self, handler, logger):
        handler.return_value = mock.Mock(name="stream_handler")
        with mock.patch("luigi.interface.setup_interface_logging.has_run", new=False):
            luigi.interface.setup_interface_logging()
            self.assertEqual([mock.call(handler.return_value)], logger.return_value.addHandler.call_args_list)

        with mock.patch("luigi.interface.setup_interface_logging.has_run", new=False):
            self.assertRaises(ConfigParser.NoSectionError, luigi.interface.setup_interface_logging, '/blah')

    @mock.patch("warnings.warn")
    @mock.patch("luigi.interface.setup_interface_logging")
    def test_cmdline_logger(self, setup_mock, warn):
        luigi.run(['Task', '--local-scheduler'])
        self.assertEqual([mock.call(None)], setup_mock.call_args_list)

        with mock.patch("luigi.configuration.get_config") as getconf:
            getconf.return_value.get.return_value = None
            getconf.return_value.get_boolean.return_value = True

            luigi.interface.setup_interface_logging.call_args_list = []
            luigi.run(['Task', '--local-scheduler'])
            self.assertEqual([], setup_mock.call_args_list)

if __name__ == '__main__':
    unittest.main()
