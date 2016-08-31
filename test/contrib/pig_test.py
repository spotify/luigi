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

import subprocess
import tempfile

import luigi
from helpers import unittest
from luigi.contrib.pig import PigJobError, PigJobTask
from mock import patch


class SimpleTestJob(PigJobTask):
    def output(self):
        return luigi.LocalTarget('simple-output')

    def pig_script_path(self):
        return "my_simple_pig_script.pig"


class ComplexTestJob(PigJobTask):
    def output(self):
        return luigi.LocalTarget('complex-output')

    def pig_script_path(self):
        return "my_complex_pig_script.pig"

    def pig_env_vars(self):
        return {'PIG_CLASSPATH': '/your/path'}

    def pig_properties(self):
        return {'pig.additional.jars': '/path/to/your/jar'}

    def pig_parameters(self):
        return {'YOUR_PARAM_NAME': 'Your param value'}

    def pig_options(self):
        return ['-x', 'local']


class SimplePigTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    @patch('subprocess.Popen')
    def test_run__success(self, mock):
        arglist_result = []
        p = subprocess.Popen
        subprocess.Popen = _get_fake_Popen(arglist_result, 0)
        try:
            job = SimpleTestJob()
            job.run()
            self.assertEqual([['/usr/share/pig/bin/pig', '-f', 'my_simple_pig_script.pig']], arglist_result)
        finally:
            subprocess.Popen = p

    @patch('subprocess.Popen')
    def test_run__fail(self, mock):
        arglist_result = []
        p = subprocess.Popen
        subprocess.Popen = _get_fake_Popen(arglist_result, 1)
        try:
            job = SimpleTestJob()
            job.run()
            self.assertEqual([['/usr/share/pig/bin/pig', '-f', 'my_simple_pig_script.pig']], arglist_result)
        except PigJobError as e:
            p = e
            self.assertEqual('stderr', p.err)
        else:
            self.fail("Should have thrown PigJobError")
        finally:
            subprocess.Popen = p


class ComplexPigTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    @patch('subprocess.Popen')
    def test_run__success(self, mock):
        arglist_result = []
        p = subprocess.Popen
        subprocess.Popen = _get_fake_Popen(arglist_result, 0)

        with tempfile.NamedTemporaryFile(delete=False) as param_file_mock, \
                tempfile.NamedTemporaryFile(delete=False) as prop_file_mock, \
                patch('luigi.contrib.pig.tempfile.NamedTemporaryFile',
                      side_effect=[param_file_mock, prop_file_mock]):
            try:
                job = ComplexTestJob()
                job.run()
                self.assertEqual([['/usr/share/pig/bin/pig', '-x', 'local',
                                   '-param_file', param_file_mock.name,
                                   '-propertyFile', prop_file_mock.name,
                                   '-f', 'my_complex_pig_script.pig']],
                                 arglist_result)

                # Check param file
                with open(param_file_mock.name) as pparams_file:
                    pparams = pparams_file.readlines()
                    self.assertEqual(1, len(pparams))
                    self.assertEqual('YOUR_PARAM_NAME=Your param value\n', pparams[0])

                # Check property file
                with open(prop_file_mock.name) as pprops_file:
                    pprops = pprops_file.readlines()
                    self.assertEqual(1, len(pprops))
                    self.assertEqual('pig.additional.jars=/path/to/your/jar\n', pprops[0])
            finally:
                subprocess.Popen = p

    @patch('subprocess.Popen')
    def test_run__fail(self, mock):
        arglist_result = []
        p = subprocess.Popen
        subprocess.Popen = _get_fake_Popen(arglist_result, 1)

        with tempfile.NamedTemporaryFile(delete=False) as param_file_mock, \
                tempfile.NamedTemporaryFile(delete=False) as prop_file_mock, \
                patch('luigi.contrib.pig.tempfile.NamedTemporaryFile',
                      side_effect=[param_file_mock, prop_file_mock]):
            try:
                job = ComplexTestJob()
                job.run()
            except PigJobError as e:
                p = e
                self.assertEqual('stderr', p.err)
                self.assertEqual([['/usr/share/pig/bin/pig', '-x', 'local',
                                   '-param_file', param_file_mock.name,
                                   '-propertyFile', prop_file_mock.name, '-f',
                                   'my_complex_pig_script.pig']],
                                 arglist_result)

                # Check param file
                with open(param_file_mock.name) as pparams_file:
                    pparams = pparams_file.readlines()
                    self.assertEqual(1, len(pparams))
                    self.assertEqual('YOUR_PARAM_NAME=Your param value\n', pparams[0])

                # Check property file
                with open(prop_file_mock.name) as pprops_file:
                    pprops = pprops_file.readlines()
                    self.assertEqual(1, len(pprops))
                    self.assertEqual('pig.additional.jars=/path/to/your/jar\n', pprops[0])
            else:
                self.fail("Should have thrown PigJobError")
            finally:
                subprocess.Popen = p


def _get_fake_Popen(arglist_result, return_code, *args, **kwargs):
    def Popen_fake(arglist, shell=None, stdout=None, stderr=None, env=None, close_fds=True):
        arglist_result.append(arglist)

        class P(object):
            number_of_process_polls = 5

            def __init__(self):
                self._process_polls_left = self.number_of_process_polls

            def wait(self):
                pass

            def poll(self):
                if self._process_polls_left:
                    self._process_polls_left -= 1
                    return None

                return 0

            def communicate(self):
                return 'end'

            def env(self):
                return self.env

        p = P()
        p.returncode = return_code

        p.stderr = tempfile.TemporaryFile()
        p.stdout = tempfile.TemporaryFile()

        p.stdout.write(b'stdout')
        p.stderr.write(b'stderr')

        # Reset temp files so the output can be read.
        p.stdout.seek(0)
        p.stderr.seek(0)

        return p

    return Popen_fake
