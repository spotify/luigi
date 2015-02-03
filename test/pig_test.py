import subprocess
import StringIO
import unittest

from mock import patch

import luigi
from luigi.contrib.pig import PigJobTask, PigJobError
from helpers import with_config

import tempfile


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
        except PigJobError as p:
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
        try:
            job = ComplexTestJob()
            job.run()
            self.assertEqual([['/usr/share/pig/bin/pig', '-x', 'local', '-p', 'YOUR_PARAM_NAME=Your param value', '-propertyFile', 'pig_property_file', '-f', 'my_complex_pig_script.pig']], arglist_result)

            # Check property file
            with open('pig_property_file') as pprops_file:
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
        try:
            job = ComplexTestJob()
            job.run()
        except PigJobError as p:
            self.assertEqual('stderr', p.err)
            self.assertEqual([['/usr/share/pig/bin/pig', '-x', 'local', '-p', 'YOUR_PARAM_NAME=Your param value', '-propertyFile', 'pig_property_file', '-f', 'my_complex_pig_script.pig']], arglist_result)

            # Check property file
            with open('pig_property_file') as pprops_file:
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

            def wait(self):
                pass

            def poll(self):
                return 0

            def communicate(self):
                return 'end'

            def env(self):
                return self.env

        p = P()
        p.returncode = return_code

        p.stderr = tempfile.TemporaryFile()
        p.stdout = tempfile.TemporaryFile()

        p.stdout.write('stdout')
        p.stderr.write('stderr')

        # Reset temp files so the output can be read.
        p.stdout.seek(0)
        p.stderr.seek(0)

        return p

    return Popen_fake
