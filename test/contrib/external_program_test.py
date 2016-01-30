# -*- coding: utf-8 -*-
#
# Copyright 2012-2016 Spotify AB
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
import os
import shutil
import tempfile

from helpers import unittest
import luigi
import luigi.contrib.hdfs
from luigi import six
from luigi.contrib.external_program import ExternalProgramTask, ExternalPythonProgramTask
from luigi.contrib.external_program import ExternalProgramRunError
from mock import patch, call

BytesIO = six.BytesIO


def poll_generator():
    yield None
    yield 1


def setup_run_process(proc):
    poll_gen = poll_generator()
    proc.return_value.poll = lambda: next(poll_gen)
    proc.return_value.returncode = 0
    proc.return_value.stdout = BytesIO()
    proc.return_value.stderr = BytesIO()


class TestExternalProgramTask(ExternalProgramTask):
    def program_args(self):
        return ['app_path', 'arg1', 'arg2']

    def output(self):
        return luigi.LocalTarget('output')


class TestLogStderrOnFailureOnlyTask(TestExternalProgramTask):
    always_log_stderr = False


class TestTouchTask(ExternalProgramTask):
    file_path = luigi.Parameter()

    def program_args(self):
        return ['touch', self.output().path]

    def output(self):
        return luigi.LocalTarget(self.file_path)


class ExternalProgramTaskTest(unittest.TestCase):
    @patch('luigi.contrib.external_program.subprocess.Popen')
    def test_run(self, proc):
        setup_run_process(proc)
        job = TestExternalProgramTask()
        job.run()

        self.assertEqual(proc.call_args[0][0],
                         ['app_path', 'arg1', 'arg2'])

    @patch('luigi.contrib.external_program.logger')
    @patch('luigi.contrib.external_program.tempfile.TemporaryFile')
    @patch('luigi.contrib.external_program.subprocess.Popen')
    def test_handle_failed_job(self, proc, file, logger):
        proc.return_value.returncode = 1
        file.return_value = BytesIO(b'stderr')
        try:
            job = TestExternalProgramTask()
            job.run()
        except ExternalProgramRunError as e:
            self.assertEqual(e.err, 'stderr')
            self.assertIn('STDERR: stderr', six.text_type(e))
            self.assertIn(call.info('Program stderr:\nstderr'), logger.mock_calls)
        else:
            self.fail('Should have thrown ExternalProgramRunError')

    @patch('luigi.contrib.external_program.logger')
    @patch('luigi.contrib.external_program.tempfile.TemporaryFile')
    @patch('luigi.contrib.external_program.subprocess.Popen')
    def test_always_log_stderr_on_failure(self, proc, file, logger):
        proc.return_value.returncode = 1
        file.return_value = BytesIO(b'stderr')
        with self.assertRaises(ExternalProgramRunError):
            job = TestLogStderrOnFailureOnlyTask()
            job.run()

        self.assertIn(call.info('Program stderr:\nstderr'), logger.mock_calls)

    @patch('luigi.contrib.external_program.logger')
    @patch('luigi.contrib.external_program.tempfile.TemporaryFile')
    @patch('luigi.contrib.external_program.subprocess.Popen')
    def test_log_stderr_on_success_by_default(self, proc, file, logger):
        proc.return_value.returncode = 0
        file.return_value = BytesIO(b'stderr')
        job = TestExternalProgramTask()
        job.run()

        self.assertIn(call.info('Program stderr:\nstderr'), logger.mock_calls)

    @patch('luigi.contrib.external_program.logger')
    @patch('luigi.contrib.external_program.tempfile.TemporaryFile')
    @patch('luigi.contrib.external_program.subprocess.Popen')
    def test_dont_log_stderr_on_success_if_disabled(self, proc, file, logger):
        proc.return_value.returncode = 0
        file.return_value = BytesIO(b'stderr')
        job = TestLogStderrOnFailureOnlyTask()
        job.run()

        self.assertNotIn(call.info('Program stderr:\nstderr'), logger.mock_calls)

    @patch('luigi.contrib.external_program.subprocess.Popen')
    def test_program_args_must_be_implemented(self, proc):
        with self.assertRaises(NotImplementedError):
            job = ExternalProgramTask()
            job.run()

    @patch('luigi.contrib.external_program.subprocess.Popen')
    def test_app_interruption(self, proc):

        def interrupt():
            raise KeyboardInterrupt()

        proc.return_value.wait = interrupt
        try:
            job = TestExternalProgramTask()
            job.run()
        except KeyboardInterrupt:
            pass
        proc.return_value.kill.check_called()

    def test_non_mocked_task_run(self):
        # create a tempdir first, to ensure an empty playground for
        # TestTouchTask to create its file in
        tempdir = tempfile.mkdtemp()
        tempfile_path = os.path.join(tempdir, 'testfile')

        try:
            job = TestTouchTask(file_path=tempfile_path)
            job.run()

            self.assertTrue(luigi.LocalTarget(tempfile_path).exists())
        finally:
            # clean up temp files even if assertion fails
            shutil.rmtree(tempdir)


class TestExternalPythonProgramTask(ExternalPythonProgramTask):
    virtualenv = '/path/to/venv'
    extra_pythonpath = '/extra/pythonpath'

    def program_args(self):
        return ["app_path", "arg1", "arg2"]

    def output(self):
        return luigi.LocalTarget('output')


class ExternalPythonProgramTaskTest(unittest.TestCase):
    @patch.dict('os.environ', {'OTHERVAR': 'otherval'}, clear=True)
    @patch('luigi.contrib.external_program.subprocess.Popen')
    def test_original_environment_is_kept_intact(self, proc):
        setup_run_process(proc)

        job = TestExternalPythonProgramTask()
        job.run()

        proc_env = proc.call_args[1]['env']
        self.assertIn('PYTHONPATH', proc_env)
        self.assertIn('OTHERVAR', proc_env)

    @patch.dict('os.environ', {'PATH': '/base/path'}, clear=True)
    @patch('luigi.contrib.external_program.subprocess.Popen')
    def test_venv_is_set_and_prepended_to_path(self, proc):
        setup_run_process(proc)

        job = TestExternalPythonProgramTask()
        job.run()

        proc_env = proc.call_args[1]['env']
        self.assertIn('PATH', proc_env)
        self.assertTrue(proc_env['PATH'].startswith('/path/to/venv/bin'))
        self.assertTrue(proc_env['PATH'].endswith('/base/path'))
        self.assertIn('VIRTUAL_ENV', proc_env)
        self.assertEquals(proc_env['VIRTUAL_ENV'], '/path/to/venv')

    @patch.dict('os.environ', {}, clear=True)
    @patch('luigi.contrib.external_program.subprocess.Popen')
    def test_pythonpath_is_set_if_empty(self, proc):
        setup_run_process(proc)

        job = TestExternalPythonProgramTask()
        job.run()

        proc_env = proc.call_args[1]['env']
        self.assertIn('PYTHONPATH', proc_env)
        self.assertTrue(proc_env['PYTHONPATH'].startswith('/extra/pythonpath'))

    @patch.dict('os.environ', {'PYTHONPATH': '/base/pythonpath'}, clear=True)
    @patch('luigi.contrib.external_program.subprocess.Popen')
    def test_pythonpath_is_prepended_if_not_empty(self, proc):
        setup_run_process(proc)

        job = TestExternalPythonProgramTask()
        job.run()

        proc_env = proc.call_args[1]['env']
        self.assertIn('PYTHONPATH', proc_env)
        self.assertTrue(proc_env['PYTHONPATH'].startswith('/extra/pythonpath'))
        self.assertTrue(proc_env['PYTHONPATH'].endswith('/base/pythonpath'))
