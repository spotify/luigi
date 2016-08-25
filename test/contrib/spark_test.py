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

import unittest
import os
import luigi
import luigi.contrib.hdfs
from luigi import six
from luigi.mock import MockTarget
from helpers import with_config, temporary_unloaded_module
from luigi.contrib.external_program import ExternalProgramRunError
from luigi.contrib.spark import SparkSubmitTask, PySparkTask
from mock import patch, call, MagicMock

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


class TestSparkSubmitTask(SparkSubmitTask):
    deploy_mode = "client"
    name = "AppName"
    entry_class = "org.test.MyClass"
    jars = ["jars/my.jar"]
    py_files = ["file1.py", "file2.py"]
    files = ["file1", "file2"]
    conf = {"Prop": "Value"}
    properties_file = "conf/spark-defaults.conf"
    driver_memory = "4G"
    driver_java_options = "-Xopt"
    driver_library_path = "library/path"
    driver_class_path = "class/path"
    executor_memory = "8G"
    driver_cores = 8
    supervise = True
    total_executor_cores = 150
    executor_cores = 10
    queue = "queue"
    num_executors = 2
    archives = ["archive1", "archive2"]
    app = "file"

    def app_options(self):
        return ["arg1", "arg2"]

    def output(self):
        return luigi.LocalTarget('output')


class TestDefaultSparkSubmitTask(SparkSubmitTask):
    app = 'test.py'

    def output(self):
        return luigi.LocalTarget('output')


class TestPySparkTask(PySparkTask):

    def input(self):
        return MockTarget('input')

    def output(self):
        return MockTarget('output')

    def main(self, sc, *args):
        sc.textFile(self.input().path).saveAsTextFile(self.output().path)


class SparkSubmitTaskTest(unittest.TestCase):
    ss = 'ss-stub'

    @with_config({'spark': {'spark-submit': ss, 'master': "yarn-client", 'hadoop-conf-dir': 'path'}})
    @patch('luigi.contrib.external_program.subprocess.Popen')
    def test_run(self, proc):
        setup_run_process(proc)
        job = TestSparkSubmitTask()
        job.run()

        self.assertEqual(proc.call_args[0][0],
                         ['ss-stub', '--master', 'yarn-client', '--deploy-mode', 'client', '--name', 'AppName',
                          '--class', 'org.test.MyClass', '--jars', 'jars/my.jar', '--py-files', 'file1.py,file2.py',
                          '--files', 'file1,file2', '--archives', 'archive1,archive2', '--conf', 'Prop=Value',
                          '--properties-file', 'conf/spark-defaults.conf', '--driver-memory', '4G', '--driver-java-options', '-Xopt',
                          '--driver-library-path', 'library/path', '--driver-class-path', 'class/path', '--executor-memory', '8G',
                          '--driver-cores', '8', '--supervise', '--total-executor-cores', '150', '--executor-cores', '10',
                          '--queue', 'queue', '--num-executors', '2', 'file', 'arg1', 'arg2'])

    @with_config({'spark': {'hadoop-conf-dir': 'path'}})
    @patch('luigi.contrib.external_program.subprocess.Popen')
    def test_environment_is_set_correctly(self, proc):
        setup_run_process(proc)
        job = TestSparkSubmitTask()
        job.run()

        self.assertIn('HADOOP_CONF_DIR', proc.call_args[1]['env'])
        self.assertEqual(proc.call_args[1]['env']['HADOOP_CONF_DIR'], 'path')

    @with_config({'spark': {'spark-submit': ss, 'master': 'spark://host:7077', 'conf': 'prop1=val1', 'jars': 'jar1.jar,jar2.jar',
                            'files': 'file1,file2', 'py-files': 'file1.py,file2.py', 'archives': 'archive1'}})
    @patch('luigi.contrib.external_program.subprocess.Popen')
    def test_defaults(self, proc):
        proc.return_value.returncode = 0
        job = TestDefaultSparkSubmitTask()
        job.run()
        self.assertEqual(proc.call_args[0][0],
                         ['ss-stub', '--master', 'spark://host:7077', '--jars', 'jar1.jar,jar2.jar',
                          '--py-files', 'file1.py,file2.py', '--files', 'file1,file2', '--archives', 'archive1',
                          '--conf', 'prop1=val1', 'test.py'])

    @patch('luigi.contrib.external_program.logger')
    @patch('luigi.contrib.external_program.tempfile.TemporaryFile')
    @patch('luigi.contrib.external_program.subprocess.Popen')
    def test_handle_failed_job(self, proc, file, logger):
        proc.return_value.returncode = 1
        file.return_value = BytesIO(b'spark test error')
        try:
            job = TestSparkSubmitTask()
            job.run()
        except ExternalProgramRunError as e:
            self.assertEqual(e.err, 'spark test error')
            self.assertIn('spark test error', six.text_type(e))
            self.assertIn(call.info('Program stderr:\nspark test error'),
                          logger.mock_calls)
        else:
            self.fail("Should have thrown ExternalProgramRunError")

    @patch('luigi.contrib.external_program.logger')
    @patch('luigi.contrib.external_program.tempfile.TemporaryFile')
    @patch('luigi.contrib.external_program.subprocess.Popen')
    def test_dont_log_stderr_on_success(self, proc, file, logger):
        proc.return_value.returncode = 0
        file.return_value = BytesIO(b'spark normal error output')
        job = TestSparkSubmitTask()
        job.run()

        self.assertNotIn(call.info(
            'Program stderr:\nspark normal error output'),
            logger.mock_calls)

    @patch('luigi.contrib.external_program.subprocess.Popen')
    def test_app_must_be_set(self, proc):
        with self.assertRaises(NotImplementedError):
            job = SparkSubmitTask()
            job.run()

    @patch('luigi.contrib.external_program.subprocess.Popen')
    def test_app_interruption(self, proc):

        def interrupt():
            raise KeyboardInterrupt()

        proc.return_value.wait = interrupt
        try:
            job = TestSparkSubmitTask()
            job.run()
        except KeyboardInterrupt:
            pass
        proc.return_value.kill.check_called()


class PySparkTaskTest(unittest.TestCase):
    ss = 'ss-stub'

    @with_config({'spark': {'spark-submit': ss, 'master': "spark://host:7077"}})
    @patch('luigi.contrib.external_program.subprocess.Popen')
    def test_run(self, proc):
        setup_run_process(proc)
        job = TestPySparkTask()
        job.run()
        proc_arg_list = proc.call_args[0][0]
        self.assertEqual(proc_arg_list[0:7], ['ss-stub', '--master', 'spark://host:7077', '--deploy-mode', 'client', '--name', 'TestPySparkTask'])
        self.assertTrue(os.path.exists(proc_arg_list[7]))
        self.assertTrue(proc_arg_list[8].endswith('TestPySparkTask.pickle'))

    @with_config({'spark': {'spark-submit': ss, 'master': "spark://host:7077"}})
    @patch('luigi.contrib.external_program.subprocess.Popen')
    def test_run_with_pickle_dump(self, proc):
        setup_run_process(proc)
        job = TestPySparkTask()
        luigi.build([job], local_scheduler=True)
        self.assertEqual(proc.call_count, 1)
        proc_arg_list = proc.call_args[0][0]
        self.assertEqual(proc_arg_list[0:7], ['ss-stub', '--master', 'spark://host:7077', '--deploy-mode', 'client', '--name', 'TestPySparkTask'])
        self.assertTrue(os.path.exists(proc_arg_list[7]))
        self.assertTrue(proc_arg_list[8].endswith('TestPySparkTask.pickle'))

    @patch.dict('sys.modules', {'pyspark': MagicMock()})
    @patch('pyspark.SparkContext')
    def test_pyspark_runner(self, spark_context):
        sc = spark_context.return_value.__enter__.return_value

        def mock_spark_submit(task):
            from luigi.contrib.pyspark_runner import PySparkRunner
            PySparkRunner(*task.app_command()[1:]).run()
            # Check py-package exists
            self.assertTrue(os.path.exists(sc.addPyFile.call_args[0][0]))

        with patch.object(SparkSubmitTask, 'run', mock_spark_submit):
            job = TestPySparkTask()
            with temporary_unloaded_module(b'') as task_module:
                with_config({'spark': {'py-packages': task_module}})(job.run)()

        sc.textFile.assert_called_with('input')
        sc.textFile.return_value.saveAsTextFile.assert_called_with('output')
