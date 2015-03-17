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

from helpers import unittest
import os
import luigi
import luigi.hdfs
from luigi import six
from luigi.mock import MockTarget
from helpers import with_config
from luigi.contrib.spark import SparkJobError, SparkSubmitTask, PySparkTask, PySpark1xJob, Spark1xJob, SparkJob
from mock import patch, MagicMock

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


class HdfsJob(luigi.ExternalTask):

    def output(self):
        return luigi.hdfs.HdfsTarget('test')


class TestSparkJob(SparkJob):

    spark_workers = '2'
    spark_master_memory = '1g'
    spark_worker_memory = '1g'

    def requires_hadoop(self):
        return HdfsJob()

    def jar(self):
        return 'jar'

    def job_class(self):
        return 'job_class'

    def output(self):
        return luigi.LocalTarget('output')


class TestSpark1xJob(Spark1xJob):

    def jar(self):
        return 'jar'

    def job_class(self):
        return 'job_class'

    def output(self):
        return luigi.LocalTarget('output')


class TestPySpark1xJob(PySpark1xJob):

    def program(self):
        return 'python_file'

    def output(self):
        return luigi.LocalTarget('output')


class SparkSubmitTaskTest(unittest.TestCase):
    ss = 'ss-stub'

    @with_config({'spark': {'spark-submit': ss, 'master': "yarn-client", 'hadoop-conf-dir': 'path'}})
    @patch('luigi.contrib.spark.subprocess.Popen')
    def test_run(self, proc):
        setup_run_process(proc)
        job = TestSparkSubmitTask()
        job.run()

        self.assertEqual(proc.call_args[0][0],
                         ['ss-stub', '--master', 'yarn-client', '--deploy-mode', 'client', '--name', 'AppName',
                          '--class', 'org.test.MyClass', '--jars', 'jars/my.jar', '--py-files', 'file1.py,file2.py',
                          '--files', 'file1,file2', '--archives', 'archive1,archive2', '--conf', '"Prop=Value"',
                          '--properties-file', 'conf/spark-defaults.conf', '--driver-memory', '4G', '--driver-java-options', '-Xopt',
                          '--driver-library-path', 'library/path', '--driver-class-path', 'class/path', '--executor-memory', '8G',
                          '--driver-cores', '8', '--supervise', '--total-executor-cores', '150', '--executor-cores', '10',
                          '--queue', 'queue', '--num-executors', '2', 'file', 'arg1', 'arg2'])

    @with_config({'spark': {'spark-submit': ss, 'master': 'spark://host:7077', 'conf': 'prop1=val1', 'jars': 'jar1.jar,jar2.jar',
                            'files': 'file1,file2', 'py-files': 'file1.py,file2.py', 'archives': 'archive1'}})
    @patch('luigi.contrib.spark.subprocess.Popen')
    def test_defaults(self, proc):
        proc.return_value.returncode = 0
        job = TestDefaultSparkSubmitTask()
        job.run()
        self.assertEqual(proc.call_args[0][0],
                         ['ss-stub', '--master', 'spark://host:7077', '--jars', 'jar1.jar,jar2.jar',
                          '--py-files', 'file1.py,file2.py', '--files', 'file1,file2', '--archives', 'archive1',
                          '--conf', '"prop1=val1"', 'test.py'])

    @patch('luigi.contrib.spark.tempfile.TemporaryFile')
    @patch('luigi.contrib.spark.subprocess.Popen')
    def test_handle_failed_job(self, proc, file):
        proc.return_value.returncode = 1
        file.return_value = BytesIO(b'stderr')
        try:
            job = TestSparkSubmitTask()
            job.run()
        except SparkJobError as e:
            self.assertEqual(e.err, 'stderr')
            self.assertTrue('STDERR: stderr' in six.text_type(e))
        else:
            self.fail("Should have thrown SparkJobError")

    @patch('luigi.contrib.spark.subprocess.Popen')
    def test_app_must_be_set(self, proc):
        with self.assertRaises(NotImplementedError):
            job = SparkSubmitTask()
            job.run()

    @patch('luigi.contrib.spark.subprocess.Popen')
    def test_app_interruption(self, proc):

        def interrupt():
            raise KeyboardInterrupt()

        proc.return_value.poll = interrupt
        try:
            job = TestSparkSubmitTask()
            job.run()
        except KeyboardInterrupt:
            pass
        proc.return_value.kill.assert_called()


class PySparkTaskTest(unittest.TestCase):
    ss = 'ss-stub'

    @with_config({'spark': {'spark-submit': ss, 'master': "spark://host:7077"}})
    @patch('luigi.contrib.spark.subprocess.Popen')
    def test_run(self, proc):
        setup_run_process(proc)
        job = TestPySparkTask()
        job.run()
        proc_arg_list = proc.call_args[0][0]
        self.assertEqual(proc_arg_list[0:7], ['ss-stub', '--master', 'spark://host:7077', '--deploy-mode', 'client', '--name', 'TestPySparkTask'])
        self.assertTrue(os.path.exists(proc_arg_list[7]))
        self.assertTrue(proc_arg_list[8].endswith('TestPySparkTask.pickle'))

    @with_config({'spark': {'py-packages': 'dummy_test_module'}})
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
            job.run()

        sc.textFile.assert_called_with('input')
        sc.textFile.return_value.saveAsTextFile.assert_called_with('output')


class SparkJobTest(unittest.TestCase):
    hcd = 'hcd-stub'
    ycd = 'ycd-stub'
    sj = 'sj-stub'
    sc = 'sc-sub'

    @with_config({'spark': {'hadoop-conf-dir': hcd, 'yarn-conf-dir': ycd, 'spark-jar': sj, 'spark-class': sc}})
    @patch('luigi.contrib.spark.subprocess.Popen')
    @patch('luigi.hdfs.HdfsTarget')
    def test_run(self, target, proc):
        setup_run_process(proc)
        job = TestSparkJob()
        job.run()
        self.assertEqual(proc.call_args[0][0], [self.sc, 'org.apache.spark.deploy.yarn.Client', '--jar', job.jar(), '--class', job.job_class(),
                                                '--num-workers', '2', '--master-memory', '1g', '--worker-memory', '1g'])

    @with_config({'spark': {'hadoop-conf-dir': hcd, 'yarn-conf-dir': ycd, 'spark-jar': sj, 'spark-class': sc}})
    @patch('luigi.contrib.spark.tempfile.TemporaryFile')
    @patch('luigi.contrib.spark.subprocess.Popen')
    def test_handle_failed_job(self, proc, file):
        proc.return_value.returncode = 1
        file.return_value = BytesIO(b'stderr')
        try:
            job = TestSparkJob()
            job.run()
        except SparkJobError as e:
            self.assertEqual(e.err, 'stderr')
            self.assertTrue('STDERR: stderr' in six.text_type(e))
        else:
            self.fail("Should have thrown SparkJobError")


class Spark1xTest(unittest.TestCase):
    ss = 'ss-stub'

    @with_config({'spark': {'spark-submit': ss}})
    @patch('luigi.contrib.spark.subprocess.Popen')
    def test_run(self, proc):
        setup_run_process(proc)
        job = TestSpark1xJob()
        job.run()
        self.assertEqual(proc.call_args[0][0], [self.ss, '--master', 'yarn-client', '--class', job.job_class(), job.jar()])

    @with_config({'spark': {'spark-submit': ss}})
    @patch('luigi.contrib.spark.tempfile.TemporaryFile')
    @patch('luigi.contrib.spark.subprocess.Popen')
    def test_handle_failed_job(self, proc, file):
        proc.return_value.returncode = 1
        file.return_value = BytesIO(b'stderr')
        try:
            job = TestSpark1xJob()
            job.run()
        except SparkJobError as e:
            self.assertEqual(e.err, 'stderr')
            self.assertTrue('STDERR: stderr' in six.text_type(e))
        else:
            self.fail("Should have thrown SparkJobError")


class PySpark1xTest(unittest.TestCase):
    ss = 'ss-stub'

    @with_config({'spark': {'spark-submit': ss}})
    @patch('luigi.contrib.spark.subprocess.Popen')
    def test_run(self, proc):
        setup_run_process(proc)
        job = TestPySpark1xJob()
        job.run()
        self.assertEqual(proc.call_args[0][0], [self.ss, '--master', 'yarn-client', job.program()])

    @with_config({'spark': {'spark-submit': ss}})
    @patch('luigi.contrib.spark.tempfile.TemporaryFile')
    @patch('luigi.contrib.spark.subprocess.Popen')
    def test_handle_failed_job(self, proc, file):
        proc.return_value.returncode = 1
        file.return_value = BytesIO(b'stderr')
        try:
            job = TestPySpark1xJob()
            job.run()
        except SparkJobError as e:
            self.assertEqual(e.err, 'stderr')
            self.assertTrue('STDERR: stderr' in six.text_type(e))
        else:
            self.fail("Should have thrown SparkJobError")
