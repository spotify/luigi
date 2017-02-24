import mock
import os

import unittest

from luigi import Parameter
from luigi.contrib import mrrunner

from luigi.contrib.hadoop import HadoopJobRunner, JobTask
from luigi.contrib.hdfs import HdfsTarget


class MockStreamingJob(JobTask):
    package_binary = Parameter(default=None)

    def output(self):
        rv = mock.MagicMock(HdfsTarget)
        rv.path = 'test_path'
        return rv


class MockStreamingJobWithExtraArguments(JobTask):
    package_binary = Parameter(default=None)

    def extra_streaming_arguments(self):
        return [('myargument', '/path/to/coolvalue')]

    def extra_archives(self):
        return ['/path/to/myarchive.zip', '/path/to/other_archive.zip']

    def output(self):
        rv = mock.MagicMock(HdfsTarget)
        rv.path = 'test_path'
        return rv


class StreamingRunTest(unittest.TestCase):

    @mock.patch('luigi.contrib.hadoop.shutil')
    @mock.patch('luigi.contrib.hadoop.run_and_track_hadoop_job')
    def test_package_binary_run(self, rath_job, shutil):
        job_runner = HadoopJobRunner('jar_path', end_job_with_atomic_move_dir=False)
        job_runner.run_job(MockStreamingJob(package_binary='test_bin.pex'))

        self.assertEqual(1, shutil.copy.call_count)
        pex_src, pex_dest = shutil.copy.call_args[0]
        runner_fname = os.path.basename(pex_dest)
        self.assertEqual('test_bin.pex', pex_src)
        self.assertEqual('mrrunner.pex', runner_fname)

        self.assertEqual(1, rath_job.call_count)
        mr_args = rath_job.call_args[0][0]
        mr_args_pairs = zip(mr_args, mr_args[1:])
        self.assertIn(('-mapper', 'python mrrunner.pex map'), mr_args_pairs)
        self.assertIn(('-file', pex_dest), mr_args_pairs)

    @mock.patch('luigi.contrib.hadoop.create_packages_archive')
    @mock.patch('luigi.contrib.hadoop.run_and_track_hadoop_job')
    def test_standard_run(self, rath_job, cpa):
        job_runner = HadoopJobRunner('jar_path', end_job_with_atomic_move_dir=False)
        job_runner.run_job(MockStreamingJob())

        self.assertEqual(1, cpa.call_count)

        self.assertEqual(1, rath_job.call_count)
        mr_args = rath_job.call_args[0][0]
        mr_args_pairs = zip(mr_args, mr_args[1:])
        self.assertIn(('-mapper', 'python mrrunner.py map'), mr_args_pairs)
        self.assertIn(('-file', mrrunner.__file__.rstrip('c')), mr_args_pairs)

    @mock.patch('luigi.contrib.hadoop.create_packages_archive')
    @mock.patch('luigi.contrib.hadoop.run_and_track_hadoop_job')
    def test_run_with_extra_arguments(self, rath_job, cpa):
        job_runner = HadoopJobRunner('jar_path', end_job_with_atomic_move_dir=False)
        job_runner.run_job(MockStreamingJobWithExtraArguments())

        self.assertEqual(1, cpa.call_count)

        self.assertEqual(1, rath_job.call_count)
        mr_args = rath_job.call_args[0][0]
        mr_args_pairs = list(zip(mr_args, mr_args[1:]))
        self.assertIn(('-myargument', '/path/to/coolvalue'), mr_args_pairs)
        self.assertIn(('-archives', '/path/to/myarchive.zip,/path/to/other_archive.zip'), mr_args_pairs)
