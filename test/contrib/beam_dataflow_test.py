# -*- coding: utf-8 -*-
#
# Copyright 2019 Spotify AB
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

import json
import luigi
from luigi.contrib import beam_dataflow
from luigi import local_target
from mock import MagicMock, patch
import unittest


class TestDataflowParams(beam_dataflow.DataflowParams):
    @property
    def runner(self):
        return "runner"

    @property
    def project(self):
        return "project"

    @property
    def zone(self):
        return "zone"

    @property
    def region(self):
        return "region"

    @property
    def staging_location(self):
        return "stagingLocation"

    @property
    def temp_location(self):
        return "tempLocation"

    @property
    def gcp_temp_location(self):
        return "gcpTempLocation"

    @property
    def num_workers(self):
        return "numWorkers"

    @property
    def autoscaling_algorithm(self):
        return "autoscalingAlgorithm"

    @property
    def max_num_workers(self):
        return "maxNumWorkers"

    @property
    def disk_size_gb(self):
        return "diskSizeGb"

    @property
    def worker_machine_type(self):
        return "workerMachineType"

    @property
    def worker_disk_type(self):
        return "workerDiskType"

    @property
    def job_name(self):
        return "jobName"

    @property
    def service_account(self):
        return "serviceAccount"

    @property
    def network(self):
        return "network"

    @property
    def subnetwork(self):
        return "subnetwork"

    @property
    def labels(self):
        return "labels"


class TestRequires(luigi.ExternalTask):
    def output(self):
        return luigi.LocalTarget(path='some-input-dir')


class SimpleTestTask(beam_dataflow.BeamDataflowJobTask):
    dataflow_params = TestDataflowParams()

    def requires(self):
        return TestRequires()

    def output(self):
        return local_target.LocalTarget(path='some-output.txt')

    def dataflow_executable(self):
        return ['java', 'com.spotify.luigi.SomeJobClass']


class FullTestTask(beam_dataflow.BeamDataflowJobTask):
    project = 'some-project'
    runner = 'DirectRunner'
    temp_location = 'some-temp'
    staging_location = 'some-staging'
    gcp_temp_location = 'some-gcp-temp'
    num_workers = 1
    autoscaling_algorithm = 'THROUGHPUT_BASED'
    max_num_workers = 2
    network = 'some-network'
    subnetwork = 'some-subnetwork'
    disk_size_gb = 5
    worker_machine_type = 'n1-standard-4'
    job_name = 'SomeJobName'
    worker_disk_type = 'compute.googleapis.com/projects//zones//diskTypes/pd-ssd'
    service_account = 'some-service-account@google.com'
    zone = 'europe-west1-c'
    region = 'europe-west1'
    labels = {'k1': 'v1'}

    dataflow_params = TestDataflowParams()

    def requires(self):
        return TestRequires()

    def output(self):
        return {'output': luigi.LocalTarget(path='some-output.txt')}

    def args(self):
        return ['--extraArg=present']

    def dataflow_executable(self):
        return ['java', 'com.spotify.luigi.SomeJobClass']


class FilePatternsTestTask(beam_dataflow.BeamDataflowJobTask):
    dataflow_params = TestDataflowParams()

    def requires(self):
        return {
            'input1': TestRequires(),
            'input2': TestRequires()
        }

    def file_pattern(self):
        return {'input2': '*.some-ext'}

    def output(self):
        return {'output': luigi.LocalTarget(path='some-output.txt')}

    def dataflow_executable(self):
        return ['java', 'com.spotify.luigi.SomeJobClass']


class DummyCmdLineTestTask(beam_dataflow.BeamDataflowJobTask):
    dataflow_params = TestDataflowParams()

    def dataflow_executable(self):
        pass

    def requires(self):
        return {}

    def output(self):
        return {}

    def _mk_cmd_line(self):
        return ['echo', '"hello world"']


class BeamDataflowTest(unittest.TestCase):

    def test_dataflow_simple_cmd_line_args(self):
        cmd_line_args = SimpleTestTask()._mk_cmd_line()

        expected = [
            'java',
            'com.spotify.luigi.SomeJobClass',
            '--runner=DirectRunner',
            '--input=some-input-dir/part-*',
            '--output=some-output.txt'
        ]

        self.assertEqual(cmd_line_args, expected)

    def test_dataflow_full_cmd_line_args(self):
        task = FullTestTask()
        cmd_line_args = task._mk_cmd_line()

        expected = [
            'java',
            'com.spotify.luigi.SomeJobClass',
            '--runner=DirectRunner',
            '--project=some-project',
            '--zone=europe-west1-c',
            '--region=europe-west1',
            '--stagingLocation=some-staging',
            '--tempLocation=some-temp',
            '--gcpTempLocation=some-gcp-temp',
            '--numWorkers=1',
            '--autoscalingAlgorithm=THROUGHPUT_BASED',
            '--maxNumWorkers=2',
            '--diskSizeGb=5',
            '--workerMachineType=n1-standard-4',
            '--workerDiskType=compute.googleapis.com/projects//zones//diskTypes/pd-ssd',
            '--network=some-network',
            '--subnetwork=some-subnetwork',
            '--jobName=SomeJobName',
            '--serviceAccount=some-service-account@google.com',
            '--labels={"k1": "v1"}',
            '--extraArg=present',
            '--input=some-input-dir/part-*',
            '--output=some-output.txt'
        ]

        self.assertEqual(json.loads(cmd_line_args[19][9:]), {'k1': 'v1'})
        self.assertEqual(cmd_line_args, expected)
        self.assertEqual(task.output_uris, ["some-output.txt"])

    def test_dataflow_with_file_patterns(self):
        cmd_line_args = FilePatternsTestTask()._mk_cmd_line()

        self.assertIn('--input1=some-input-dir/part-*', cmd_line_args)
        self.assertIn('--input2=some-input-dir/*.some-ext', cmd_line_args)

    def test_dataflow_with_invalid_file_patterns(self):
        task = FilePatternsTestTask()
        task.file_pattern = MagicMock(return_value='notadict')
        with self.assertRaises(ValueError):
            task._mk_cmd_line()

    def test_dataflow_runner_resolution(self):
        task = SimpleTestTask()
        # Test that supported runners are passed through
        for runner in ["DirectRunner", "BlockingDataflowPipelineRunner",
                       "InProcessPipelineRunner", "DataflowRunner"]:
            task.runner = runner
            self.assertEqual(task._get_runner(), runner)

        # Test that unsupported runners default to DirectRunner
        task.runner = "UnsupportedRunner"
        self.assertEqual(task._get_runner(), "DirectRunner")

    def test_dataflow_successful_run_callbacks(self):
        task = DummyCmdLineTestTask()

        task.validate_output = MagicMock()
        task.on_successful_run = MagicMock()
        task.on_output_validation = MagicMock()
        task.cleanup_on_error = MagicMock()

        task.run()

        task.validate_output.assert_called_once_with()
        task.cleanup_on_error.assert_not_called()
        task.on_successful_run.assert_called_once_with()
        task.on_output_validation.assert_called_once_with()

    def test_dataflow_successful_run_invalid_output_callbacks(self):
        task = DummyCmdLineTestTask()

        task.validate_output = MagicMock(return_value=False)
        task.on_successful_run = MagicMock()
        task.on_output_validation = MagicMock()
        task.cleanup_on_error = MagicMock()

        with self.assertRaises(ValueError):
            task.run()

        task.validate_output.assert_called_once_with()
        task.cleanup_on_error.assert_called_once_with()
        task.on_successful_run.assert_called_once_with()
        task.on_output_validation.assert_not_called()

    @patch('luigi.contrib.beam_dataflow.subprocess.Popen.wait', return_value=1)
    @patch('luigi.contrib.beam_dataflow.os._exit', side_effect=OSError)
    def test_dataflow_failed_run_callbacks(self, popen, os_exit):
        task = DummyCmdLineTestTask()

        task.validate_output = MagicMock()
        task.on_successful_run = MagicMock()
        task.on_output_validation = MagicMock()
        task.cleanup_on_error = MagicMock()

        with self.assertRaises(OSError):
            task.run()

        task.validate_output.assert_not_called()
        task.cleanup_on_error.assert_called_once_with()
        task.on_successful_run.assert_not_called()
        task.on_output_validation.assert_not_called()
