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
from luigi.contrib import beam_dataflow, bigquery, gcs
from luigi import local_target
import mock
from mock import MagicMock, patch
import unittest


class TestDataflowParamKeys(beam_dataflow.DataflowParamKeys):
    runner = "runner"
    project = "project"
    zone = "zone"
    region = "region"
    staging_location = "stagingLocation"
    temp_location = "tempLocation"
    gcp_temp_location = "gcpTempLocation"
    num_workers = "numWorkers"
    autoscaling_algorithm = "autoscalingAlgorithm"
    max_num_workers = "maxNumWorkers"
    disk_size_gb = "diskSizeGb"
    worker_machine_type = "workerMachineType"
    worker_disk_type = "workerDiskType"
    job_name = "jobName"
    service_account = "serviceAccount"
    network = "network"
    subnetwork = "subnetwork"
    labels = "labels"


class TestRequires(luigi.ExternalTask):
    def output(self):
        return luigi.LocalTarget(path='some-input-dir')


class SimpleTestTask(beam_dataflow.BeamDataflowJobTask):
    dataflow_params = TestDataflowParamKeys()

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

    dataflow_params = TestDataflowParamKeys()

    def requires(self):
        return TestRequires()

    def output(self):
        return {'output': luigi.LocalTarget(path='some-output.txt')}

    def args(self):
        return ['--extraArg=present']

    def dataflow_executable(self):
        return ['java', 'com.spotify.luigi.SomeJobClass']


class FilePatternsTestTask(beam_dataflow.BeamDataflowJobTask):
    dataflow_params = TestDataflowParamKeys()

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
    dataflow_params = TestDataflowParamKeys()

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
        task = SimpleTestTask()
        task.runner = 'DirectRunner'

        expected = [
            'java',
            'com.spotify.luigi.SomeJobClass',
            '--runner=DirectRunner',
            '--input=some-input-dir/part-*',
            '--output=some-output.txt'
        ]

        self.assertEqual(task._mk_cmd_line(), expected)

    def test_dataflow_full_cmd_line_args(self):
        full_test_task = FullTestTask()
        cmd_line_args = full_test_task._mk_cmd_line()

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

    def test_dataflow_with_file_patterns(self):
        cmd_line_args = FilePatternsTestTask()._mk_cmd_line()

        self.assertIn('--input1=some-input-dir/part-*', cmd_line_args)
        self.assertIn('--input2=some-input-dir/*.some-ext', cmd_line_args)

    def test_dataflow_with_invalid_file_patterns(self):
        task = FilePatternsTestTask()
        task.file_pattern = MagicMock(return_value='notadict')
        with self.assertRaises(ValueError):
            task._mk_cmd_line()

    def test_dataflow_input_arg_formatting(self):
        class TestTaskListOfTargetsInput(SimpleTestTask):
            class TestRequiresListOfTargets(luigi.ExternalTask):
                def output(self):
                    return [luigi.LocalTarget(path='some-input-1'),
                            luigi.LocalTarget(path='some-input-2')]

            def requires(self):
                return self.TestRequiresListOfTargets()

        task_list_input = TestTaskListOfTargetsInput()
        self.assertEqual(task_list_input._format_input_args(),
                         ['--input=some-input-1/part-*,some-input-2/part-*'])

        class TestTaskListOfTuplesInput(SimpleTestTask):
            class TestRequiresListOfTuples(luigi.ExternalTask):
                def output(self):
                    return [('input1', luigi.LocalTarget(path='some-input-1')),
                            ('input2', luigi.LocalTarget(path='some-input-2'))]

            def requires(self):
                return self.TestRequiresListOfTuples()

        task_list_tuples_input = TestTaskListOfTuplesInput()
        self.assertEqual(task_list_tuples_input._format_input_args(),
                         ['--input1=some-input-1/part-*',
                          '--input2=some-input-2/part-*'])

        class TestTaskDictInput(SimpleTestTask):
            class TestRequiresDict(luigi.ExternalTask):
                def output(self):
                    return {'input1': luigi.LocalTarget(path='some-input-1'),
                            'input2': luigi.LocalTarget(path='some-input-2')}

            def requires(self):
                return self.TestRequiresDict()

        task_dict_input = TestTaskDictInput()
        self.assertEqual(task_dict_input._format_input_args(),
                         ['--input1=some-input-1/part-*',
                          '--input2=some-input-2/part-*'])

        class TestTaskTupleInput(SimpleTestTask):
            class TestRequiresTuple(luigi.ExternalTask):
                def output(self):
                    return 'some-key', luigi.LocalTarget(path='some-input')

            def requires(self):
                return self.TestRequiresTuple()

        task_tuple_input = TestTaskTupleInput()
        self.assertEqual(task_tuple_input._format_input_args(),
                         ['--some-key=some-input/part-*'])

    def test_task_output_arg_completion(self):
        class TestCompleteTarget(luigi.Target):
            def exists(self):
                return True

        class TestIncompleteTarget(luigi.Target):
            def exists(self):
                return False

        class TestTaskDictOfCompleteOutput(SimpleTestTask):
            def output(self):
                return {
                    "output": TestCompleteTarget()
                }

        self.assertEqual(TestTaskDictOfCompleteOutput().complete(), True)

        class TestTaskDictOfIncompleteOutput(SimpleTestTask):
            def output(self):
                return {
                    "output": TestIncompleteTarget()
                }

        self.assertEqual(TestTaskDictOfIncompleteOutput().complete(), False)

        class TestTaskDictOfMixedCompleteOutput(SimpleTestTask):
            def output(self):
                return {
                    "output1": TestIncompleteTarget(),
                    "output2": TestCompleteTarget()
                }

        self.assertEqual(TestTaskDictOfMixedCompleteOutput().complete(), False)

    def test_get_target_path(self):
        bq_target = bigquery.BigQueryTarget("p", "d", "t", client="fake_client")
        self.assertEqual(
            SimpleTestTask.get_target_path(bq_target),
            "p:d.t")

        gcs_target = gcs.GCSTarget("gs://foo/bar.txt", client="fake_client")
        self.assertEqual(
            SimpleTestTask.get_target_path(gcs_target),
            "gs://foo/bar.txt")

        with self.assertRaises(ValueError):
            SimpleTestTask.get_target_path("not_a_target")

    def test_dataflow_runner_resolution(self):
        task = SimpleTestTask()
        # Test that supported runners are passed through
        for runner in ["DirectRunner", "DataflowRunner"]:
            task.runner = runner
            self.assertEqual(task._get_runner(), runner)

        # Test that unsupported runners throw an error
        task.runner = "UnsupportedRunner"
        with self.assertRaises(ValueError):
            task._get_runner()

    def test_dataflow_successful_run_callbacks(self):
        task = DummyCmdLineTestTask()

        task.before_run = MagicMock()
        task.validate_output = MagicMock()
        task.on_successful_run = MagicMock()
        task.on_successful_output_validation = MagicMock()
        task.cleanup_on_error = MagicMock()

        task.run()

        task.before_run.assert_called_once_with()
        task.validate_output.assert_called_once_with()
        task.cleanup_on_error.assert_not_called()
        task.on_successful_run.assert_called_once_with()
        task.on_successful_output_validation.assert_called_once_with()

    def test_dataflow_successful_run_invalid_output_callbacks(self):
        task = DummyCmdLineTestTask()

        task.before_run = MagicMock()
        task.validate_output = MagicMock(return_value=False)
        task.on_successful_run = MagicMock()
        task.on_successful_output_validation = MagicMock()
        task.cleanup_on_error = MagicMock()

        with self.assertRaises(ValueError):
            task.run()

        task.before_run.assert_called_once_with()
        task.validate_output.assert_called_once_with()
        task.cleanup_on_error.assert_called_once_with(mock.ANY)
        task.on_successful_run.assert_called_once_with()
        task.on_successful_output_validation.assert_not_called()

    @patch('luigi.contrib.beam_dataflow.subprocess.Popen.wait', return_value=1)
    @patch('luigi.contrib.beam_dataflow.os._exit', side_effect=OSError)
    def test_dataflow_failed_run_callbacks(self, popen, os_exit):
        task = DummyCmdLineTestTask()

        task.before_run = MagicMock()
        task.validate_output = MagicMock()
        task.on_successful_run = MagicMock()
        task.on_successful_output_validation = MagicMock()
        task.cleanup_on_error = MagicMock()

        with self.assertRaises(OSError):
            task.run()

        task.before_run.assert_called_once_with()
        task.validate_output.assert_not_called()
        task.cleanup_on_error.assert_called_once_with(mock.ANY)
        task.on_successful_run.assert_not_called()
        task.on_successful_output_validation.assert_not_called()
