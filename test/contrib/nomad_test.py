
# -*- coding: utf-8 -*-
#
# Copyright 2021 Volvo Car Corporation
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

"""
Tests for the Nomad Job wrapper.

Requires:

- nomad: ``pip install python-nomad``
- A local nomad cluster up and running: https://learn.hashicorp.com/nomad

Written by Anders Björklund
"""

import unittest
import luigi
import logging
import mock
from luigi.contrib.nomad import NomadJobTask

import pytest

logger = logging.getLogger('luigi-interface')

try:
    from nomad import Nomad
except ImportError:
    raise unittest.SkipTest('nomad is not installed. This test requires nomad.')


class SuccessJob(NomadJobTask):
    name = "success"
    spec_schema = {
        "TaskGroups": [{
            "Name": "main",
            "Tasks": [{
                "Name": "hello",
                "Driver": "docker",
                "Config": {
                    "image": "alpine:3.4",
                    "command": "echo",
                    "args": ["Hello World!"]
                },
            }]
        }]
    }


class FailJob(NomadJobTask):
    name = "fail"
    max_retrials = 3
    spec_schema = {
        "TaskGroups": [{
            "Name": "main",
            "Tasks": [{
                "Name": "fail",
                "Driver": "docker",
                "Config": {
                    "image": "alpine:3.4",
                    "command": "false",
                    "args": ["You",  "Shall", "Not", "Pass"]
                },
            }]
        }]
    }

    @property
    def labels(self):
        return {"dummy_label": "dummy_value"}


@pytest.mark.contrib
class TestNomadTask(unittest.TestCase):

    def test_success_job(self):
        success = luigi.run(["SuccessJob", "--local-scheduler"])
        self.assertTrue(success)

    def test_fail_job(self):
        fail = FailJob()
        self.assertRaises(AssertionError, fail.run)
        # Check for retrials
        n = Nomad()  # assumes vagrant
        job = n.job.get_job(fail.uu_name)
        self.assertTrue(job['Meta'].items() >= fail.labels.items())

    @mock.patch.object(NomadJobTask, "_NomadJobTask__verify_job_has_started")
    @mock.patch.object(NomadJobTask, "_NomadJobTask__get_job_status")
    @mock.patch.object(NomadJobTask, "signal_complete")
    def test_output(self, mock_signal, mock_job_status, mock_verify_job):
        # mock that the job has started
        mock_verify_job.return_value = True
        # mock that the job succeeded
        mock_job_status.return_value = "SUCCEEDED"
        # create a nomad job
        nomad_job = NomadJobTask()
        # set logger and uu_name due to logging in __track_job()
        nomad_job._NomadJobTask__logger = logger
        nomad_job.uu_name = "test"
        # track the job (bc included in run method)
        nomad_job._NomadJobTask__track_job()
        # Make sure successful job signals
        self.assertTrue(mock_signal.called)
