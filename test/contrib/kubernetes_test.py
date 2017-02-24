
# -*- coding: utf-8 -*-
#
# Copyright 2015 Outlier Bio, LLC
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
Tests for the Kubernetes Job wrapper.

Requires:

- pykube: ``pip install pykube``
- A local minikube custer up and running: http://kubernetes.io/docs/getting-started-guides/minikube/

**WARNING**: For Python versions < 3.5 the kubeconfig file must point to a Kubernetes API
hostname, and NOT to an IP address.

Written and maintained by Marco Capuccini (@mcapuccini).
"""

import unittest
import luigi
import logging
import mock
from luigi.contrib.kubernetes import KubernetesJobTask

logger = logging.getLogger('luigi-interface')

try:
    from pykube.config import KubeConfig
    from pykube.http import HTTPClient
    from pykube.objects import Job
except ImportError:
    raise unittest.SkipTest('pykube is not installed. This test requires pykube.')


class SuccessJob(KubernetesJobTask):
    name = "success"
    spec_schema = {
        "containers": [{
            "name": "hello",
            "image": "alpine:3.4",
            "command": ["echo",  "Hello World!"]
        }]
    }


class FailJob(KubernetesJobTask):
    name = "fail"
    max_retrials = 3
    spec_schema = {
        "containers": [{
            "name": "fail",
            "image": "alpine:3.4",
            "command": ["You",  "Shall", "Not", "Pass"]
        }]
    }


class TestK8STask(unittest.TestCase):

    def test_success_job(self):
        success = luigi.run(["SuccessJob", "--local-scheduler"])
        self.assertTrue(success)

    def test_fail_job(self):
        fail = FailJob()
        self.assertRaises(RuntimeError, fail.run)
        # Check for retrials
        kube_api = HTTPClient(KubeConfig.from_file("~/.kube/config"))  # assumes minikube
        jobs = Job.objects(kube_api).filter(selector="luigi_task_id="
                                                     + fail.job_uuid)
        self.assertEqual(len(jobs.response["items"]), 1)
        job = Job(kube_api, jobs.response["items"][0])
        self.assertTrue("failed" in job.obj["status"])
        self.assertTrue(job.obj["status"]["failed"] > fail.max_retrials)

    @mock.patch.object(KubernetesJobTask, "_KubernetesJobTask__get_job_status")
    @mock.patch.object(KubernetesJobTask, "signal_complete")
    def test_output(self, mock_signal, mock_job_status):
        # mock that the job succeeded
        mock_job_status.return_value = "succeeded"
        # create a kubernetes job
        kubernetes_job = KubernetesJobTask()
        # set logger and uu_name due to logging in __track_job()
        kubernetes_job._KubernetesJobTask__logger = logger
        kubernetes_job.uu_name = "test"
        # track the job (bc included in run method)
        kubernetes_job._KubernetesJobTask__track_job()
        # Make sure successful job signals
        self.assertTrue(mock_signal.called)
