
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

- Official kubernetes-client python library: ``pip install kubernetes``
    - See: https://github.com/kubernetes-client/python/
- A kubectl configuration and that is active and functional
    - Can be your kubectl config setup for EKS with `aws eks update-kubeconfig --name clustername`
    - Or access to any other hosted/managed/self-setup Kubernetes cluster
    - For devs can a local minikube custer up and running: http://kubernetes.io/docs/getting-started-guides/minikube/
    - Or for devs Docker Desktop has support for minikube: https://www.docker.com/products/docker-desktop

Written and maintained by Marco Capuccini (@mcapuccini).
"""

import unittest
import luigi
import logging
# import mock
from luigi.contrib.kubernetes import KubernetesJobTask

import pytest

logger = logging.getLogger('luigi-interface')

try:
    import kubernetes as kubernetes_api
except ImportError:
    raise unittest.SkipTest('kubernetes is not installed. This test requires kubernetes.')

# Configs can be set in Configuration class directly or using helper utility, by default lets try to load in-cluster config
# and if that fails cascade into using an kube config
kubernetes_core_api = kubernetes_api.client.CoreV1Api()
try:
    kubernetes_api.config.load_incluster_config()
except Exception:
    try:
        kubernetes_api.config.load_kube_config()
    except Exception as ex:
        raise ex


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
    spec_schema = {
        "containers": [{
            "name": "fail",
            "image": "alpine:3.4",
            "command": ["You",  "Shall", "Not", "Pass"]
        }]
    }

# TODO TESTING
# class FailContainerCanNotRunInvalidCommandJob(KubernetesJobTask):
#     name = "fail"
#     backoff_limit = 1 # We will set 1 retry on purpose and check below if it is retrying properly
#     spec_schema = {
#         "containers": [{
#             "name": "invalidcommand",
#             "image": "alpine:3.4",
#             "command": ["You",  "Shall", "Not", "Pass"]
#         }]
#     }
#
#     @property
#     def labels(self):
#         return {"dummy_label": "dummy_value"}


@pytest.mark.contrib
class TestK8STask(unittest.TestCase):

    def test_success_job(self):
        success = luigi.run(["SuccessJob", "--local-scheduler"])
        self.assertTrue(success)

    def test_fail_job(self):
        fail = FailJob()
        self.assertRaises(RuntimeError, fail.run)

    # TODO WORK IN PROGRESS...
    # def test_fail_container_can_not_run_invalid_command_job(self):
    #     failure = luigi.run(["FailContainerCanNotRunInvalidCommandJob", "--local-scheduler"])
    #     self.assertFalse(failure)
    #     print("failure")
    #     print(failure)
    #     print("failure")

    # def test_fail_container_can_not_run_invalid_command_job(self):
    #     print('starting...')
    #     kubernetes_job = FailContainerCanNotRunInvalidCommandJob()
    #     print('done...')
    #     try:
    #         kubernetes_job.run()
    #         print('we are here')
    #         assert True is False
    #     except Exception as e:
    #         print("exception")
    #         print(e)
    #
    #     pods = kubernetes_job.__get_pods()
    #     print('pods')
    #     print(pods)
    #

    # def test_fail_job(self):
    #     fail = FailJob()
    #     self.assertRaises(RuntimeError, fail.run)

    # @mock.patch.object(KubernetesJobTask, "_KubernetesJobTask__get_job_status")
    # @mock.patch.object(KubernetesJobTask, "signal_complete")
    # def test_output(self, mock_signal, mock_job_status):
    #     # mock that the job succeeded
    #     mock_job_status.return_value = "succeeded"
    #     # create a kubernetes job
    #     kubernetes_job = KubernetesJobTask()
    #     # set logger and uu_name due to logging in __track_job()
    #     kubernetes_job._KubernetesJobTask__logger = logger
    #     kubernetes_job.uu_name = "test"
    #     # track the job (bc included in run method)
    #     kubernetes_job._KubernetesJobTask__track_job()
    #     # Make sure successful job signals
    #     self.assertTrue(mock_signal.called)

    # TODO:
    #
    # def test_cluster_is_scaling(self):
    #     kubernetes_job = KubernetesJobTask()
    #     condition = {
    #         "reason": "Unschedulable",
    #         "message": "0/1 nodes are available: 1 Insufficient cpu, 1 Insufficient memory."
    #     }
    #     assert kubernetes_job.__is_scaling_in_progress(condition)
    #
    #     condition = {
    #         "reason": "ContainersNotReady",
    #         "message": "0/1 nodes are available: 1 Insufficient cpu, 1 Insufficient memory."
    #     }
    #     assert kubernetes_job.__is_scaling_in_progress(condition) is False
    #
    #     condition = {
    #         "reason": "Unschedulable",
    #         "message": "1/1 nodes are available: 1 Insufficient cpu, 1 Insufficient memory."
    #     }
    #     assert kubernetes_job.__is_scaling_in_progress(condition) is True
    #
    #     condition = {
    #         "reason": "Unschedulable",
    #         "message": "other message"
    #     }
    #     assert kubernetes_job.__is_scaling_in_progress(condition) is False
    #
    #     condition = {
    #         "message": "other message"
    #     }
    #     assert kubernetes_job.__is_scaling_in_progress(condition) is False
    #
    # @mock.patch.object(KubernetesJobTask, "_KubernetesJobTask__get_job_status")
    # @mock.patch.object(KubernetesJobTask, "KubernetesJobTask__get_pods")
    # def test_output_when_scaling(self, mock_get_pods, mock_job_status):
    #     # mock that the job succeeded
    #     cond1 = {
    #         "reason": "Unschedulable",
    #         "message": "1/1 nodes are available: 1 Insufficient cpu, 1 Insufficient memory."
    #     }
    #     mock_job_status.return_value = "succeeded"
    #     mock_get_pods.return_value = [
    #         {
    #             'conditions': [
    #                 cond1
    #             ]
    #          }
    #     ]
    #     # create a kubernetes job
    #     kubernetes_job = KubernetesJobTask()
    #     # set logger and uu_name due to logging in __track_job()
    #     kubernetes_job._KubernetesJobTask__logger = logger
    #     kubernetes_job.uu_name = "test"
    #     self.assertTrue(kubernetes_job._KubernetesJobTask____verify_job_has_started())
