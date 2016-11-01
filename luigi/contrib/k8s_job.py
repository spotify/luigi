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
Kubernetes Job wrapper for Luigi

From the Kubernetes website:

    Kubernetes is an open-source system for automating deployment, scaling,
    and management of containerized applications.

For more information about Kubernetes Jobs: http://kubernetes.io/docs/user-guide/jobs/

Requires:

- pykube: ``pip install pykube``

Written and maintained by Marco Capuccini (@mcapuccini)
"""

import luigi
from luigi import configuration
import logging
import uuid
import time

try:
    from pykube.config import KubeConfig
    from pykube.http import HTTPClient
    from pykube.objects import Job
except ImportError:
    logger.warning('pykube is not installed. KubernetesJobTask requires pykube.')

class KubernetesJobTask(luigi.Task):

    __POLL_TIME = 5 # see __track_job

    def __init__(self, *args, **kwargs):
        super(KubernetesJobTask, self).__init__(*args, **kwargs)
        self.__kube_api = HTTPClient(KubeConfig.from_file(self.kubeconfig_path))
        self.__logger = logging.getLogger('luigi-interface')
        self.__job_uuid = str(uuid.uuid4().hex)
        self.__uu_name = self.name + "-luigi-" + self.__job_uuid
        if (self.max_retrials > 0):
            self.spec_schema["restartPolicy"] = "OnFailure"

    @property
    def kubeconfig_path(self):
        """
        Path to kubeconfing file, for cluster authentication.
        It defaults to "~/.kube/config", which is the default location
        when using minikube (http://kubernetes.io/docs/getting-started-guides/minikube).

        **WARNING**: For Python versions < 3.5 kubeconfing must point to a Kubernetes API
        hostname, and NOT to an IP address.

        For more details please referer to:
        http://kubernetes.io/docs/user-guide/kubeconfig-file
        """
        return configuration.get_config().get("k8s", "kubeconfig_path", "~/.kube/config")

    @property
    def name(self):
        """
        A name for this job. This task will automatically append a UUID to the
        name before to submit to Kubernetes.
        """
        pass

    @property
    def spec_schema(self):
        """
        Kubernetes Job spec schema in JSON format, example::

            {
                "containers": [{
                    "name": "pi",
                    "image": "perl",
                    "command": ["perl",  "-Mbignum=bpi", "-wle", "print bpi(2000)"]
                }],
                "restartPolicy": "Never"
            }

        For more informations please refer to:
        http://kubernetes.io/docs/user-guide/pods/multi-container/#the-spec-schema
        """

    @property
    def max_retrials(self):
        """
        Maximum number of retrials in case of failure. If this is greater than 0,
        the RestartPolicy will be automatically set to "OnFailure" in the
        spec schema definition (spec_schema).
        """
        return 0

    def __track_job(self):
        """Poll job status while active"""
        while (self.__get_job_status() == "running"):
            self.__logger.debug("Kubernetes job " + self.__uu_name + " is still running")
            time.sleep(self.__POLL_TIME)
        if(self.__get_job_status() == "succeeded"):
            self.__logger.info("Kubernetes job " + self.__uu_name + " succeeded")
        else:
            raise Exception("Kubernetes job " + self.__uu_name + " failed")

    def __get_job_status(self):
        """It returns the Kubernetes job status"""
        # Look for the required job
        jobs = Job.objects(self.__kube_api).filter(selector="luigi_task_id=" + self.__job_uuid)
        # Raise an exception if no such job found
        if len(jobs.response["items"]) == 0:
            raise Exception("Kubernetes job " + self.__uu_name + " not found")
        # Figure out status and return it
        job = Job(self.__kube_api, jobs.response["items"][0])
        if ("succeeded" in job.obj["status"] and job.obj["status"]["succeeded"] > 0):
            return "succeeded"
        if ("failed" in job.obj["status"]):
            failed_cnt = job.obj["status"]["failed"]
            self.__logger.debug("Kubernetes job " + self.__uu_name +
                " status.failed: " + str(failed_cnt))
            if (failed_cnt > self.max_retrials):
                job.scale(replicas=0) # avoid more retrials
                return "failed"
        return "running"

    def run(self):
        # Submit the Job
        self.__logger.info("Submitting Kubernetes Job: " + self.__uu_name)
        job_json = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": self.__uu_name,
                "labels": {
                    "luigi_task_id": self.__job_uuid
                }
            },
            "spec": {
                "template": {
                    "metadata": {
                        "name": self.__uu_name
                    },
                    "spec": self.spec_schema
                }
            }
        }
        job = Job(self.__kube_api, job_json)
        job.create()
        # Track the Job (wait while active)
        self.__logger.info("Start tracking Kubernetes Job: " + self.__uu_name)
        self.__track_job()
