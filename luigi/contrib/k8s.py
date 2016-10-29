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

import luigi
from luigi import configuration
import logging
import uuid
import time
import sys

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

    @property
    def kubeconfig_path(self):
        """
        Path to kubeconfing file, for cluster authentication.
        It defaults to "~/.kube/config", which is the default location
        when using minikube (http://kubernetes.io/docs/getting-started-guides/minikube).

        For more details please referer to:
        http://kubernetes.io/docs/user-guide/kubeconfig-file
        """
        return configuration.get_config().get("k8s", "kubeconfig_path", "~/.kube/config")

    @property
    def job_def(self):
        """
        Kubernetes Job definition in JSON format, example::

        {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": "pi"
            },
            "spec": {
                "template": {
                    "metadata": {
                        "name": "pi"
                    },
                    "spec": {
                        "containers": [{
                            "name": "pi",
                            "image": "perl",
                            "command": ["perl",  "-Mbignum=bpi", "-wle", "print bpi(2000)"]
                        }],
                        "restartPolicy": "Never"
                    }
                }
            }
        }

        For more details please refer to: http://kubernetes.io/docs/user-guide/jobs
        """
        pass

    @property
    def max_retrials(self):
        """
        Maximum number of allowed job failures. If this is greater than 0,
        RestartPolicy will be automatically set to "OnFailure" in the
        Job definition (job_def).
        """
        return 0

    def __track_job(self):
        """Poll job status while active"""
        job_uuid = self.job_def["metadata"]["labels"]["luigi_task_id"]
        job_name = self.job_def["metadata"]["name"]
        status = self.__get_job_status()
        while (status == "running"):
            self.__logger.debug("Kubernetes job " + job_name + " is still running")
            time.sleep(self.__POLL_TIME)
            status = self.__get_job_status()
        if(status == "succeeded"):
            self.__logger.info("Kubernetes job " + job_name + " succeeded")
        else:
            self.__logger.warning("Kubernetes job " + job_name + " failed")

    def __get_job_status(self):
        """It returns the Kubernetes job status"""
        # Look for the required job
        job_uuid = self.job_def["metadata"]["labels"]["luigi_task_id"]
        jobs = Job.objects(self.__kube_api).filter(selector="luigi_task_id=" + job_uuid)
        job_name = self.job_def["metadata"]["name"]
        # Raise an exception if no such job found
        if len(jobs.response["items"]) == 0:
            raise Exception("Kubernetes job " + job_name + " not found")

        # Compute success threshold for parallel jobs
        success_thr = 1
        if("completions" in self.job_def["spec"]):
            success_thr = int(self.job_def["spec"]["completions"])
        elif("parallelism" in self.job_def["spec"]):
            success_thr = int(self.job_def["spec"]["parallelism"])

        # Figure out status and return it
        job = Job(self.__kube_api, jobs.response["items"][0])
        if ("succeeded" in job.obj["status"]):
            succeeded_cnt = job.obj["status"]["succeeded"]
            self.__logger.debug("Kubernetes job " + job_name +
                " status.succeeded: " + str(succeeded_cnt))
            if(succeeded_cnt >= success_thr):
                return "succeeded"
        if ("failed" in job.obj["status"]):
            failed_cnt = job.obj["status"]["failed"]
            self.__logger.debug("Kubernetes job " + job_name +
                " status.failed: " + str(succeeded_cnt))
            if (failed_cnt > self.max_retrials * success_thr):
                job.scale(replicas=0) # avoid more retrials
                return "failed"
        return "running"

    def __validate_job_def(self):
        """Validate Job Definition (job_def)"""
        if (not self.job_def):
            raise ValueError("Missing Job definition (job_def)")
        if("metadata" not in self.job_def):
            raise ValueError("Missing .metadata in Job definition (job_def)")
        if("name" not in self.job_def["metadata"]):
            raise ValueError("Missing .metadata.name in Job definition (job_def)")
        if("spec" not in self.job_def):
            raise ValueError("Missing .spec in Job definition (job_def)")
        if("template" not in self.job_def["spec"]):
            raise ValueError("Missing .spec.template in Job definition (job_def)")
        if("spec" not in self.job_def["spec"]["template"]):
            raise ValueError("Missing .spec.template.spec in Job definition (job_def)")

    def __format_job_def(self):
        """
        Add some additional informations to the user-provided
        Job definition (job_def).
        """
        # Add a label to track the job
        job_uuid = str(uuid.uuid4().hex)
        if ("labels" in self.job_def["metadata"]):
            self.job_def["metadata"]["labels"]["luigi_task_id"] = job_uuid
        else:
            self.job_def["metadata"]["labels"] = { "luigi_task_id": job_uuid }
        # Add a suffix to the name
        self.job_def["metadata"]["name"] = \
            self.job_def["metadata"]["name"] + "-luigi-" + job_uuid
        # If max_retrials > 0 set RestartPolicy to OnFailure
        if (self.max_retrials > 0):
            self.job_def["spec"]["template"]["spec"]["restartPolicy"] = "OnFailure"

    def run(self):
        # Start by validating job_def
        self.__validate_job_def()
        # Format Job definition
        self.__format_job_def()
        # Submit the Job
        job_name = self.job_def["metadata"]["name"]
        self.__logger.info("Submitting Kubernetes Job: " + job_name)
        job = Job(self.__kube_api, self.job_def)
        job.create()
        # Track the Job (wait while active)
        self.__logger.info("Start tracking Kubernetes Job: " + job_name)
        self.__track_job()
