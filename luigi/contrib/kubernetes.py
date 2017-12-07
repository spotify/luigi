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
Kubernetes Job wrapper for Luigi.

From the Kubernetes website:

    Kubernetes is an open-source system for automating deployment, scaling,
    and management of containerized applications.

For more information about Kubernetes Jobs: http://kubernetes.io/docs/user-guide/jobs/

Requires:

- pykube: ``pip install pykube``

Written and maintained by Marco Capuccini (@mcapuccini).
"""

import luigi
import logging
import uuid
import time

logger = logging.getLogger('luigi-interface')

try:
    from pykube.config import KubeConfig
    from pykube.http import HTTPClient
    from pykube.objects import Job
except ImportError:
    logger.warning('pykube is not installed. KubernetesJobTask requires pykube.')


class kubernetes(luigi.Config):
    auth_method = luigi.Parameter(
        default="kubeconfig",
        description="Authorization method to access the cluster")
    kubeconfig_path = luigi.Parameter(
        default="~/.kube/config",
        description="Path to kubeconfig file for cluster authentication")
    max_retrials = luigi.IntParameter(
        default=0,
        description="Max retrials in event of job failure")


class KubernetesJobTask(luigi.Task):

    __POLL_TIME = 5  # see __track_job
    kubernetes_config = kubernetes()

    def _init_kubernetes(self):
        self.__logger = logger
        self.__logger.debug("Kubernetes auth method: " + self.auth_method)
        if(self.auth_method == "kubeconfig"):
            self.__kube_api = HTTPClient(KubeConfig.from_file(self.kubeconfig_path))
        elif(self.auth_method == "service-account"):
            self.__kube_api = HTTPClient(KubeConfig.from_service_account())
        else:
            raise ValueError("Illegal auth_method")
        self.job_uuid = str(uuid.uuid4().hex)
        self.uu_name = self.name + "-luigi-" + self.job_uuid

    @property
    def auth_method(self):
        """
        This can be set to ``kubeconfig`` or ``service-account``.
        It defaults to ``kubeconfig``.

        For more details, please refer to:

        - kubeconfig: http://kubernetes.io/docs/user-guide/kubeconfig-file
        - service-account: http://kubernetes.io/docs/user-guide/service-accounts
        """
        return self.kubernetes_config.auth_method

    @property
    def kubeconfig_path(self):
        """
        Path to kubeconfig file used for cluster authentication.
        It defaults to "~/.kube/config", which is the default location
        when using minikube (http://kubernetes.io/docs/getting-started-guides/minikube).
        When auth_method is ``service-account`` this property is ignored.

        **WARNING**: For Python versions < 3.5 kubeconfig must point to a Kubernetes API
        hostname, and NOT to an IP address.

        For more details, please refer to:
        http://kubernetes.io/docs/user-guide/kubeconfig-file
        """
        return self.kubernetes_config.kubeconfig_path

    @property
    def name(self):
        """
        A name for this job. This task will automatically append a UUID to the
        name before to submit to Kubernetes.
        """
        raise NotImplementedError("subclass must define name")

    @property
    def labels(self):
        """
        Return custom labels for kubernetes job.
        example::
        ``{"run_dt": datetime.date.today().strftime('%F')}``
        """
        return {}

    @property
    def spec_schema(self):
        """
        Kubernetes Job spec schema in JSON format, an example follows.

        .. code-block:: javascript

            {
                "containers": [{
                    "name": "pi",
                    "image": "perl",
                    "command": ["perl",  "-Mbignum=bpi", "-wle", "print bpi(2000)"]
                }],
                "restartPolicy": "Never"
            }

        **restartPolicy**

        - If restartPolicy is not defined, it will be set to "Never" by default.
        - **Warning**: restartPolicy=OnFailure will bypass max_retrials, and restart
          the container until success, with the risk of blocking the Luigi task.

        For more informations please refer to:
        http://kubernetes.io/docs/user-guide/pods/multi-container/#the-spec-schema
        """
        raise NotImplementedError("subclass must define spec_schema")

    @property
    def max_retrials(self):
        """
        Maximum number of retrials in case of failure.
        """
        return self.kubernetes_config.max_retrials

    def __track_job(self):
        """Poll job status while active"""
        while (self.__get_job_status() == "running"):
            self.__logger.debug("Kubernetes job " + self.uu_name
                                + " is still running")
            time.sleep(self.__POLL_TIME)
        if(self.__get_job_status() == "succeeded"):
            self.__logger.info("Kubernetes job " + self.uu_name + " succeeded")
            # Use signal_complete to notify of job completion
            self.signal_complete()
        else:
            raise RuntimeError("Kubernetes job " + self.uu_name + " failed")

    def signal_complete(self):
        """Signal job completion for scheduler and dependent tasks.

         Touching a system file is an easy way to signal completion. example::
         .. code-block:: python

         with self.output().open('w') as output_file:
             output_file.write('')
        """
        pass

    def __get_job_status(self):
        """Return the Kubernetes job status"""
        # Look for the required job
        jobs = Job.objects(self.__kube_api).filter(selector="luigi_task_id="
                                                            + self.job_uuid)
        # Raise an exception if no such job found
        if len(jobs.response["items"]) == 0:
            raise RuntimeError("Kubernetes job " + self.uu_name + " not found")
        # Figure out status and return it
        job = Job(self.__kube_api, jobs.response["items"][0])
        if ("succeeded" in job.obj["status"] and job.obj["status"]["succeeded"] > 0):
            job.scale(replicas=0)  # Downscale the job, but keep it for logging
            return "succeeded"
        if ("failed" in job.obj["status"]):
            failed_cnt = job.obj["status"]["failed"]
            self.__logger.debug("Kubernetes job " + self.uu_name
                                + " status.failed: " + str(failed_cnt))
            if (failed_cnt > self.max_retrials):
                job.scale(replicas=0)  # avoid more retrials
                return "failed"
        return "running"

    def run(self):
        self._init_kubernetes()
        # Render job
        job_json = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": self.uu_name,
                "labels": {
                    "spawned_by": "luigi",
                    "luigi_task_id": self.job_uuid
                }
            },
            "spec": {
                "template": {
                    "metadata": {
                        "name": self.uu_name
                    },
                    "spec": self.spec_schema
                }
            }
        }
        # Update user labels
        job_json['metadata']['labels'].update(self.labels)
        # Add default restartPolicy if not specified
        if ("restartPolicy" not in self.spec_schema):
            job_json["spec"]["template"]["spec"]["restartPolicy"] = "Never"
        # Submit job
        self.__logger.info("Submitting Kubernetes Job: " + self.uu_name)
        job = Job(self.__kube_api, job_json)
        job.create()
        # Track the Job (wait while active)
        self.__logger.info("Start tracking Kubernetes Job: " + self.uu_name)
        self.__track_job()

    def output(self):
        """
        An output target is necessary for checking job completion unless
        an alternative complete method is defined.

        Example::

            return luigi.LocalTarget(os.path.join('/tmp', 'example'))

        """
        pass
