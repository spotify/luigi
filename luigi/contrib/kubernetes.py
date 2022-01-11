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

- pykube: ``pip install pykube-ng``

Written and maintained by Marco Capuccini (@mcapuccini).
"""
import logging
import time
import uuid
from datetime import datetime

import luigi

logger = logging.getLogger('luigi-interface')

try:
    from pykube.config import KubeConfig
    from pykube.http import HTTPClient
    from pykube.objects import Job, Pod
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
    kubernetes_namespace = luigi.OptionalParameter(
        default=None,
        description="K8s namespace in which the job will run")


class KubernetesJobTask(luigi.Task):
    __DEFAULT_POLL_INTERVAL = 5  # see __track_job
    __DEFAULT_POD_CREATION_INTERVAL = 5
    _kubernetes_config = None  # Needs to be loaded at runtime

    def _init_kubernetes(self):
        self.__logger = logger
        self.__logger.debug("Kubernetes auth method: " + self.auth_method)
        if self.auth_method == "kubeconfig":
            self.__kube_api = HTTPClient(KubeConfig.from_file(self.kubeconfig_path))
        elif self.auth_method == "service-account":
            self.__kube_api = HTTPClient(KubeConfig.from_service_account())
        else:
            raise ValueError("Illegal auth_method")
        self.job_uuid = str(uuid.uuid4().hex)
        now = datetime.utcnow()
        self.uu_name = "%s-%s-%s" % (self.name, now.strftime('%Y%m%d%H%M%S'), self.job_uuid[:16])

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
    def kubernetes_namespace(self):
        """
        Namespace in Kubernetes where the job will run.
        It defaults to the default namespace in Kubernetes

        For more details, please refer to:
        https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/
        """
        return self.kubernetes_config.kubernetes_namespace

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

    @property
    def backoff_limit(self):
        """
        Maximum number of retries before considering the job as failed.
        See: https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/#pod-backoff-failure-policy
        """
        return 6

    @property
    def delete_on_success(self):
        """
        Delete the Kubernetes workload if the job has ended successfully.
        """
        return True

    @property
    def print_pod_logs_on_exit(self):
        """
        Fetch and print the pod logs once the job is completed.
        """
        return False

    @property
    def active_deadline_seconds(self):
        """
        Time allowed to successfully schedule pods.
        See: https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/#job-termination-and-cleanup
        """
        return None

    @property
    def kubernetes_config(self):
        if not self._kubernetes_config:
            self._kubernetes_config = kubernetes()
        return self._kubernetes_config

    @property
    def poll_interval(self):
        """How often to poll Kubernetes for job status, in seconds."""
        return self.__DEFAULT_POLL_INTERVAL

    @property
    def pod_creation_wait_interal(self):
        """Delay for initial pod creation for just submitted job in seconds"""
        return self.__DEFAULT_POD_CREATION_INTERVAL

    def __track_job(self):
        """Poll job status while active"""
        while not self.__verify_job_has_started():
            time.sleep(self.poll_interval)
            self.__logger.debug("Waiting for Kubernetes job " + self.uu_name + " to start")
        self.__print_kubectl_hints()

        status = self.__get_job_status()
        while status == "RUNNING":
            self.__logger.debug("Kubernetes job " + self.uu_name + " is running")
            time.sleep(self.poll_interval)
            status = self.__get_job_status()

        assert status != "FAILED", "Kubernetes job " + self.uu_name + " failed"

        # status == "SUCCEEDED"
        self.__logger.info("Kubernetes job " + self.uu_name + " succeeded")
        self.signal_complete()

    def signal_complete(self):
        """Signal job completion for scheduler and dependent tasks.

         Touching a system file is an easy way to signal completion. example::
         .. code-block:: python

         with self.output().open('w') as output_file:
             output_file.write('')
        """
        pass

    def __get_pods(self):
        pod_objs = Pod.objects(self.__kube_api, namespace=self.kubernetes_namespace) \
            .filter(selector="job-name=" + self.uu_name) \
            .response['items']
        return [Pod(self.__kube_api, p) for p in pod_objs]

    def __get_job(self):
        jobs = Job.objects(self.__kube_api, namespace=self.kubernetes_namespace) \
            .filter(selector="luigi_task_id=" + self.job_uuid) \
            .response['items']
        assert len(jobs) == 1, "Kubernetes job " + self.uu_name + " not found"
        return Job(self.__kube_api, jobs[0])

    def __print_pod_logs(self):
        for pod in self.__get_pods():
            logs = pod.logs(timestamps=True).strip()
            self.__logger.info("Fetching logs from " + pod.name)
            if len(logs) > 0:
                for line in logs.split('\n'):
                    self.__logger.info(line)

    def __print_kubectl_hints(self):
        self.__logger.info("To stream Pod logs, use:")
        for pod in self.__get_pods():
            self.__logger.info("`kubectl logs -f pod/%s -n %s`" % (pod.name, pod.namespace))

    def __verify_job_has_started(self):
        """Asserts that the job has successfully started"""
        # Verify that the job started
        self.__get_job()

        # Verify that the pod started
        pods = self.__get_pods()
        if not pods:
            self.__logger.debug(
                'No pods found for %s, waiting for cluster state to match the job definition' % self.uu_name
            )
            time.sleep(self.pod_creation_wait_interal)
            pods = self.__get_pods()

        assert len(pods) > 0, "No pod scheduled by " + self.uu_name
        for pod in pods:
            status = pod.obj['status']
            for cont_stats in status.get('containerStatuses', []):
                if 'terminated' in cont_stats['state']:
                    t = cont_stats['state']['terminated']
                    err_msg = "Pod %s %s (exit code %d). Logs: `kubectl logs pod/%s`" % (
                        pod.name, t['reason'], t['exitCode'], pod.name)
                    assert t['exitCode'] == 0, err_msg

                if 'waiting' in cont_stats['state']:
                    wr = cont_stats['state']['waiting']['reason']
                    assert wr == 'ContainerCreating', "Pod %s %s. Logs: `kubectl logs pod/%s`" % (
                        pod.name, wr, pod.name)

            for cond in status.get('conditions', []):
                if 'message' in cond:
                    if cond['reason'] == 'ContainersNotReady':
                        return False
                    assert cond['status'] != 'False', \
                        "[ERROR] %s - %s" % (cond['reason'], cond['message'])
        return True

    def __get_job_status(self):
        """Return the Kubernetes job status"""
        # Figure out status and return it
        job = self.__get_job()

        if "succeeded" in job.obj["status"] and job.obj["status"]["succeeded"] > 0:
            job.scale(replicas=0)
            if self.print_pod_logs_on_exit:
                self.__print_pod_logs()
            if self.delete_on_success:
                self.__delete_job_cascade(job)
            return "SUCCEEDED"

        if "failed" in job.obj["status"]:
            failed_cnt = job.obj["status"]["failed"]
            self.__logger.debug("Kubernetes job " + self.uu_name
                                + " status.failed: " + str(failed_cnt))
            if self.print_pod_logs_on_exit:
                self.__print_pod_logs()
            if failed_cnt > self.max_retrials:
                job.scale(replicas=0)  # avoid more retrials
                return "FAILED"
        return "RUNNING"

    def __delete_job_cascade(self, job):
        delete_options_cascade = {
            "kind": "DeleteOptions",
            "apiVersion": "v1",
            "propagationPolicy": "Background"
        }
        r = self.__kube_api.delete(json=delete_options_cascade, **job.api_kwargs())
        if r.status_code != 200:
            self.__kube_api.raise_for_status(r)

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
                "backoffLimit": self.backoff_limit,
                "template": {
                    "metadata": {
                        "name": self.uu_name,
                        "labels": {}
                    },
                    "spec": self.spec_schema
                }
            }
        }
        if self.kubernetes_namespace is not None:
            job_json['metadata']['namespace'] = self.kubernetes_namespace
        if self.active_deadline_seconds is not None:
            job_json['spec']['activeDeadlineSeconds'] = \
                self.active_deadline_seconds
        # Update user labels
        job_json['metadata']['labels'].update(self.labels)
        job_json['spec']['template']['metadata']['labels'].update(self.labels)

        # Add default restartPolicy if not specified
        if "restartPolicy" not in self.spec_schema:
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
