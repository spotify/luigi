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

- Official kubernetes-client python library: ``pip install kubernetes``
    - See: https://github.com/kubernetes-client/python/

Written and maintained by Marco Capuccini (@mcapuccini)
Pivoted to official kubernetes-client python module by Farley (@AndrewFarley)
"""
import logging
import time
import uuid
from datetime import datetime

import luigi

logger = logging.getLogger('luigi-interface')

try:
    import kubernetes as kubernetes_api
except ImportError as i:
    logger.warning("WARNING: kubernetes is not installed. KubernetesJobTask requires kubernetes")
    logger.warning("  Please run 'pip install kubernetes' and try again")


class kubernetes(luigi.Config):
    kubernetes_namespace = luigi.OptionalParameter(
        default=None,
        description="K8s namespace in which the job will run")

class KubernetesJobTask(luigi.Task):
    __DEFAULT_POLL_INTERVAL = 5  # see __track_job
    __DEFAULT_POD_CREATION_INTERVAL = 5

    def _init_kubernetes(self):
        self.__logger = logger
        self.__kubernetes_core_api = kubernetes_api.client.CoreV1Api()
        # Configs can be set in Configuration class directly or using helper utility, by default lets try to load in-cluster config
        # TODO: Make library support forcing one of these instead of automatic cascading logic...?
        try:
           kubernetes_api.config.load_incluster_config()
        except Exception as e:
           try:
              kubernetes_api.config.load_kube_config()
           except Exception as ex:
              raise ex
        
        # Create our API instances for Kubernetes
        self.__kubernetes_api_instance = kubernetes_api.client.CoreV1Api()
        self.__kubernetes_batch_instance = kubernetes_api.client.BatchV1Api()
        
        self.job_uuid = str(uuid.uuid4().hex)
        now = datetime.utcnow()
        
        # Set a namespace if not specified because we run jobs in a specific namespace, always
        if self.kubernetes_namespace is None:
            self.kubernetes_namespace = "default"

        self.uu_name = "%s-%s-%s" % (self.name, now.strftime('%Y%m%d%H%M%S'), self.job_uuid[:16])

    @property
    def kubernetes_namespace(self):
        """
        Namespace in Kubernetes where the job will run.
        It defaults to the default namespace in Kubernetes

        For more details, please refer to:
        https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/
        """
        return "default"

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
    def backoff_limit(self):
        """
        Maximum number of retries before considering the job as failed.  6 times is the Kubernetes default
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
        api_response = self.__kubernetes_api_instance.list_namespaced_pod(self.kubernetes_namespace, limit=10, label_selector="job-name=" + self.uu_name)
        return api_response.items

    def __get_job(self):
        api_response = self.__kubernetes_batch_instance.list_namespaced_job(self.kubernetes_namespace, limit=10, label_selector="luigi_task_id=" + self.job_uuid)
        assert len(api_response.items) == 1, "Kubernetes job " + self.uu_name + " not found"
        return api_response.items[0]

    def __print_pod_logs(self):
        for pod in self.__get_pods():
            self.__logger.info("Fetching logs from " + pod.metadata.name)
            try:
                api_response = self.__kubernetes_api_instance.read_namespaced_pod_log(pod.metadata.name, self.kubernetes_namespace, timestamps=True)
                if len(api_response) > 0:
                    for line in api_response.split('\n'):
                        self.__logger.info(line)
            except Exception as e:
                logger.warning("WARNING: Unable to get logs because...")
                logger.warning(e)

    def __print_kubectl_hints(self):
        self.__logger.info("To stream Pod logs, use:")
        for pod in self.__get_pods():
            self.__logger.info("`kubectl logs -f pod/%s -n %s`" % (pod.metadata.name, pod.metadata.namespace))

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
            for cont_stats in pod.status.container_statuses:
                if cont_stats.state.terminated is not None:
                    t = cont_stats.state.terminated
                    err_msg = "Pod %s %s (exit code %d). Logs: `kubectl logs pod/%s`" % (
                        pod.name, t['reason'], t['exitCode'], pod.name)
                    assert t['exitCode'] == 0, err_msg

                if cont_stats.state.waiting is not None:
                    wr = cont_stats.state.waiting.reason
                    assert wr == 'ContainerCreating', "Pod %s %s. Logs: `kubectl logs pod/%s`" % (
                        pod.name, wr, pod.name)

            for cond in pod.status.conditions:
                if cond.message is not None:
                    if cond.message == 'ContainersNotReady':
                        return False
                    if cond.status != 'False':
                        self.__logger.warning("[ERROR] %s - %s" % (cond.reason, cond.message))
                        return False
        return True

    def __scale_down_job(self, job_name):
        api_response = self.__kubernetes_batch_instance.patch_namespaced_job(
            job_name, self.kubernetes_namespace, {"spec": {"parallelism": 0, "backoff_limit": 0}})
        if api_response.spec.parallelism == 0:
            return True
        return False

    def __get_job_status(self):
        """Return the Kubernetes job status"""
        # Figure out status and return it
        job = self.__get_job()

        if job.status.succeeded is not None:
            self.__scale_down_job(job.metadata.name)
            if self.print_pod_logs_on_exit:
                self.__print_pod_logs()
            if self.delete_on_success:
                self.__delete_job_cascade(job)
            return "SUCCEEDED"

        if job.status.failed is not None:
            failed_cnt = job.status.failed
            self.__logger.debug("Kubernetes job " + self.uu_name
                                + " status.failed: " + str(failed_cnt))
            if self.print_pod_logs_on_exit:
                self.__print_pod_logs()
            if failed_cnt >= self.backoff_limit:
                self.__scale_down_job(job.metadata.name)
                return "FAILED"
        return "RUNNING"

    def __delete_job_cascade(self, job):
        api_response = self.__kubernetes_batch_instance.delete_namespaced_job(job.metadata.name, self.kubernetes_namespace, body={"grace_period_seconds": 0, "propagation_policy": "Background"})
        # TODO: Check status of this request...?
        print(api_response.status)

    def run(self):
        self._init_kubernetes()

        job_metadata = {
            "name": self.uu_name,
            "labels": {
                "spawned_by": "luigi",
                "luigi_task_id": self.job_uuid,
                "luigi_name": self.uu_name,
            }
        }
        
        job_spec = {
            "backoffLimit": self.backoff_limit,
            "template": {
                "metadata": {
                    "name": self.uu_name,
                    "labels": {}
                },
                "spec": self.spec_schema
            }
        }

        if self.kubernetes_namespace is not None:
            job_metadata['namespace'] = self.kubernetes_namespace
        if self.active_deadline_seconds is not None:
            job_spec['activeDeadlineSeconds'] = self.active_deadline_seconds

        # Update user labels
        job_metadata['labels'].update(self.labels)
        job_spec['template']['metadata']['labels'].update(self.labels)

        # Add default restartPolicy if not specified
        if "restartPolicy" not in self.spec_schema:
            job_spec["template"]["spec"]["restartPolicy"] = "Never"
        # Submit job
        self.__logger.info("Submitting Kubernetes Job: " + self.uu_name)
        body = kubernetes_api.client.V1Job(metadata=job_metadata, spec=job_spec)
        
        try:
            api_response = self.__kubernetes_batch_instance.create_namespaced_job(self.kubernetes_namespace, body)
            self.__logger.info("Successfully Created Kubernetes Job uid: " + api_response.metadata.uid)
        except kubernetes_api.client.rest.ApiException as e:
           print("Exception when calling BatchV1Api->create_namespaced_job: %s\n" % e)


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
