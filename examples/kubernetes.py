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
Example Kubernetes Job Task.

Requires:

- Official kubernetes-client python library: ``pip install kubernetes``
    - See: https://github.com/kubernetes-client/python/
- Run Within' an Kubernetes cluster with an ClusterRole granting it access to Kubernetes APIs
- OR A working kubectl configuration and that is active and functional
    - This can be your kubectl config setup for EKS with `aws eks update-kubeconfig --name clustername`
    - Or access to any other hosted/managed/self-setup Kubernetes cluster
    - For devs can a local minikube custer up and running: http://kubernetes.io/docs/getting-started-guides/minikube/
    - Or for devs Docker Desktop has support for minikube: https://www.docker.com/products/docker-desktop

You can run this code example like this:

    .. code:: console
        $ PYTHONPATH=. luigi --module examples.kubernetes_job PerlPi --local-scheduler
        # Or alternatively...
        $ python -m luigi --module examples.kubernetes PerlPi --local-scheduler

Running this code will create a pi-luigi-uuid kubernetes job within the cluster
of whatever your current context is for kubectl.  The login herein will auto-detect if this
is running in an Kubernetes Cluster and has functional access to the local cluster's APIs
via an ClusterRole, and if not it will then try to use the kubeconfig and its various environment
variables to support configuring and tweaking kubectl.

DEPRECATION NOTE: The previous version of the kubernetes library had you configure your kubectl configuration file
as an property in the config file.  This was removed in favor of using the kubectl standards.  To specify a config use
the env variable KUBECONFIG.  For example:

    .. code:: console
        $ KUBECONFIG=~/.kube/my-custom-config PYTHONPATH=. luigi --module examples.kubernetes_job PerlPi --local-scheduler

For more see: https://kubernetes.io/docs/tasks/access-application-cluster/configure-access-multiple-clusters/
"""

# import os
# import luigi
from luigi.contrib.kubernetes import KubernetesJobTask

class PerlPi(KubernetesJobTask):

    name = "pi"                         # The name (prefix) of the job that will be created for identification purposes
    kubernetes_namespace = "default"    # This is the kubernetes namespace you wish to run, if not specified it uses "default"
    labels = {"job_name": "pi"}         # This is to add labels in Kubernetes to help identify it, this is on top of the internal luigi labels
    backoff_limit = 0                   # This is the number of retries incase there is a pod/node/code failure, default 6
    active_deadline_seconds = None      # This is a "timeout" in seconds, how long to wait for the pods to schedule and execute before failing, default None
    poll_interval = 1                   # To poll more regularly reduce this number, default 5
    print_pod_logs_on_exit = False      # Set this to True if you wish to see the logs of this run after completion, False by default
    print_pod_logs_during_run = True    # Set this to True if you wish to see the logs of this run while it is running, False by default
    delete_on_success = True            # Set this to False to keep the job after it finishes successfully, for debugging purposes, True by default
    spec_schema = {                     # This is the standard "spec" of the containers in this job, this is a good sane example with resource requests/limits
        "containers": [{
            "name": "pi",
            "image": "perl",
            "command": ["perl",  "-Mbignum=bpi", "-wle", "print bpi(2000)"],
            "resources": {
                "requests": {
                    "cpu": "50m",
                    "memory": "50Mi"
                },
                "limits": {
                    "cpu": "100m",
                    "memory": "100Mi"
                }
            }
        }]
    }

    # defining the two functions below allows for dependency checking,
    # but isn't a requirement
    # def signal_complete(self):
    #     with self.output().open('w') as output:
    #         output.write('')
    #
    # def output(self):
    #     target = os.path.join("/tmp", "PerlPi")
    #     return luigi.LocalTarget(target)
