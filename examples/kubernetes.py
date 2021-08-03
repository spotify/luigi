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

- pykube: ``pip install pykube-ng``
- A local minikube custer up and running: http://kubernetes.io/docs/getting-started-guides/minikube/

**WARNING**: For Python versions < 3.5 the kubeconfig file must point to a Kubernetes API
hostname, and NOT to an IP address.

You can run this code example like this:

    .. code:: console
        $ luigi --module examples.kubernetes_job PerlPi --local-scheduler

Running this code will create a pi-luigi-uuid kubernetes job within the cluster
pointed to by the default context in "~/.kube/config".

If running within a kubernetes cluster, set auth_method = "service-account" to
access the local cluster.
"""

# import os
# import luigi
from luigi.contrib.kubernetes import KubernetesJobTask


class PerlPi(KubernetesJobTask):

    name = "pi"                         # The name (prefix) of the job that will be created for identification purposes
    backoff_limit = 2                   # This is the number of retries incase there is a pod/node/code failure
    kubernetes_namespace = "farley"     # This is the kubernetes namespace you wish to run in
    print_pod_logs_on_exit = True       # Set this to true if you wish to see the logs of this
    spec_schema = {                     # This is the standard "spec" of the containers in this job, this is a good sane example
        "containers": [{
            "name": "pi",
            "image": "perl",
            "command": ["perl",  "-Mbignum=bpi", "-wle", "print bpi(2000)"]
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
