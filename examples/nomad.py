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
Example Nomad Job Task.

Requires:

- nomad: ``pip install python-nomad``
- A local nomad cluster up and running: https://learn.hashicorp.com/nomad

You can run this code example like this:

    .. code:: console
        $ luigi --module examples.nomad PerlPi --local-scheduler

Running this code will create a pi-luigi-uuid nomad job within the cluster
"""

# import os
# import luigi
from luigi.contrib.nomad import NomadJobTask


class PerlPi(NomadJobTask):

    name = "pi"
    max_retrials = 3
    spec_schema = {
        "Type": "batch",
        "TaskGroups": [{
            "Name": "main",
            "Tasks": [{
                "Name": "pi",
                "Driver": "docker",
                "Config": {
                    "image": "perl",
                    "command": "perl",
                    "args": ["-Mbignum=bpi", "-wle", "print bpi(2000)"]
                },
            }]
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
