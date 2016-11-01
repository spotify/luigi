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
from luigi.contrib.k8s_job import KubernetesJobTask

class PerlPi(KubernetesJobTask):

    name = "pi"
    max_retrials = 3
    spec_schema = {
        "containers": [{
            "name": "pi",
            "image": "perl",
            "command": ["perl",  "-Mbignum=bpi", "-wle", "print bpi(2000)"]
        }]
    }

if __name__ == "__main__":
    luigi.run(['PerlPi', '--local-scheduler'])
