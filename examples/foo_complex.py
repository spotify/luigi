# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
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
You can run this example like this:

    .. code:: console

            $ rm -rf '/tmp/bar'
            $ luigi --module examples.foo_complex examples.Foo --workers 2 --local-scheduler

"""
from __future__ import division
import time
import random

import luigi

max_depth = 10
max_total_nodes = 50
current_nodes = 0


class Foo(luigi.Task):
    task_namespace = 'examples'

    def run(self):
        print("Running Foo")

    def requires(self):
        global current_nodes
        for i in range(30 // max_depth):
            current_nodes += 1
            yield Bar(i)


class Bar(luigi.Task):
    task_namespace = 'examples'

    num = luigi.IntParameter()

    def run(self):
        time.sleep(1)
        self.output().open('w').close()

    def requires(self):
        global current_nodes

        if max_total_nodes > current_nodes:
            valor = int(random.uniform(1, 30))
            for i in range(valor // max_depth):
                current_nodes += 1
                yield Bar(current_nodes)

    def output(self):
        """
        Returns the target output for this task.

        :return: the target output for this task.
        :rtype: object (:py:class:`~luigi.target.Target`)
        """
        time.sleep(1)
        return luigi.LocalTarget('/tmp/bar/%d' % self.num)
