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

from helpers import unittest

import luigi
import luigi.notifications

luigi.notifications.DEBUG = True


class PrioTask(luigi.Task):
    prio = luigi.Parameter()
    run_counter = 0

    @property
    def priority(self):
        return self.prio

    def requires(self):
        if self.prio > 10:
            return PrioTask(self.prio - 10)

    def run(self):
        self.t = PrioTask.run_counter
        PrioTask.run_counter += 1

    def complete(self):
        return hasattr(self, 't')


class PriorityTest(unittest.TestCase):

    def test_priority(self):
        p, q, r = PrioTask(1), PrioTask(2), PrioTask(3)
        luigi.build([p, q, r], local_scheduler=True)
        self.assertTrue(r.t < q.t < p.t)

    def test_priority_w_dep(self):
        x, y, z = PrioTask(25), PrioTask(15), PrioTask(5)
        a, b, c = PrioTask(24), PrioTask(14), PrioTask(4)
        luigi.build([a, b, c, x, y, z], local_scheduler=True)
        self.assertTrue(z.t < y.t < x.t < c.t < b.t < a.t)
