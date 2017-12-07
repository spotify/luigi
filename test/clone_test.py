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


class LinearSum(luigi.Task):
    lo = luigi.IntParameter()
    hi = luigi.IntParameter()

    def requires(self):
        if self.hi > self.lo:
            return self.clone(hi=self.hi - 1)

    def run(self):
        if self.hi > self.lo:
            self.s = self.requires().s + self.f(self.hi - 1)
        else:
            self.s = 0
        self.complete = lambda: True  # workaround since we don't write any output

    def complete(self):
        return False

    def f(self, x):
        return x


class PowerSum(LinearSum):
    p = luigi.IntParameter()

    def f(self, x):
        return x ** self.p


class CloneTest(unittest.TestCase):

    def test_args(self):
        t = LinearSum(lo=42, hi=45)
        self.assertEqual(t.param_args, (42, 45))
        self.assertEqual(t.param_kwargs, {'lo': 42, 'hi': 45})

    def test_recursion(self):
        t = LinearSum(lo=42, hi=45)
        luigi.build([t], local_scheduler=True)
        self.assertEqual(t.s, 42 + 43 + 44)

    def test_inheritance(self):
        t = PowerSum(lo=42, hi=45, p=2)
        luigi.build([t], local_scheduler=True)
        self.assertEqual(t.s, 42 ** 2 + 43 ** 2 + 44 ** 2)

    def test_inheritance_from_non_parameter(self):
        """
        Cloning can pull non-source-parameters from source to target parameter.
        """

        class SubTask(luigi.Task):
            lo = 1

            @property
            def hi(self):
                return 2

        t1 = SubTask()
        t2 = t1.clone(cls=LinearSum)
        self.assertEqual(t2.lo, 1)
        self.assertEqual(t2.hi, 2)
