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


class Factorial(luigi.Task):

    ''' This calculates factorials *online* and does not write its results anywhere

    Demonstrates the ability for dependencies between Tasks and not just between their output.
    '''
    n = luigi.IntParameter(default=100)

    def requires(self):
        if self.n > 1:
            return Factorial(self.n - 1)

    def run(self):
        if self.n > 1:
            self.value = self.n * self.requires().value
        else:
            self.value = 1
        self.complete = lambda: True

    def complete(self):
        return False


class FactorialTest(unittest.TestCase):

    def test_invoke(self):
        luigi.build([Factorial(100)], local_scheduler=True)
        self.assertEqual(Factorial(42).value, 1405006117752879898543142606244511569936384000000000)
