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
import luigi.interface
from luigi.mock import MockTarget

# Calculates Fibonacci numbers :)


class Fib(luigi.Task):
    N = luigi.IntParameter(default=100)

    def requires(self):
        if self.N >= 2:
            return [Fib(self.N - 1), Fib(self.N - 2)]
        else:
            return []

    def output(self):
        return MockTarget('/tmp/fib_%d' % self.N)

    def run(self):
        if self.N == 0:
            s = 0
        elif self.N == 1:
            s = 1
        else:
            s = 0
            for input in self.input():
                for line in input.open('r'):
                    s += int(line.strip())

        f = self.output().open('w')
        f.write('%d\n' % s)
        f.close()


class FibTestBase(unittest.TestCase):

    def setUp(self):
        MockTarget.fs.clear()


class FibTest(FibTestBase):

    def test_invoke(self):
        luigi.build([Fib(100)], local_scheduler=True)
        self.assertEqual(MockTarget.fs.get_data('/tmp/fib_10'), b'55\n')
        self.assertEqual(MockTarget.fs.get_data('/tmp/fib_100'), b'354224848179261915075\n')

    def test_cmdline(self):
        luigi.run(['--local-scheduler', '--no-lock', 'Fib', '--N', '100'])

        self.assertEqual(MockTarget.fs.get_data('/tmp/fib_10'), b'55\n')
        self.assertEqual(MockTarget.fs.get_data('/tmp/fib_100'), b'354224848179261915075\n')

    def test_build_internal(self):
        luigi.build([Fib(100)], local_scheduler=True)

        self.assertEqual(MockTarget.fs.get_data('/tmp/fib_10'), b'55\n')
        self.assertEqual(MockTarget.fs.get_data('/tmp/fib_100'), b'354224848179261915075\n')

if __name__ == '__main__':
    luigi.run()
