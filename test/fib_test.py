# Copyright (c) 2012 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import luigi
import luigi.interface
import unittest
from luigi.mock import MockFile

File = MockFile

# Calculates Fibonacci numbers :)


class Fib(luigi.Task):
    n = luigi.IntParameter(default=100)

    def requires(self):
        if self.n >= 2:
            return [Fib(self.n - 1), Fib(self.n - 2)]
        else:
            return []

    def output(self):
        return File('/tmp/fib_%d' % self.n)

    def run(self):
        if self.n == 0:
            s = 0
        elif self.n == 1:
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
        global File
        File = MockFile
        MockFile._file_contents.clear()


class FibTest(FibTestBase):
    def test_invoke(self):
        w = luigi.worker.Worker()
        w.add(Fib(100))
        w.run()
        w.stop()
        self.assertEqual(MockFile._file_contents['/tmp/fib_10'], '55\n')
        self.assertEqual(MockFile._file_contents['/tmp/fib_100'], '354224848179261915075\n')

    def test_cmdline(self):
        luigi.run(['--local-scheduler', 'Fib', '--n', '100'])

        self.assertEqual(MockFile._file_contents['/tmp/fib_10'], '55\n')
        self.assertEqual(MockFile._file_contents['/tmp/fib_100'], '354224848179261915075\n')

    def test_build_internal(self):
        luigi.build([Fib(100)], local_scheduler=True)

        self.assertEqual(MockFile._file_contents['/tmp/fib_10'], '55\n')
        self.assertEqual(MockFile._file_contents['/tmp/fib_100'], '354224848179261915075\n')

if __name__ == '__main__':
    luigi.run()
