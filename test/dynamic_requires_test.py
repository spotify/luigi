# Copyright (c) 2014 Spotify AB
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

import unittest
import luigi
from luigi.mock import MockFile
import server_test

class Power(luigi.Task):
    i = luigi.IntParameter()
    p = luigi.IntParameter()
    def run(self):
        f = self.output().open('w')
        print >>f, self.i ** self.p
        f.close()

    def output(self):
        return MockFile('/foo/bar/baz/%d-%d' % (self.i, self.p))


class PowerSum(luigi.Task):
    i = luigi.IntParameter()
    p = luigi.IntParameter()

    def run(self):
        s = 0
        for i in xrange(self.i):
            x = yield Power(i, self.p)
            s += int(x.open('r').readline().strip())

        f = self.output().open('w')
        print >>f, s
        f.close()

    def output(self):
        return MockFile('/foo/bar/baz/sum-%d-%d' % (self.i, self.p))

class DynamicRequiresTest(unittest.TestCase):
    def test_run(self):
        ps = PowerSum(10, 2)
        luigi.build([ps], local_scheduler=True)
        s = int(ps.output().open('r').readline().strip())
        self.assertEquals(s, 285)

class RemoteDynamicRequiresTest(server_test.ServerTestBase):
    def test_run_single_worker(self):
        ps = PowerSum(10, 3)
        luigi.build([ps], scheduler_host='localhost', scheduler_port=self._api_port, workers=1)
        s = int(ps.output().open('r').readline().strip())
        self.assertEquals(s, 2025)

    def test_run_multiple_workers(self):
        ps = PowerSum(10, 4)
        luigi.build([ps], scheduler_host='localhost', scheduler_port=self._api_port, workers=10)
        s = int(ps.output().open('r').readline().strip())
        self.assertEquals(s, 15333)
