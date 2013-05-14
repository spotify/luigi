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

from luigi.mock import MockFile
import luigi, luigi.hdfs, luigi.hadoop
import unittest


class MockFileTest(unittest.TestCase):
    def test_1(self):
        t = MockFile('test')
        p = t.open('w')
        print >> p, 'test'
        p.close()

        q = t.open('r')
        self.assertEqual(list(q), ['test\n'])
        q.close()


class X(luigi.Task):
    x = luigi.Parameter()
    def output(self):
        return luigi.hdfs.HdfsTarget('blah/%s' % self.x)

    def run(self):
        f = self.output().open('w')
        for i in xrange(1000):
            print >>f, i % 73
        f.close()


class Mapred(luigi.hadoop.JobTask):
    x = luigi.Parameter()
    def requires(self):
        return X(self.x)

    def mapper(self, line):
        yield line.strip(), 1

    def reducer(self, key, values):
        yield key, sum(values)

    def output(self):
        return luigi.hdfs.HdfsTarget('mapred/%s' % self.x)


class External(luigi.ExternalTask):
    def output(self):
        return luigi.hdfs.HdfsTarget('e')


class Recursive(luigi.Task):
    x = luigi.IntParameter()
    def requires(self):
        if self.x > 1:
            yield Recursive(self.x - 1)
        else:
            yield External()

    def output(self):
        return luigi.hdfs.HdfsTarget('y/%d' % self.x)

    def run(self):
        self.output().open('w').close()

class MockChainTest(unittest.TestCase):
    def test_mapred(self):
        m = Mapred(42)
        luigi.build([m], mock=True, local_scheduler=True)
        self.assertTrue(m.complete())

    def test_recursive(self):
        r = Recursive(100)
        luigi.build([r], mock=True, local_scheduler=True)
        self.assertFalse(r.complete()) # because Recursive(0) can't be built

        MockFile('e').open('w').close()
        luigi.build([r], mock=True, local_scheduler=True)
        self.assertTrue(r.complete())
        
