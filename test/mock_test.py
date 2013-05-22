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

    def test_with(self):
        t = MockFile("foo")
        with t.open('w') as b:
            b.write("bar")

        with t.open('r') as b:
            self.assertEquals(list(b), ['bar'])
