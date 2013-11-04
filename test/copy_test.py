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
from luigi.mock import MockFile
import unittest
import datetime
from luigi.util import Copy

# TODO: this is deprecated, replaced by luigi.util.copies decorator instead
# See unit tests in decorator_test.py

File = MockFile

class A(luigi.Task):
    date = luigi.DateParameter()

    def output(self):
        return File(self.date.strftime('/tmp/data-%Y-%m-%d.txt'))

    def run(self):
        f = self.output().open('w')
        print >>f, 'hello, world'
        f.close()

class ACopy(Copy(A)):
    def output(self):
        return File(self.date.strftime('/tmp/copy-data-%Y-%m-%d.txt'))

class UtilTest(unittest.TestCase):
    def test_a(self):
        luigi.build([ACopy(date=datetime.date(2012, 1, 1))], local_scheduler=True)
        self.assertEqual(MockFile._file_contents['/tmp/data-2012-01-01.txt'], 'hello, world\n')

if __name__ == '__main__':
    luigi.run()
