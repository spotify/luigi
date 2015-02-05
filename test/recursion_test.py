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

import datetime
import sys
import unittest

import luigi
import luigi.interface
from luigi.mock import MockFile

File = MockFile


class Popularity(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(1))

    def output(self):
        return File('/tmp/popularity/%s.txt' % self.date.strftime('%Y-%m-%d'))

    def requires(self):
        return Popularity(self.date - datetime.timedelta(1))

    def run(self):
        f = self.output().open('w')
        for line in self.input().open('r'):
            print >> f, int(line.strip()) + 1

        f.close()


class RecursionTest(unittest.TestCase):

    def setUp(self):
        MockFile.fs.get_all_data()['/tmp/popularity/2009-01-01.txt'] = '0\n'

    def test_invoke(self):
        w = luigi.worker.Worker()
        w.add(Popularity(datetime.date(2010, 1, 1)))
        w.run()
        w.stop()

        self.assertEqual(MockFile.fs.get_data('/tmp/popularity/2010-01-01.txt'), '365\n')
