# Copyright (c) 2013 Spotify AB
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
import luigi.util
import luigi.notifications
from luigi.contrib.latest import latest
from luigi.mock import MockFile
import datetime

luigi.notifications.DEBUG = True

@latest(default_lookback=10)
class DataDump(luigi.Task):
    date = luigi.DateParameter()
    def output(self):
        return MockFile(self.date.strftime('foo/bar/%Y-%m-%d.txt'))

class LatestTest(unittest.TestCase):
    def test_latest(self):
        dd = DataDump(datetime.date(2000, 1, 1))
        dd.output().open('w').close()

        dd_5 = DataDump.latest(date=datetime.date(2000, 1, 5))
        self.assertEquals(dd_5.date, datetime.date(2000, 1, 1))

        with self.assertRaises(Exception):
            # Lookback is too low to find it
            DataDump.latest(date=datetime.date(2000, 1, 15))

        # Now, create a new dump
        dd_3 = DataDump(datetime.date(2000, 1, 3))
        dd_3.output().open('w').close()

        # latest() is cached so it should still return the same value
        dd_5 = DataDump.latest(date=datetime.date(2000, 1, 5))
        self.assertEquals(dd_5.date, datetime.date(2000, 1, 1))

    def test_name(self):
        self.assertEquals(DataDump.__name__, 'DataDump')
