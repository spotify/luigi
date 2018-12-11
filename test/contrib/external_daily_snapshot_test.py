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
from luigi.contrib.external_daily_snapshot import ExternalDailySnapshot
from luigi.mock import MockTarget
import datetime


class DataDump(ExternalDailySnapshot):
    param = luigi.Parameter()
    a = luigi.Parameter(default='zebra')
    aa = luigi.Parameter(default='Congo')

    def output(self):
        return MockTarget('data-%s-%s-%s-%s' % (self.param, self.a, self.aa, self.date))


class ExternalDailySnapshotTest(unittest.TestCase):
    def test_latest(self):
        MockTarget('data-xyz-zebra-Congo-2012-01-01').open('w').close()
        d = DataDump.latest(date=datetime.date(2012, 1, 10), param='xyz')
        self.assertEquals(d.date, datetime.date(2012, 1, 1))

    def test_latest_not_exists(self):
        MockTarget('data-abc-zebra-Congo-2012-01-01').open('w').close()
        d = DataDump.latest(date=datetime.date(2012, 1, 11), param='abc', lookback=5)
        self.assertEquals(d.date, datetime.date(2012, 1, 7))

    def test_deterministic(self):
        MockTarget('data-pqr-zebra-Congo-2012-01-01').open('w').close()
        d = DataDump.latest(date=datetime.date(2012, 1, 10), param='pqr', a='zebra', aa='Congo')
        self.assertEquals(d.date, datetime.date(2012, 1, 1))

        MockTarget('data-pqr-zebra-Congo-2012-01-05').open('w').close()
        d = DataDump.latest(date=datetime.date(2012, 1, 10), param='pqr', aa='Congo', a='zebra')
        self.assertEquals(d.date, datetime.date(2012, 1, 1))  # Should still be the same
