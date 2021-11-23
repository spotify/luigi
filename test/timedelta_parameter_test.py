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
from helpers import unittest

import luigi
import luigi.interface


FLOAT_STRING = '788640.0'
LONG_STRING = '1 week 2 days 3 h 4 m'
ISO8601 = 'P1W2DT3H4M'
SERIALIZED = '1 w 2 d 3 h 4 m 0 s'
TIMEDELTA = datetime.timedelta(weeks=1, days=2, hours=3, minutes=4)


class TimeDeltaParameterTest(unittest.TestCase):
    def test_parse_float(self):
        td = luigi.TimeDeltaParameter().parse(FLOAT_STRING)
        self.assertEqual(td, TIMEDELTA)

    def test_parse_long(self):
        td = luigi.TimeDeltaParameter().parse(LONG_STRING)
        self.assertEqual(td, TIMEDELTA)

    def test_parse_iso(self):
        td = luigi.TimeDeltaParameter().parse(ISO8601)
        self.assertEqual(td, TIMEDELTA)

    def test_serialize(self):
        td = luigi.TimeDeltaParameter().serialize(TIMEDELTA)
        self.assertEqual(td, SERIALIZED)
