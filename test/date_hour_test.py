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


class DateHourTask(luigi.Task):
    dh = luigi.DateHourParameter()


class DateHourTest(unittest.TestCase):

    def test_parse(self):
        dh = luigi.DateHourParameter().parse('2013-01-01T18')
        self.assertEqual(dh, datetime.datetime(2013, 1, 1, 18, 0, 0))

    def test_serialize(self):
        dh = luigi.DateHourParameter().serialize(datetime.datetime(2013, 1, 1, 18, 0, 0))
        self.assertEqual(dh, '2013-01-01T18')

    def test_parse_interface(self):
        task = luigi.interface.ArgParseInterface().parse(["DateHourTask", "--dh", "2013-01-01T18"])[0]
        self.assertEqual(task.dh, datetime.datetime(2013, 1, 1, 18, 0, 0))

    def test_serialize_task(self):
        t = DateHourTask(datetime.datetime(2013, 1, 1, 18, 0, 0))
        self.assertEqual(str(t), 'DateHourTask(dh=2013-01-01T18)')
