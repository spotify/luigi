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


class DateMinuteTask(luigi.Task):
    dh = luigi.DateMinuteParameter()


class DateMinuteTest(unittest.TestCase):

    def test_parse(self):
        dh = luigi.DateMinuteParameter().parse('2013-01-01T18H42')
        self.assertEqual(dh, datetime.datetime(2013, 1, 1, 18, 42, 0))

    def test_parse_padding_zero(self):
        dh = luigi.DateMinuteParameter().parse('2013-01-01T18H07')
        self.assertEqual(dh, datetime.datetime(2013, 1, 1, 18, 7, 0))

    def test_serialize(self):
        dh = luigi.DateMinuteParameter().serialize(datetime.datetime(2013, 1, 1, 18, 42, 0))
        self.assertEqual(dh, '2013-01-01T18H42')

    def test_serialize_padding_zero(self):
        dh = luigi.DateMinuteParameter().serialize(datetime.datetime(2013, 1, 1, 18, 7, 0))
        self.assertEqual(dh, '2013-01-01T18H07')

    def test_parse_interface(self):
        task = luigi.interface.ArgParseInterface().parse(["DateMinuteTask", "--dh", "2013-01-01T18H42"])[0]
        self.assertEqual(task.dh, datetime.datetime(2013, 1, 1, 18, 42, 0))

    def test_serialize_task(self):
        t = DateMinuteTask(datetime.datetime(2013, 1, 1, 18, 42, 0))
        self.assertEqual(str(t), 'DateMinuteTask(dh=2013-01-01T18H42)')
