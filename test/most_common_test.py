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

from helpers import unittest

from luigi.tools.range import most_common


class MostCommonTest(unittest.TestCase):

    def setUp(self):
        self.runs = [
            ([1], (1, 1)),
            ([1, 1], (1, 2)),
            ([1, 1, 2], (1, 2)),
            ([1, 1, 2, 2, 2], (2, 3))
        ]

    def test_runs(self):
        for args, result in self.runs:
            actual = most_common(args)
            expected = result
            self.assertEqual(expected, actual)
