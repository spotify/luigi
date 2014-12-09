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

import unittest
import luigi
import luigi.util
import luigi.notifications
luigi.notifications.DEBUG = True


class A(luigi.Task):
    x = luigi.IntParameter(default=3)


class B(luigi.util.Derived(A)):
    y = luigi.IntParameter(default=4)


class A2(luigi.Task):
    x = luigi.IntParameter(default=3)
    g = luigi.IntParameter(is_global=True, default=42)


class B2(luigi.util.Derived(A2)):
    pass


class UtilTest(unittest.TestCase):
    def test_derived_extended(self):
        b = B(1, 2)
        self.assertEqual(b.x, 1)
        self.assertEqual(b.y, 2)
        a = A(1)
        self.assertEqual(b.parent_obj, a)

    def test_derived_extended_default(self):
        b = B()
        self.assertEqual(b.x, 3)
        self.assertEqual(b.y, 4)

    def test_derived_global_param(self):
        # Had a bug with this
        b = B2()
        self.assertEqual(b.g, 42)
