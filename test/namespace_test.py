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

import luigi
import namespace_test_helper  # declares another Foo in namespace mynamespace


class Foo(luigi.Task):
    pass


class FooSubclass(Foo):
    pass


class TestNamespacing(unittest.TestCase):

    def test_vanilla(self):
        self.assertEqual(Foo.task_namespace, None)
        self.assertEqual(Foo.task_family, "Foo")
        self.assertEqual(str(Foo()), "Foo()")

        self.assertEqual(FooSubclass.task_namespace, None)
        self.assertEqual(FooSubclass.task_family, "FooSubclass")
        self.assertEqual(str(FooSubclass()), "FooSubclass()")

    def test_namespace(self):
        self.assertEqual(namespace_test_helper.Foo.task_namespace, "mynamespace")
        self.assertEqual(namespace_test_helper.Foo.task_family, "mynamespace.Foo")
        self.assertEqual(str(namespace_test_helper.Foo(1)), "mynamespace.Foo(p=1)")

        self.assertEqual(namespace_test_helper.Bar.task_namespace, "othernamespace")
        self.assertEqual(namespace_test_helper.Bar.task_family, "othernamespace.Bar")
        self.assertEqual(str(namespace_test_helper.Bar(1)), "othernamespace.Bar(p=1)")
