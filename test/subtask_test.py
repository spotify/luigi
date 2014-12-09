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

# This test is more of an example of how to do composition to build up "interface" tasks.
# An example is a task that connects to a database and exposes a feature
# Due to constraints you can't use the normal dependency resolution to do this, because
# you might end up running tasks in different processes

import abc
import luigi
import unittest
import random, tempfile, os
from luigi.util import CompositionTask


class F(luigi.Task):
    k = luigi.IntParameter()

    def f(self, x):
        return x ** self.k


class SubtaskTask(CompositionTask):
    def subtasks(self):
        return [F(1), F(2)]

    def run(self):
        self.run_subtasks()

        for t in self.subtasks():
            t.f(42)


class SubtaskTest(unittest.TestCase):
    def test_multiple_workers(self):
        luigi.build([SubtaskTask()], local_scheduler=True)


class AbstractTask(luigi.Task):
    k = luigi.IntParameter()

    @abc.abstractproperty
    def foo(self):
        raise NotImplementedError

    @abc.abstractmethod
    def helper_function(self):
        raise NotImplementedError

    def run(self):
        return ",".join([self.foo, self.helper_function()])


class Implementation(AbstractTask):
    @property
    def foo(self):
        return "bar"

    def helper_function(self):
        return "hello" * self.k


class AbstractSubclassTest(unittest.TestCase):
    def test_instantiate_abstract(self):
        def try_instantiate():
            AbstractTask(k=1)

        self.assertRaises(TypeError, try_instantiate)

    def test_instantiate(self):
        self.assertEqual("bar,hellohello", Implementation(k=2).run())

if __name__ == '__main__':
    luigi.run()

