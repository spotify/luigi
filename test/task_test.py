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
import doctest
import pickle
import warnings

from helpers import unittest, LuigiTestCase
from datetime import datetime, timedelta

import luigi
import luigi.task
import luigi.util
import collections
from luigi.task_register import load_task


class DummyTask(luigi.Task):

    param = luigi.Parameter()
    bool_param = luigi.BoolParameter()
    int_param = luigi.IntParameter()
    float_param = luigi.FloatParameter()
    date_param = luigi.DateParameter()
    datehour_param = luigi.DateHourParameter()
    timedelta_param = luigi.TimeDeltaParameter()
    insignificant_param = luigi.Parameter(significant=False)


DUMMY_TASK_OK_PARAMS = dict(
    param='test',
    bool_param=True,
    int_param=666,
    float_param=123.456,
    date_param=datetime(2014, 9, 13).date(),
    datehour_param=datetime(2014, 9, 13, 9),
    timedelta_param=timedelta(44),  # doesn't support seconds
    insignificant_param='test')


class DefaultInsignificantParamTask(luigi.Task):
    insignificant_param = luigi.Parameter(significant=False, default='value')
    necessary_param = luigi.Parameter(significant=False)


class TaskTest(unittest.TestCase):

    def test_tasks_doctest(self):
        doctest.testmod(luigi.task)

    def test_task_to_str_to_task(self):
        original = DummyTask(**DUMMY_TASK_OK_PARAMS)
        other = DummyTask.from_str_params(original.to_str_params())
        self.assertEqual(original, other)

    def test_task_from_str_insignificant(self):
        params = {'necessary_param': 'needed'}
        original = DefaultInsignificantParamTask(**params)
        other = DefaultInsignificantParamTask.from_str_params(params)
        self.assertEqual(original, other)

    def test_task_missing_necessary_param(self):
        with self.assertRaises(luigi.parameter.MissingParameterException):
            DefaultInsignificantParamTask.from_str_params({})

    def test_external_tasks_loadable(self):
        task = load_task("luigi", "ExternalTask", {})
        assert(isinstance(task, luigi.ExternalTask))

    def test_getpaths(self):
        class RequiredTask(luigi.Task):
            def output(self):
                return luigi.LocalTarget("/path/to/target/file")

        t = RequiredTask()
        reqs = {}
        reqs["bare"] = t
        reqs["dict"] = {"key": t}
        reqs["OrderedDict"] = collections.OrderedDict([("key", t)])
        reqs["list"] = [t]
        reqs["tuple"] = (t,)
        reqs["generator"] = (t for _ in range(10))

        struct = luigi.task.getpaths(reqs)
        self.assertIsInstance(struct, dict)
        self.assertIsInstance(struct["bare"], luigi.Target)
        self.assertIsInstance(struct["dict"], dict)
        self.assertIsInstance(struct["OrderedDict"], collections.OrderedDict)
        self.assertIsInstance(struct["list"], list)
        self.assertIsInstance(struct["tuple"], tuple)
        self.assertTrue(hasattr(struct["generator"], "__iter__"))

    def test_flatten(self):
        flatten = luigi.task.flatten
        self.assertEqual(sorted(flatten({'a': 'foo', 'b': 'bar'})), ['bar', 'foo'])
        self.assertEqual(sorted(flatten(['foo', ['bar', 'troll']])), ['bar', 'foo', 'troll'])
        self.assertEqual(flatten('foo'), ['foo'])
        self.assertEqual(flatten(42), [42])
        self.assertEqual(flatten((len(i) for i in ["foo", "troll"])), [3, 5])
        self.assertRaises(TypeError, flatten, (len(i) for i in ["foo", "troll", None]))

    def test_externalized_task_picklable(self):
        task = luigi.task.externalize(luigi.Task())
        pickled_task = pickle.dumps(task)
        self.assertEqual(task, pickle.loads(pickled_task))

    def test_no_unpicklable_properties(self):
        task = luigi.Task()
        task.set_tracking_url = lambda tracking_url: tracking_url
        task.set_status_message = lambda message: message
        with task.no_unpicklable_properties():
            pickle.dumps(task)
        self.assertIsNotNone(task.set_tracking_url)
        self.assertIsNotNone(task.set_status_message)
        tracking_url = task.set_tracking_url('http://test.luigi.com/')
        self.assertEqual(tracking_url, 'http://test.luigi.com/')
        message = task.set_status_message('message')
        self.assertEqual(message, 'message')

    def test_no_warn_if_param_types_ok(self):
        with warnings.catch_warnings(record=True) as w:
            DummyTask(**DUMMY_TASK_OK_PARAMS)
        self.assertEqual(len(w), 0, msg='No warning should be raised when correct parameter types are used')

    def test_warn_on_non_str_param(self):
        params = dict(**DUMMY_TASK_OK_PARAMS)
        params['param'] = 42
        with self.assertWarnsRegex(UserWarning, 'Parameter "param" with value "42" is not of type string.'):
            DummyTask(**params)

    def test_warn_on_non_timedelta_param(self):
        params = dict(**DUMMY_TASK_OK_PARAMS)

        class MockTimedelta:
            days = 1
            seconds = 1

        params['timedelta_param'] = MockTimedelta()
        with self.assertWarnsRegex(UserWarning, 'Parameter "timedelta_param" with value ".*" is not of type timedelta.'):
            DummyTask(**params)


class ExternalizeTaskTest(LuigiTestCase):

    def test_externalize_taskclass(self):
        class MyTask(luigi.Task):
            def run(self):
                pass

        self.assertIsNotNone(MyTask.run)  # Assert what we believe
        task_object = luigi.task.externalize(MyTask)()
        self.assertIsNone(task_object.run)
        self.assertIsNotNone(MyTask.run)  # Check immutability
        self.assertIsNotNone(MyTask().run)  # Check immutability

    def test_externalize_taskobject(self):
        class MyTask(luigi.Task):
            def run(self):
                pass

        task_object = luigi.task.externalize(MyTask())
        self.assertIsNone(task_object.run)
        self.assertIsNotNone(MyTask.run)  # Check immutability
        self.assertIsNotNone(MyTask().run)  # Check immutability

    def test_externalize_taskclass_readable_name(self):
        class MyTask(luigi.Task):
            def run(self):
                pass

        task_class = luigi.task.externalize(MyTask)
        self.assertIsNot(task_class, MyTask)
        self.assertIn("MyTask", task_class.__name__)

    def test_externalize_taskclass_instance_cache(self):
        class MyTask(luigi.Task):
            def run(self):
                pass

        task_class = luigi.task.externalize(MyTask)
        self.assertIsNot(task_class, MyTask)
        self.assertIs(MyTask(), MyTask())  # Assert it have enabled the instance caching
        self.assertIsNot(task_class(), MyTask())  # Now, they should not be the same of course

    def test_externalize_same_id(self):
        class MyTask(luigi.Task):
            def run(self):
                pass

        task_normal = MyTask()
        task_ext_1 = luigi.task.externalize(MyTask)()
        task_ext_2 = luigi.task.externalize(MyTask())
        self.assertEqual(task_normal.task_id, task_ext_1.task_id)
        self.assertEqual(task_normal.task_id, task_ext_2.task_id)

    def test_externalize_same_id_with_task_namespace(self):
        # Dependent on the new behavior from spotify/luigi#1953
        class MyTask(luigi.Task):
            task_namespace = "something.domething"

            def run(self):
                pass

        task_normal = MyTask()
        task_ext_1 = luigi.task.externalize(MyTask())
        task_ext_2 = luigi.task.externalize(MyTask)()
        self.assertEqual(task_normal.task_id, task_ext_1.task_id)
        self.assertEqual(task_normal.task_id, task_ext_2.task_id)
        self.assertEqual(str(task_normal), str(task_ext_1))
        self.assertEqual(str(task_normal), str(task_ext_2))

    def test_externalize_same_id_with_luigi_namespace(self):
        # Dependent on the new behavior from spotify/luigi#1953
        luigi.namespace('lets.externalize')

        class MyTask(luigi.Task):
            def run(self):
                pass
        luigi.namespace()

        task_normal = MyTask()
        task_ext_1 = luigi.task.externalize(MyTask())
        task_ext_2 = luigi.task.externalize(MyTask)()
        self.assertEqual(task_normal.task_id, task_ext_1.task_id)
        self.assertEqual(task_normal.task_id, task_ext_2.task_id)
        self.assertEqual(str(task_normal), str(task_ext_1))
        self.assertEqual(str(task_normal), str(task_ext_2))

    def test_externalize_with_requires(self):
        class MyTask(luigi.Task):
            def run(self):
                pass

        @luigi.util.requires(luigi.task.externalize(MyTask))
        class Requirer(luigi.Task):
            def run(self):
                pass

        self.assertIsNotNone(MyTask.run)  # Check immutability
        self.assertIsNotNone(MyTask().run)  # Check immutability

    def test_externalize_doesnt_affect_the_registry(self):
        class MyTask(luigi.Task):
            pass
        reg_orig = luigi.task_register.Register._get_reg()
        luigi.task.externalize(MyTask)
        reg_afterwards = luigi.task_register.Register._get_reg()
        self.assertEqual(reg_orig, reg_afterwards)

    def test_can_uniquely_command_line_parse(self):
        class MyTask(luigi.Task):
            pass
        # This first check is just an assumption rather than assertion
        self.assertTrue(self.run_locally(['MyTask']))
        luigi.task.externalize(MyTask)
        # Now we check we don't encounter "ambiguous task" issues
        self.assertTrue(self.run_locally(['MyTask']))
        # We do this once again, is there previously was a bug like this.
        luigi.task.externalize(MyTask)
        self.assertTrue(self.run_locally(['MyTask']))


class TaskNamespaceTest(LuigiTestCase):

    def setup_tasks(self):
        class Foo(luigi.Task):
            pass

        class FooSubclass(Foo):
            pass
        return (Foo, FooSubclass, self.go_mynamespace())

    def go_mynamespace(self):
        luigi.namespace("mynamespace")

        class Foo(luigi.Task):
            p = luigi.IntParameter()

        class Bar(Foo):
            task_namespace = "othernamespace"  # namespace override

        class Baz(Bar):  # inherits namespace for Bar
            pass
        luigi.namespace()
        return collections.namedtuple('mynamespace', 'Foo Bar Baz')(Foo, Bar, Baz)

    def test_vanilla(self):
        (Foo, FooSubclass, namespace_test_helper) = self.setup_tasks()
        self.assertEqual(Foo.task_family, "Foo")
        self.assertEqual(str(Foo()), "Foo()")

        self.assertEqual(FooSubclass.task_family, "FooSubclass")
        self.assertEqual(str(FooSubclass()), "FooSubclass()")

    def test_namespace(self):
        (Foo, FooSubclass, namespace_test_helper) = self.setup_tasks()
        self.assertEqual(namespace_test_helper.Foo.task_family, "mynamespace.Foo")
        self.assertEqual(str(namespace_test_helper.Foo(1)), "mynamespace.Foo(p=1)")

        self.assertEqual(namespace_test_helper.Bar.task_namespace, "othernamespace")
        self.assertEqual(namespace_test_helper.Bar.task_family, "othernamespace.Bar")
        self.assertEqual(str(namespace_test_helper.Bar(1)), "othernamespace.Bar(p=1)")

        self.assertEqual(namespace_test_helper.Baz.task_namespace, "othernamespace")
        self.assertEqual(namespace_test_helper.Baz.task_family, "othernamespace.Baz")
        self.assertEqual(str(namespace_test_helper.Baz(1)), "othernamespace.Baz(p=1)")

    def test_uses_latest_namespace(self):
        luigi.namespace('a')

        class _BaseTask(luigi.Task):
            pass
        luigi.namespace('b')

        class _ChildTask(_BaseTask):
            pass
        luigi.namespace()  # Reset everything
        child_task = _ChildTask()
        self.assertEqual(child_task.task_family, 'b._ChildTask')
        self.assertEqual(str(child_task), 'b._ChildTask()')

    def test_with_scope(self):
        luigi.namespace('wohoo', scope='task_test')
        luigi.namespace('bleh', scope='')

        class MyTask(luigi.Task):
            pass
        luigi.namespace(scope='task_test')
        luigi.namespace(scope='')
        self.assertEqual(MyTask.get_task_namespace(), 'wohoo')

    def test_with_scope_not_matching(self):
        luigi.namespace('wohoo', scope='incorrect_namespace')
        luigi.namespace('bleh', scope='')

        class MyTask(luigi.Task):
            pass
        luigi.namespace(scope='incorrect_namespace')
        luigi.namespace(scope='')
        self.assertEqual(MyTask.get_task_namespace(), 'bleh')


class AutoNamespaceTest(LuigiTestCase):
    this_module = 'task_test'

    def test_auto_namespace_global(self):
        luigi.auto_namespace()

        class MyTask(luigi.Task):
            pass

        luigi.namespace()
        self.assertEqual(MyTask.get_task_namespace(), self.this_module)

    def test_auto_namespace_scope(self):
        luigi.auto_namespace(scope='task_test')
        luigi.namespace('bleh', scope='')

        class MyTask(luigi.Task):
            pass
        luigi.namespace(scope='task_test')
        luigi.namespace(scope='')
        self.assertEqual(MyTask.get_task_namespace(), self.this_module)

    def test_auto_namespace_not_matching(self):
        luigi.auto_namespace(scope='incorrect_namespace')
        luigi.namespace('bleh', scope='')

        class MyTask(luigi.Task):
            pass
        luigi.namespace(scope='incorrect_namespace')
        luigi.namespace(scope='')
        self.assertEqual(MyTask.get_task_namespace(), 'bleh')

    def test_auto_namespace_not_matching_2(self):
        luigi.auto_namespace(scope='incorrect_namespace')

        class MyTask(luigi.Task):
            pass
        luigi.namespace(scope='incorrect_namespace')
        self.assertEqual(MyTask.get_task_namespace(), '')
