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

from helpers import unittest
from datetime import datetime, timedelta

import luigi
import luigi.task
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


class DefaultInsignificantParamTask(luigi.Task):
    insignificant_param = luigi.Parameter(significant=False, default='value')
    necessary_param = luigi.Parameter(significant=False)


class DummyListTarget(luigi.Target, list):
    pass


class TaskTest(unittest.TestCase):

    def test_tasks_doctest(self):
        doctest.testmod(luigi.task)

    def test_task_to_str_to_task(self):
        params = dict(
            param='test',
            bool_param=True,
            int_param=666,
            float_param=123.456,
            date_param=datetime(2014, 9, 13).date(),
            datehour_param=datetime(2014, 9, 13, 9),
            timedelta_param=timedelta(44),  # doesn't support seconds
            insignificant_param='test')

        original = DummyTask(**params)
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

    def test_flatten(self):
        flatten = luigi.task.flatten
        self.assertEqual(sorted(flatten({'a': 'foo', 'b': 'bar'})), ['bar', 'foo'])
        self.assertEqual(sorted(flatten(['foo', ['bar', 'troll']])), ['bar', 'foo', 'troll'])
        self.assertEqual(flatten('foo'), ['foo'])
        self.assertEqual(flatten(42), [42])
        self.assertEqual(flatten((len(i) for i in ["foo", "troll"])), [3, 5])

        dummy_list_target = DummyListTarget()
        self.assertEqual(flatten(dummy_list_target), [dummy_list_target])

        self.assertRaises(TypeError, flatten, (len(i) for i in ["foo", "troll", None]))

    def test_externalized_task_picklable(self):
        task = luigi.task.externalize(luigi.Task())
        pickled_task = pickle.dumps(task)
        self.assertEqual(task, pickle.loads(pickled_task))


if __name__ == '__main__':
    unittest.main()
