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
from helpers import unittest
from datetime import datetime, timedelta

import luigi
import luigi.task


class DummyTask(luigi.Task):

    param = luigi.Parameter()
    bool_param = luigi.BoolParameter()
    int_param = luigi.IntParameter()
    float_param = luigi.FloatParameter()
    date_param = luigi.DateParameter()
    datehour_param = luigi.DateHourParameter()
    timedelta_param = luigi.TimeDeltaParameter()
    list_param = luigi.Parameter(is_list=True)


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
            list_param=['in', 'flames'])

        original = DummyTask(**params)
        other = DummyTask.from_str_params(original.to_str_params())
        self.assertEqual(original, other)

    def test_id_to_name_and_params(self):
        task_id = "InputText(date=2014-12-29)"
        (name, params) = luigi.task.id_to_name_and_params(task_id)
        self.assertEquals(name, "InputText")
        self.assertEquals(params, dict(date="2014-12-29"))

    def test_id_to_name_and_params_multiple_args(self):
        task_id = "InputText(date=2014-12-29,foo=bar)"
        (name, params) = luigi.task.id_to_name_and_params(task_id)
        self.assertEquals(name, "InputText")
        self.assertEquals(params, dict(date="2014-12-29", foo="bar"))

    def test_id_to_name_and_params_list_args(self):
        task_id = "InputText(date=2014-12-29,foo=[bar,baz-foo])"
        (name, params) = luigi.task.id_to_name_and_params(task_id)
        self.assertEquals(name, "InputText")
        self.assertEquals(params, dict(date="2014-12-29", foo=["bar", "baz-foo"]))

if __name__ == '__main__':
    unittest.main()
