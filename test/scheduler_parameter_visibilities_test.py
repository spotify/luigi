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

from helpers import LuigiTestCase, RunOnceTask
import server_test

import luigi
import luigi.scheduler
import luigi.worker
from luigi.parameter import ParameterVisibility
import json
import time


class SchedulerParameterVisibilitiesTest(LuigiTestCase):
    def test_task_with_deps(self):
        s = luigi.scheduler.Scheduler(send_messages=True)
        with luigi.worker.Worker(scheduler=s) as w:
            class DynamicTask(RunOnceTask):
                dynamic_public = luigi.Parameter(default="dynamic_public")
                dynamic_hidden = luigi.Parameter(default="dynamic_hidden", visibility=ParameterVisibility.HIDDEN)
                dynamic_private = luigi.Parameter(default="dynamic_private", visibility=ParameterVisibility.PRIVATE)

            class RequiredTask(RunOnceTask):
                required_public = luigi.Parameter(default="required_param")
                required_hidden = luigi.Parameter(default="required_hidden", visibility=ParameterVisibility.HIDDEN)
                required_private = luigi.Parameter(default="required_private", visibility=ParameterVisibility.PRIVATE)

            class Task(RunOnceTask):
                a = luigi.Parameter(default="a")
                b = luigi.Parameter(default="b", visibility=ParameterVisibility.HIDDEN)
                c = luigi.Parameter(default="c", visibility=ParameterVisibility.PRIVATE)
                d = luigi.Parameter(default="d", visibility=ParameterVisibility.PUBLIC)

                def requires(self):
                    return required_task

                def run(self):
                    yield dynamic_task

            dynamic_task = DynamicTask()
            required_task = RequiredTask()
            task = Task()

            w.add(task)
            w.run()

            time.sleep(1)
            task_deps = s.dep_graph(task_id=task.task_id)
            required_task_deps = s.dep_graph(task_id=required_task.task_id)
            dynamic_task_deps = s.dep_graph(task_id=dynamic_task.task_id)

            self.assertEqual('Task(a=a, d=d)', task_deps[task.task_id]['display_name'])
            self.assertEqual('RequiredTask(required_public=required_param)',
                             required_task_deps[required_task.task_id]['display_name'])
            self.assertEqual('DynamicTask(dynamic_public=dynamic_public)',
                             dynamic_task_deps[dynamic_task.task_id]['display_name'])

            self.assertEqual({'a': 'a', 'd': 'd'}, task_deps[task.task_id]['params'])
            self.assertEqual({'required_public': 'required_param'},
                             required_task_deps[required_task.task_id]['params'])
            self.assertEqual({'dynamic_public': 'dynamic_public'},
                             dynamic_task_deps[dynamic_task.task_id]['params'])

    def test_public_and_hidden_params(self):
        s = luigi.scheduler.Scheduler(send_messages=True)
        with luigi.worker.Worker(scheduler=s) as w:
            class Task(RunOnceTask):
                a = luigi.Parameter(default="a")
                b = luigi.Parameter(default="b", visibility=ParameterVisibility.HIDDEN)
                c = luigi.Parameter(default="c", visibility=ParameterVisibility.PRIVATE)
                d = luigi.Parameter(default="d", visibility=ParameterVisibility.PUBLIC)

            task = Task()

            w.add(task)
            w.run()

            time.sleep(1)
            t = s._state.get_task(task.task_id)
            self.assertEqual({'b': 'b'}, t.hidden_params)
            self.assertEqual({'a': 'a', 'd': 'd'}, t.public_params)
            self.assertEqual({'a': 0, 'b': 1, 'd': 0}, t.param_visibilities)


class Task(RunOnceTask):
    a = luigi.Parameter(default="a")
    b = luigi.Parameter(default="b", visibility=ParameterVisibility.HIDDEN)
    c = luigi.Parameter(default="c", visibility=ParameterVisibility.PRIVATE)
    d = luigi.Parameter(default="d", visibility=ParameterVisibility.PUBLIC)


class RemoteSchedulerParameterVisibilitiesTest(server_test.ServerTestBase):
    def test_public_params(self):
        task = Task()
        luigi.build(tasks=[task], workers=2, scheduler_port=self.get_http_port())

        time.sleep(1)

        response = self.fetch('/api/graph')

        body = response.body
        decoded = body.decode('utf8').replace("'", '"')
        data = json.loads(decoded)

        self.assertEqual({'a': 'a', 'd': 'd'}, data['response'][task.task_id]['params'])
