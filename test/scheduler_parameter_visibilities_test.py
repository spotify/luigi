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
from luigi.scheduler import OrderedSet, _get_default, Failures
import luigi.worker
from luigi.parameter import ParameterVisibility
import json
import time
import os


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


class OldVersionedTask():
    def __init__(self, task_id, status, deps, resources=None, priority=0, family='', module=None,
                 params=None, accepts_messages=False, tracking_url=None, status_message=None,
                 progress_percentage=None, retry_policy='notoptional'):
        self.id = task_id
        self.stakeholders = set()
        self.workers = OrderedSet()
        if deps is None:
            self.deps = set()
        else:
            self.deps = set(deps)
        self.status = status
        self.time = time.time()
        self.updated = self.time
        self.retry = None
        self.remove = None
        self.worker_running = None
        self.time_running = None
        self.expl = None
        self.priority = priority
        self.resources = _get_default(resources, {})
        self.family = family
        self.module = module
        self.params = _get_default(params, {})
        self.accepts_messages = accepts_messages
        self.retry_policy = retry_policy
        self.tracking_url = tracking_url
        self.status_message = status_message
        self.progress_percentage = progress_percentage
        self.scheduler_message_responses = {}
        self.scheduler_disable_time = None
        self.runnable = False
        self.batchable = False
        self.batch_id = None

    def __repr__(self):
        return "Task(%r)" % vars(self)

    def is_batchable(self):
        try:
            return self.batchable
        except AttributeError:
            return False

    def add_failure(self):
        self.failures.add_failure()

    def has_excessive_failures(self):
        if self.failures.first_failure_time is not None:
            if (time.time() >= self.failures.first_failure_time + self.retry_policy.disable_hard_timeout):
                return True

        if self.failures.num_failures() >= self.retry_policy.retry_count:
            return True

        return False

    @property
    def pretty_id(self):
        param_str = ', '.join(u'{}={}'.format(key, value) for key, value in sorted(self.params.items()))
        return u'{}({})'.format(self.family, param_str)


class OldVersionedTaskTest(LuigiTestCase):
    def test_pickled_old_versioned_task_pretty_id(self):
        s = luigi.scheduler.Scheduler(send_messages=True)
        s._state._tasks['1'] = OldVersionedTask('1', 'PENDING', None, params={'a': 'a', 'b': 'b', 'c': 'c'})
        path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'data')
        s._state._state_path = path
        s.dump()

        def failed_pretty_id(that):
            params = that.public_params
            param_str = ', '.join(u'{}={}'.format(key, value) for key, value in sorted(params.items()))
            return u'{}({})'.format(that.family, param_str)

        OldVersionedTask.pretty_id = lambda x: failed_pretty_id(x)

        d = luigi.scheduler.Scheduler(send_messages=True)
        d._state._state_path = path
        d.load()

        self.assertRaises(Exception, lambda: d._state._tasks['1'].pretty_id())

        def fixed_pretty_id(that):
            params = that.public_params if hasattr(that, 'public_params') else that.params
            param_str = ', '.join(u'{}={}'.format(key, value) for key, value in sorted(params.items()))
            return u'{}({})'.format(that.family, param_str)

        OldVersionedTask.pretty_id = lambda x: fixed_pretty_id(x)

        self.assertEqual(d._state._tasks['1'].pretty_id(), '(a=a, b=b, c=c)')

    def test_pickled_old_versioned_task_serialize(self):
        s = luigi.scheduler.Scheduler(send_messages=True)
        s._state._tasks['1'] = OldVersionedTask('1', 'PENDING', None, params={'a': 'a', 'b': 'b', 'c': 'c'})
        path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'data')
        s._state._state_path = path
        s.dump()

        d = luigi.scheduler.Scheduler(send_messages=True)
        d._state._state_path = path
        d.load()

        from functools import partial
        def old_serialize(self, task_id, include_deps=True, deps=None):
            task = self._state.get_task(task_id)

            ret = {
                'display_name': task.pretty_id,
                'status': task.status,
                'workers': list(task.workers),
                'worker_running': task.worker_running,
                'time_running': getattr(task, "time_running", None),
                'start_time': task.time,
                'last_updated': getattr(task, "updated", task.time),
                'params': task.public_params,
                'name': task.family,
                'priority': task.priority,
                'resources': task.resources,
                'resources_running': getattr(task, "resources_running", None),
                'tracking_url': getattr(task, "tracking_url", None),
                'status_message': getattr(task, "status_message", None),
                'progress_percentage': getattr(task, "progress_percentage", None),
            }
            if task.status == "DISABLED":
                ret['re_enable_able'] = task.scheduler_disable_time is not None
            if include_deps:
                ret['deps'] = list(task.deps if deps is None else deps)
            if self._config.send_messages and task.status == "RUNNING":
                ret['accepts_messages'] = task.accepts_messages
            return ret

        d._serialize_task = (lambda x: partial(old_serialize, x))(d)

        self.assertRaises(Exception, lambda: d._serialize_task(task_id='1'))

        def fixed_serialize(self, task_id, include_deps=True, deps=None):
            task = self._state.get_task(task_id)
            params = task.public_params if hasattr(task, 'public_params') else task.params

            ret = {
                'display_name': task.pretty_id,
                'status': task.status,
                'workers': list(task.workers),
                'worker_running': task.worker_running,
                'time_running': getattr(task, "time_running", None),
                'start_time': task.time,
                'last_updated': getattr(task, "updated", task.time),
                'params': params,
                'name': task.family,
                'priority': task.priority,
                'resources': task.resources,
                'resources_running': getattr(task, "resources_running", None),
                'tracking_url': getattr(task, "tracking_url", None),
                'status_message': getattr(task, "status_message", None),
                'progress_percentage': getattr(task, "progress_percentage", None),
            }
            if task.status == "DISABLED":
                ret['re_enable_able'] = task.scheduler_disable_time is not None
            if include_deps:
                ret['deps'] = list(task.deps if deps is None else deps)
            if self._config.send_messages and task.status == "RUNNING":
                ret['accepts_messages'] = task.accepts_messages
            return ret

        d._serialize_task = (lambda x: partial(fixed_serialize, x))(d)

        self.assertTrue(d._serialize_task(task_id='1'))
