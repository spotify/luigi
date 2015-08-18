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
import mock

from helpers import with_config
from luigi import notifications
from luigi.scheduler import CentralPlannerScheduler
from luigi.worker import Worker
from luigi import six
import luigi


class TestEmail(unittest.TestCase):

    def testEmailNoPrefix(self):
        self.assertEqual("subject", notifications._prefix('subject'))

    @with_config({"core": {"email-prefix": "[prefix]"}})
    def testEmailPrefix(self):
        self.assertEqual("[prefix] subject", notifications._prefix('subject'))


class TestException(Exception):
    pass


class TestTask(luigi.Task):
    foo = luigi.Parameter()
    bar = luigi.Parameter()


class FailSchedulingTask(TestTask):
    def requires(self):
        raise TestException('Oops!')

    def run(self):
        pass

    def complete(self):
        return False


class FailRunTask(TestTask):
    def run(self):
        raise TestException('Oops!')

    def complete(self):
        return False


class ExceptionFormatTest(unittest.TestCase):

    def setUp(self):
        self.sch = CentralPlannerScheduler()
        self.w = Worker(scheduler=self.sch)

    def tear_down(self):
        self.w.stop()

    def test_fail_run(self):
        task = FailRunTask(foo='foo', bar='bar')
        self._run_task(task)

    def test_fail_run_html(self):
        task = FailRunTask(foo='foo', bar='bar')
        self._run_task_html(task)

    def test_fail_schedule(self):
        task = FailSchedulingTask(foo='foo', bar='bar')
        self._run_task(task)

    def test_fail_schedule_html(self):
        task = FailSchedulingTask(foo='foo', bar='bar')
        self._run_task_html(task)

    @with_config({'core': {'error-email': 'nowhere@example.com',
                           'email-prefix': '[TEST] '}})
    @mock.patch('luigi.notifications.send_error_email')
    def _run_task(self, task, mock_send):
        self.w.add(task)
        self.w.run()

        self.assertEqual(mock_send.call_count, 1)
        args, kwargs = mock_send.call_args
        self._check_subject(args[0], task)
        self._check_body(args[1], task, html=False)

    @with_config({'core': {'error-email': 'nowhere@axample.com',
                           'email-prefix': '[TEST] ',
                           'email-type': 'html'}})
    @mock.patch('luigi.notifications.send_error_email')
    def _run_task_html(self, task, mock_send):
        self.w.add(task)
        self.w.run()

        self.assertEqual(mock_send.call_count, 1)
        args, kwargs = mock_send.call_args
        self._check_subject(args[0], task)
        self._check_body(args[1], task, html=True)

    def _check_subject(self, subject, task):
        self.assertIn(task.task_id, subject)

    def _check_body(self, body, task, html=False):
        if html:
            self.assertIn('<th>name</th><td>{}</td>'.format(task.task_family), body)
            self.assertIn('<div class="highlight"', body)
            self.assertIn('Oops!', body)

            for param, value in task.param_kwargs.items():
                self.assertIn('<th>{}</th><td>{}</td>'.format(param, value), body)
        else:
            self.assertIn('Name: {}\n'.format(task.task_family), body)
            self.assertIn('Parameters:\n', body)
            self.assertIn('TestException: Oops!', body)

            for param, value in task.param_kwargs.items():
                self.assertIn('{}: {}\n'.format(param, value), body)

    @with_config({"core": {"error-email": "a@a.a"}})
    def testEmailRecipients(self):
        six.assertCountEqual(self, notifications._email_recipients(), ["a@a.a"])
        six.assertCountEqual(self, notifications._email_recipients("b@b.b"), ["a@a.a", "b@b.b"])
        six.assertCountEqual(self, notifications._email_recipients(["b@b.b", "c@c.c"]),
                             ["a@a.a", "b@b.b", "c@c.c"])

    @with_config({"core": {}}, replace_sections=True)
    def testEmailRecipientsNoConfig(self):
        six.assertCountEqual(self, notifications._email_recipients(), [])
        six.assertCountEqual(self, notifications._email_recipients("a@a.a"), ["a@a.a"])
        six.assertCountEqual(self, notifications._email_recipients(["a@a.a", "b@b.b"]),
                             ["a@a.a", "b@b.b"])
