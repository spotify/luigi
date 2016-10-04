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
import sys
import socket

from helpers import with_config
from luigi import notifications
from luigi.scheduler import Scheduler
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
        self.sch = Scheduler()

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
        with Worker(scheduler=self.sch) as w:
            w.add(task)
            w.run()

        self.assertEqual(mock_send.call_count, 1)
        args, kwargs = mock_send.call_args
        self._check_subject(args[0], task)
        self._check_body(args[1], task, html=False)

    @with_config({'core': {'error-email': 'nowhere@axample.com',
                           'email-prefix': '[TEST] ',
                           'email-type': 'html'}})
    @mock.patch('luigi.notifications.send_error_email')
    def _run_task_html(self, task, mock_send):
        with Worker(scheduler=self.sch) as w:
            w.add(task)
            w.run()

        self.assertEqual(mock_send.call_count, 1)
        args, kwargs = mock_send.call_args
        self._check_subject(args[0], task)
        self._check_body(args[1], task, html=True)

    def _check_subject(self, subject, task):
        self.assertIn(str(task), subject)

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


class NotificationFixture(object):
    """
    Defines API and message fixture.

    config, sender, subject, message, recipients, image_png
    """
    sender = 'luigi@unittest'
    subject = 'Oops!'
    message = """A multiline
                 message."""
    recipients = ['noone@nowhere.no', 'phantom@opera.fr']
    image_png = None

    notification_args = [sender, subject, message, recipients, image_png]
    mocked_email_msg = '''Content-Type: multipart/related; boundary="===============0998157881=="
MIME-Version: 1.0
Subject: Oops!
From: luigi@unittest
To: noone@nowhere.no,phantom@opera.fr

--===============0998157881==
MIME-Version: 1.0
Content-Transfer-Encoding: 7bit
Content-Type: text/plain; charset="utf-8"

A multiline
message.
--===============0998157881==--'''


class TestSMTPEmail(unittest.TestCase, NotificationFixture):
    """
    Tests sending SMTP email.
    """

    def setUp(self):
        sys.modules['smtplib'] = mock.MagicMock()
        import smtplib  # noqa: F401

    def tearDown(self):
        del sys.modules['smtplib']

    @with_config({"core": {"smtp_ssl": "False",
                           "smtp_host": "my.smtp.local",
                           "smtp_port": "999",
                           "smtp_local_hostname": "ptms",
                           "smtp_timeout": "1200",
                           "smtp_login": "Robin",
                           "smtp_password": "dooH",
                           "smtp_without_tls": "False"}})
    def test_sends_smtp_email(self):
        """
        Call notificaions.send_email_smtp with fixture parameters with smtp_without_tls  set to False
        and check that sendmail is properly called.
        """

        smtp_kws = {"host": "my.smtp.local",
                    "port": 999,
                    "local_hostname": "ptms",
                    "timeout": 1200}

        with mock.patch('smtplib.SMTP') as SMTP:
            with mock.patch('luigi.notifications.generate_email') as generate_email:
                generate_email.return_value\
                    .as_string.return_value = self.mocked_email_msg

                notifications.send_email_smtp(*self.notification_args)

                SMTP.assert_called_once_with(**smtp_kws)
                SMTP.return_value.login.assert_called_once_with("Robin", "dooH")
                SMTP.return_value.starttls.assert_called_once_with()
                SMTP.return_value.sendmail\
                    .assert_called_once_with(self.sender, self.recipients,
                                             self.mocked_email_msg)

    @with_config({"core": {"smtp_ssl": "False",
                           "smtp_host": "my.smtp.local",
                           "smtp_port": "999",
                           "smtp_local_hostname": "ptms",
                           "smtp_timeout": "1200",
                           "smtp_login": "Robin",
                           "smtp_password": "dooH",
                           "smtp_without_tls": "True"}})
    def test_sends_smtp_email_without_tls(self):
        """
        Call notificaions.send_email_smtp with fixture parameters with smtp_without_tls  set to True
        and check that sendmail is properly called without also calling
        starttls.
        """
        smtp_kws = {"host": "my.smtp.local",
                    "port": 999,
                    "local_hostname": "ptms",
                    "timeout": 1200}

        with mock.patch('smtplib.SMTP') as SMTP:
            with mock.patch('luigi.notifications.generate_email') as generate_email:
                generate_email.return_value \
                    .as_string.return_value = self.mocked_email_msg

                notifications.send_email_smtp(*self.notification_args)

                SMTP.assert_called_once_with(**smtp_kws)
                self.assertEqual(SMTP.return_value.starttls.called, False)
                SMTP.return_value.login.assert_called_once_with("Robin", "dooH")
                SMTP.return_value.sendmail \
                    .assert_called_once_with(self.sender, self.recipients,
                                             self.mocked_email_msg)

    @with_config({"core": {"smtp_ssl": "False",
                           "smtp_host": "my.smtp.local",
                           "smtp_port": "999",
                           "smtp_local_hostname": "ptms",
                           "smtp_timeout": "1200",
                           "smtp_login": "Robin",
                           "smtp_password": "dooH",
                           "smtp_without_tls": "True"}})
    def test_sends_smtp_email_exceptions(self):
        """
        Call notificaions.send_email_smtp when it cannot connect to smtp server (socket.error)
        starttls.
        """
        smtp_kws = {"host": "my.smtp.local",
                    "port": 999,
                    "local_hostname": "ptms",
                    "timeout": 1200}

        with mock.patch('smtplib.SMTP') as SMTP:
            with mock.patch('luigi.notifications.generate_email') as generate_email:
                SMTP.side_effect = socket.error()
                generate_email.return_value \
                    .as_string.return_value = self.mocked_email_msg

                try:
                    notifications.send_email_smtp(*self.notification_args)
                except socket.error:
                    self.fail("send_email_smtp() raised expection unexpectedly")

                SMTP.assert_called_once_with(**smtp_kws)
                self.assertEqual(notifications.generate_email.called, False)
                self.assertEqual(SMTP.sendemail.called, False)


class TestSendgridEmail(unittest.TestCase, NotificationFixture):
    """
    Tests sending Sendgrid email.
    """

    def setUp(self):
        sys.modules['sendgrid'] = mock.MagicMock()
        import sendgrid  # noqa: F401

    def tearDown(self):
        del sys.modules['sendgrid']

    @with_config({"email": {"SENDGRID_USERNAME": "Nikola",
                            "SENDGRID_PASSWORD": "jahuS"}})
    def test_sends_sendgrid_email(self):
        """
        Call notificaions.send_email_sendgrid with fixture parameters
        and check that SendGridClient is properly called.
        """

        with mock.patch('sendgrid.SendGridClient') as SendgridClient:
            notifications.send_email_sendgrid(*self.notification_args)

            SendgridClient.assert_called_once_with("Nikola", "jahuS", raise_errors=True)
            self.assertTrue(SendgridClient.return_value.send.called)


class TestSESEmail(unittest.TestCase, NotificationFixture):
    """
    Tests sending email through AWS SES.
    """

    def setUp(self):
        sys.modules['boto3'] = mock.MagicMock()
        import boto3  # noqa: F401

    def tearDown(self):
        del sys.modules['boto3']

    @with_config({})
    def test_sends_ses_email(self):
        """
        Call notificaions.send_email_ses with fixture parameters
        and check that boto is properly called.
        """

        with mock.patch('boto3.client') as boto_client:
            with mock.patch('luigi.notifications.generate_email') as generate_email:
                generate_email.return_value\
                    .as_string.return_value = self.mocked_email_msg

                notifications.send_email_ses(*self.notification_args)

                SES = boto_client.return_value
                SES.send_raw_email.assert_called_once_with(
                    Source=self.sender,
                    Destinations=self.recipients,
                    RawMessage={'Data': self.mocked_email_msg})


class TestSNSNotification(unittest.TestCase, NotificationFixture):
    """
    Tests sending email through AWS SNS.
    """

    def setUp(self):
        sys.modules['boto3'] = mock.MagicMock()
        import boto3  # noqa: F401

    def tearDown(self):
        del sys.modules['boto3']

    @with_config({})
    def test_sends_sns_email(self):
        """
        Call notificaions.send_email_sns with fixture parameters
        and check that boto3 is properly called.
        """

        with mock.patch('boto3.resource') as res:
            notifications.send_email_sns(*self.notification_args)

            SNS = res.return_value
            SNS.Topic.assert_called_once_with(self.recipients[0])
            SNS.Topic.return_value.publish.assert_called_once_with(
                Subject=self.subject, Message=self.message)

    @with_config({})
    def test_sns_subject_is_shortened(self):
        """
        Call notificaions.send_email_sns with too long Subject (more than 100 chars)
        and check that it is cut to lenght of 100 chars.
        """

        long_subject = 'Luigi: SanityCheck(regexPattern=aligned-source\\|data-not-older\\|source-chunks-compl,'\
                       'mailFailure=False, mongodb=mongodb://localhost/stats) FAILED'

        with mock.patch('boto3.resource') as res:
            notifications.send_email_sns(self.sender, long_subject, self.message,
                                         self.recipients, self.image_png)

            SNS = res.return_value
            SNS.Topic.assert_called_once_with(self.recipients[0])
            called_subj = SNS.Topic.return_value.publish.call_args[1]['Subject']
            self.assertTrue(len(called_subj) <= 100,
                            "Subject can be max 100 chars long! Found {}.".format(len(called_subj)))


class TestNotificationDispatcher(unittest.TestCase, NotificationFixture):
    """
    Test dispatching of notifications on configuration values.
    """

    def check_dispatcher(self, target):
        """
        Call notifications.send_email and test that the proper
        function was called.
        """

        expected_args = self.notification_args

        with mock.patch('luigi.notifications.{}'.format(target)) as sender:
            notifications.send_email(self.subject, self.message, self.sender,
                                     self.recipients, image_png=self.image_png)

            self.assertTrue(sender.called)

            call_args = sender.call_args[0]

            self.assertEqual(tuple(expected_args), call_args)

    @with_config({'email': {'force-send': 'True',
                            'type': 'smtp'}})
    def test_smtp(self):
        return self.check_dispatcher('send_email_smtp')

    @with_config({'email': {'force-send': 'True',
                            'type': 'ses'}})
    def test_ses(self):
        return self.check_dispatcher('send_email_ses')

    @with_config({'email': {'force-send': 'True',
                            'type': 'sendgrid'}})
    def test_sendgrid(self):
        return self.check_dispatcher('send_email_sendgrid')

    @with_config({'email': {'force-send': 'True',
                            'type': 'sns'}})
    def test_sns(self):
        return self.check_dispatcher('send_email_sns')
