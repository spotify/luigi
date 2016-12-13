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

''' Supports sending emails when tasks fail.

This needs some more documentation.
See :doc:`/configuration` for configuration options.
In particular using the config `receiver` should set up Luigi so that it will send emails when tasks fail.

.. code-block:: ini

    [email]
    receiver=foo@bar.baz
'''

import logging
import socket
import sys
import textwrap

import luigi.task
import luigi.parameter

logger = logging.getLogger("luigi-interface")
DEFAULT_CLIENT_EMAIL = 'luigi-client@%s' % socket.gethostname()


class TestNotificationsTask(luigi.task.Task):
    """
    You may invoke this task to quickly check if you correctly have setup your
    notifications Configuration.  You can run:

    .. code-block:: console

            $ luigi TestNotificationsTask --local-scheduler --email-force-send

    And then check your email inbox to see if you got an error email or any
    other kind of notifications that you expected.
    """
    raise_in_complete = luigi.parameter.BoolParameter(description='If true, fail in complete() instead of run()')

    def run(self):
        raise ValueError('Testing notifications triggering')

    def complete(self):
        if self.raise_in_complete:
            raise ValueError('Testing notifications triggering')
        return False


class email(luigi.Config):
    force_send = luigi.parameter.BoolParameter(
        default=False,
        description='Send e-mail even from a tty')
    format = luigi.parameter.ChoiceParameter(
        default='plain',
        config_path=dict(section='core', name='email-type'),
        choices=('plain', 'html', 'none'),
        description='Format type for sent e-mails')
    method = luigi.parameter.ChoiceParameter(
        default='smtp',
        config_path=dict(section='email', name='type'),
        choices=('smtp', 'sendgrid', 'ses', 'sns'),
        description='Method for sending e-mail')
    prefix = luigi.parameter.Parameter(
        default='',
        config_path=dict(section='core', name='email-prefix'),
        description='Prefix for subject lines of all e-mails')
    receiver = luigi.parameter.Parameter(
        default='',
        config_path=dict(section='core', name='error-email'),
        description='Address to send error e-mails to')
    sender = luigi.parameter.Parameter(
        default=DEFAULT_CLIENT_EMAIL,
        config_path=dict(section='core', name='email-sender'),
        description='Address to send e-mails from')


class smtp(luigi.Config):
    host = luigi.parameter.Parameter(
        default='localhost',
        config_path=dict(section='core', name='smtp_host'),
        description='Hostname of smtp server')
    local_hostname = luigi.parameter.Parameter(
        default=None,
        config_path=dict(section='core', name='smtp_local_hostname'),
        description='If specified, local_hostname is used as the FQDN of the local host in the HELO/EHLO command')
    no_tls = luigi.parameter.BoolParameter(
        default=False,
        config_path=dict(section='core', name='smtp_without_tls'),
        description='Do not use TLS in SMTP connections')
    password = luigi.parameter.Parameter(
        default=None,
        config_path=dict(section='core', name='smtp_password'),
        description='Password for the SMTP server login')
    port = luigi.parameter.IntParameter(
        default=0,
        config_path=dict(section='core', name='smtp_port'),
        description='Port number for smtp server')
    ssl = luigi.parameter.BoolParameter(
        default=False,
        config_path=dict(section='core', name='smtp_ssl'),
        description='Use SSL for the SMTP connection.')
    timeout = luigi.parameter.FloatParameter(
        default=10.0,
        config_path=dict(section='core', name='smtp_timeout'),
        description='Number of seconds before timing out the smtp connection')
    username = luigi.parameter.Parameter(
        default=None,
        config_path=dict(section='core', name='smtp_login'),
        description='Username used to log in to the SMTP host')


class sendgrid(luigi.Config):
    username = luigi.parameter.Parameter(
        config_path=dict(section='email', name='SENDGRID_USERNAME'),
        description='Username for sendgrid login')
    password = luigi.parameter.Parameter(
        config_path=dict(section='email', name='SENDGRID_PASSWORD'),
        description='Username for sendgrid login')


def generate_email(sender, subject, message, recipients, image_png):
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.image import MIMEImage

    msg_root = MIMEMultipart('related')

    msg_text = MIMEText(message, email().format)
    msg_text.set_charset('utf-8')
    msg_root.attach(msg_text)

    if image_png:
        with open(image_png, 'rb') as fp:
            msg_image = MIMEImage(fp.read(), 'png')
        msg_root.attach(msg_image)

    msg_root['Subject'] = subject
    msg_root['From'] = sender
    msg_root['To'] = ','.join(recipients)

    return msg_root


def wrap_traceback(traceback):
    """
    For internal use only (until further notice)
    """
    if email().format == 'html':
        try:
            from pygments import highlight
            from pygments.lexers import PythonTracebackLexer
            from pygments.formatters import HtmlFormatter
            with_pygments = True
        except ImportError:
            with_pygments = False

        if with_pygments:
            formatter = HtmlFormatter(noclasses=True)
            wrapped = highlight(traceback, PythonTracebackLexer(), formatter)
        else:
            wrapped = '<pre>%s</pre>' % traceback
    else:
        wrapped = traceback

    return wrapped


def send_email_smtp(sender, subject, message, recipients, image_png):
    import smtplib

    smtp_config = smtp()
    kwargs = dict(
        host=smtp_config.host,
        port=smtp_config.port,
        local_hostname=smtp_config.local_hostname,
    )
    if smtp_config.timeout:
        kwargs['timeout'] = smtp_config.timeout

    try:
        smtp_conn = smtplib.SMTP_SSL(**kwargs) if smtp_config.ssl else smtplib.SMTP(**kwargs)
        smtp_conn.ehlo_or_helo_if_needed()
        if smtp_conn.has_extn('starttls') and not smtp_config.no_tls:
            smtp_conn.starttls()
        if smtp_config.username and smtp_config.password:
            smtp_conn.login(smtp_config.username, smtp_config.password)

        msg_root = generate_email(sender, subject, message, recipients, image_png)

        smtp_conn.sendmail(sender, recipients, msg_root.as_string())
    except socket.error as exception:
        logger.error("Not able to connect to smtp server: %s", exception)


def send_email_ses(sender, subject, message, recipients, image_png):
    """
    Sends notification through AWS SES.

    Does not handle access keys.  Use either
      1/ configuration file
      2/ EC2 instance profile

    See also https://boto3.readthedocs.io/en/latest/guide/configuration.html.
    """
    from boto3 import client as boto3_client

    client = boto3_client('ses')

    msg_root = generate_email(sender, subject, message, recipients, image_png)
    response = client.send_raw_email(Source=sender,
                                     Destinations=recipients,
                                     RawMessage={'Data': msg_root.as_string()})

    logger.debug(("Message sent to SES.\nMessageId: {},\nRequestId: {},\n"
                 "HTTPSStatusCode: {}").format(response['MessageId'],
                                               response['ResponseMetadata']['RequestId'],
                                               response['ResponseMetadata']['HTTPStatusCode']))


def send_email_sendgrid(sender, subject, message, recipients, image_png):
    import sendgrid as sendgrid_lib
    client = sendgrid_lib.SendGridClient(
        sendgrid().username, sendgrid().password, raise_errors=True)
    to_send = sendgrid_lib.Mail()
    to_send.add_to(recipients)
    to_send.set_from(sender)
    to_send.set_subject(subject)
    if email().format == 'html':
        to_send.set_html(message)
    else:
        to_send.set_text(message)
    if image_png:
        to_send.add_attachment(image_png)

    client.send(to_send)


def _email_disabled_reason():
    if email().format == 'none':
        return "email format is 'none'"
    elif email().force_send:
        return None
    elif sys.stdout.isatty():
        return "running from a tty"
    else:
        return None


def send_email_sns(sender, subject, message, topic_ARN, image_png):
    """
    Sends notification through AWS SNS. Takes Topic ARN from recipients.

    Does not handle access keys.  Use either
      1/ configuration file
      2/ EC2 instance profile

    See also https://boto3.readthedocs.io/en/latest/guide/configuration.html.
    """
    from boto3 import resource as boto3_resource

    sns = boto3_resource('sns')
    topic = sns.Topic(topic_ARN[0])

    # Subject is max 100 chars
    if len(subject) > 100:
        subject = subject[0:48] + '...' + subject[-49:]

    response = topic.publish(Subject=subject, Message=message)

    logger.debug(("Message sent to SNS.\nMessageId: {},\nRequestId: {},\n"
                 "HTTPSStatusCode: {}").format(response['MessageId'],
                                               response['ResponseMetadata']['RequestId'],
                                               response['ResponseMetadata']['HTTPStatusCode']))


def send_email(subject, message, sender, recipients, image_png=None):
    """
    Decides whether to send notification. Notification is cancelled if there are
    no recipients or if stdout is onto tty or if in debug mode.

    Dispatches on config value email.method.  Default is 'smtp'.
    """
    notifiers = {
        'ses': send_email_ses,
        'sendgrid': send_email_sendgrid,
        'smtp': send_email_smtp,
        'sns': send_email_sns,
    }

    subject = _prefix(subject)
    if not recipients or recipients == (None,):
        return

    if _email_disabled_reason():
        logger.info("Not sending email to %r because %s",
                    recipients, _email_disabled_reason())
        return

    # Clean the recipients lists to allow multiple email addresses, comma
    # separated in luigi.cfg
    recipients_tmp = []
    for r in recipients:
        recipients_tmp.extend([a.strip() for a in r.split(',') if a.strip()])

    # Replace original recipients with the clean list
    recipients = recipients_tmp

    logger.info("Sending email to %r", recipients)

    # Get appropriate sender and call it to send the notification
    email_sender = notifiers[email().method]
    email_sender(sender, subject, message, recipients, image_png)


def _email_recipients(additional_recipients=None):
    receiver = email().receiver
    recipients = [receiver] if receiver else []
    if additional_recipients:
        if isinstance(additional_recipients, str):
            recipients.append(additional_recipients)
        else:
            recipients.extend(additional_recipients)
    return recipients


def send_error_email(subject, message, additional_recipients=None):
    """
    Sends an email to the configured error email, if it's configured.
    """
    recipients = _email_recipients(additional_recipients)
    sender = email().sender
    send_email(
        subject=subject,
        message=message,
        sender=sender,
        recipients=recipients
    )


def _prefix(subject):
    """
    If the config has a special prefix for emails then this function adds
    this prefix.
    """
    if email().prefix:
        return "{} {}".format(email().prefix, subject)
    else:
        return subject


def format_task_error(headline, task, command, formatted_exception=None):
    """
    Format a message body for an error email related to a luigi.task.Task

    :param headline: Summary line for the message
    :param task: `luigi.task.Task` instance where this error occurred
    :param formatted_exception: optional string showing traceback

    :return: message body
    """

    if formatted_exception:
        formatted_exception = wrap_traceback(formatted_exception)
    else:
        formatted_exception = ""

    if email().format == 'html':
        msg_template = textwrap.dedent('''
        <html>
        <body>
        <h2>{headline}</h2>

        <table style="border-top: 1px solid black; border-bottom: 1px solid black">
        <thead>
        <tr><th>name</th><td>{name}</td></tr>
        </thead>
        <tbody>
        {param_rows}
        </tbody>
        </table>
        </pre>

        <h2>Command line</h2>
        <pre>
        {command}
        </pre>

        <h2>Traceback</h2>
        {traceback}
        </body>
        </html>
        ''')

        str_params = task.to_str_params()
        params = '\n'.join('<tr><th>{}</th><td>{}</td></tr>'.format(*items) for items in str_params.items())
        body = msg_template.format(headline=headline, name=task.task_family, param_rows=params,
                                   command=command, traceback=formatted_exception)
    else:
        msg_template = textwrap.dedent('''\
        {headline}

        Name: {name}

        Parameters:
        {params}

        Command line:
          {command}

        {traceback}
        ''')

        str_params = task.to_str_params()
        max_width = max([0] + [len(x) for x in str_params.keys()])
        params = '\n'.join('  {:{width}}: {}'.format(*items, width=max_width) for items in str_params.items())
        body = msg_template.format(headline=headline, name=task.task_family, params=params,
                                   command=command, traceback=formatted_exception)

    return body
