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
In particular using the config `error-email` should set up Luigi so that it will send emails when tasks fail.

::

    [core]
    error-email: foo@bar.baz
'''

import logging
import socket
import sys
import textwrap

from luigi import configuration

logger = logging.getLogger("luigi-interface")


DEFAULT_CLIENT_EMAIL = 'luigi-client@%s' % socket.gethostname()
DEBUG = False


def email_type():
    return configuration.get_config().get('core', 'email-type', 'plain')


def generate_email(sender, subject, message, recipients, image_png):
    import email
    import email.mime
    import email.mime.multipart
    import email.mime.text
    import email.mime.image

    msg_root = email.mime.multipart.MIMEMultipart('related')

    msg_text = email.mime.text.MIMEText(message, email_type())
    msg_text.set_charset('utf-8')
    msg_root.attach(msg_text)

    if image_png:
        with open(image_png, 'rb') as fp:
            msg_image = email.mime.image.MIMEImage(fp.read(), 'png')
        msg_root.attach(msg_image)

    msg_root['Subject'] = subject
    msg_root['From'] = sender
    msg_root['To'] = ','.join(recipients)

    return msg_root


def wrap_traceback(traceback):
    if email_type() == 'html':
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


def send_email_smtp(config, sender, subject, message, recipients, image_png):
    import smtplib

    smtp_ssl = config.getboolean('core', 'smtp_ssl', False)
    smtp_host = config.get('core', 'smtp_host', 'localhost')
    smtp_port = config.getint('core', 'smtp_port', 0)
    smtp_local_hostname = config.get('core', 'smtp_local_hostname', None)
    smtp_timeout = config.getfloat('core', 'smtp_timeout', None)
    kwargs = dict(host=smtp_host, port=smtp_port, local_hostname=smtp_local_hostname)
    if smtp_timeout:
        kwargs['timeout'] = smtp_timeout

    smtp_login = config.get('core', 'smtp_login', None)
    smtp_password = config.get('core', 'smtp_password', None)
    smtp = smtplib.SMTP(**kwargs) if not smtp_ssl else smtplib.SMTP_SSL(**kwargs)
    smtp.ehlo_or_helo_if_needed()
    smtp.starttls()
    if smtp_login and smtp_password:
        smtp.login(smtp_login, smtp_password)

    msg_root = generate_email(sender, subject, message, recipients, image_png)

    smtp.sendmail(sender, recipients, msg_root.as_string())


def send_email_ses(config, sender, subject, message, recipients, image_png):
    """
    Sends notification through AWS SES.

    Does not handle access keys.  Use either
      1/ configuration file
      2/ EC2 instance profile

    See also http://boto3.readthedocs.org/en/latest/guide/configuration.html.
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


def send_email_sendgrid(config, sender, subject, message, recipients, image_png):
    import sendgrid
    client = sendgrid.SendGridClient(config.get('email', 'SENDGRID_USERNAME', None),
                                     config.get('email', 'SENDGRID_PASSWORD', None),
                                     raise_errors=True)
    to_send = sendgrid.Mail()
    to_send.add_to(recipients)
    to_send.set_from(sender)
    to_send.set_subject(subject)
    if email_type() == 'html':
        to_send.set_html(message)
    else:
        to_send.set_text(message)
    if image_png:
        to_send.add_attachment(image_png)

    client.send(to_send)


def _email_disabled():
    if email_type() == 'none':
        logger.info("Not sending email when email-type is none")
        return True
    elif configuration.get_config().getboolean('email', 'force-send', False):
        return False
    elif sys.stdout.isatty():
        logger.info("Not sending email when running from a tty")
        return True
    elif DEBUG:
        logger.info("Not sending email when running in debug mode")
    else:
        return False


def send_email_sns(config, sender, subject, message, topic_ARN, image_png):
    """
    Sends notification through AWS SNS. Takes Topic ARN from recipients.

    Does not handle access keys.  Use either
      1/ configuration file
      2/ EC2 instance profile

    See also http://boto3.readthedocs.org/en/latest/guide/configuration.html.
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

    Dispatches on config value email.type.  Default is 'smtp'.
    """
    config = configuration.get_config()
    notifiers = {'ses': send_email_ses,
                 'sendgrid': send_email_sendgrid,
                 'smtp': send_email_smtp,
                 'sns': send_email_sns}

    subject = _prefix(subject)
    if not recipients or recipients == (None,):
        return
    if _email_disabled():
        return

    # Clean the recipients lists to allow multiple error-email addresses, comma
    # separated in luigi.cfg
    recipients_tmp = []
    for r in recipients:
        recipients_tmp.extend(r.split(','))

    # Replace original recipients with the clean list
    recipients = recipients_tmp

    # Get appropriate sender and call it to send the notification
    email_sender_type = config.get('email', 'type', None)
    email_sender = notifiers.get(email_sender_type, send_email_smtp)
    email_sender(config, sender, subject, message, recipients, image_png)


def _email_recipients(additional_recipients=None):
    config = configuration.get_config()
    receiver = config.get('core', 'error-email', None)
    recipients = [receiver] if receiver else []
    if additional_recipients:
        if isinstance(additional_recipients, str):
            recipients.append(additional_recipients)
        else:
            recipients.extend(additional_recipients)
    return recipients


def send_error_email(subject, message, additional_recipients=None):
    """
    Sends an email to the configured error-email.

    If no error-email is configured, then a message is logged.
    """
    config = configuration.get_config()
    recipients = _email_recipients(additional_recipients)
    if recipients:
        sender = config.get('core', 'email-sender', DEFAULT_CLIENT_EMAIL)
        logger.info("Sending warning email to %r", recipients)
        send_email(
            subject=subject,
            message=message,
            sender=sender,
            recipients=recipients
        )
    else:
        logger.info("Skipping error email. Set `error-email` in the `core` "
                    "section of the luigi config file or override `owner_email`"
                    "in the task to receive error emails.")


def _prefix(subject):
    """
    If the config has a special prefix for emails then this function adds
    this prefix.
    """
    config = configuration.get_config()
    email_prefix = config.get('core', 'email-prefix', None)
    if email_prefix is not None:
        subject = "%s %s" % (email_prefix, subject)
    return subject


def format_task_error(headline, task, formatted_exception=None):
    """
    Format a message body for an error email related to a luigi.task.Task

    :param headline: Summary line for the message
    :param task: `luigi.task.Task` instance where this error occurred
    :param formatted_exception: optional string showing traceback

    :return: message body

    """

    typ = email_type()
    if formatted_exception:
        formatted_exception = wrap_traceback(formatted_exception)
    else:
        formatted_exception = ""

    if typ == 'html':
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

        <h2>Traceback</h2>
        {traceback}
        </body>
        </html>
        ''')

        str_params = task.to_str_params()
        params = '\n'.join('<tr><th>{}</th><td>{}</td></tr>'.format(*items) for items in str_params.items())
        body = msg_template.format(headline=headline, name=task.task_family, param_rows=params,
                                   traceback=formatted_exception)
    else:
        msg_template = textwrap.dedent('''\
        {headline}

        Name: {name}

        Parameters:
        {params}

        {traceback}
        ''')

        str_params = task.to_str_params()
        max_width = max([0] + [len(x) for x in str_params.keys()])
        params = '\n'.join('  {:{width}}: {}'.format(*items, width=max_width) for items in str_params.items())
        body = msg_template.format(headline=headline, name=task.task_family, params=params,
                                   traceback=formatted_exception)

    return body
