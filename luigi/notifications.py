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

from luigi import configuration

logger = logging.getLogger("luigi-interface")


DEFAULT_CLIENT_EMAIL = 'luigi-client@%s' % socket.getfqdn()
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
        fp = open(image_png, 'rb')
        msg_image = email.mime.image.MIMEImage(fp.read(), 'png')
        fp.close()
        msg_root.attach(msg_image)

    msg_root['Subject'] = subject
    msg_root['From'] = sender
    msg_root['To'] = ','.join(recipients)

    return msg_root


def wrap_traceback(traceback):
    if email_type() == 'html':
        return '<pre>%s</pre>' % traceback
    return traceback


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
    if smtp_login and smtp_password:
        smtp.login(smtp_login, smtp_password)

    msg_root = generate_email(sender, subject, message, recipients, image_png)

    smtp.sendmail(sender, recipients, msg_root.as_string())


def send_email_ses(config, sender, subject, message, recipients, image_png):
    import boto.ses
    con = boto.ses.connect_to_region(config.get('email', 'region', 'us-east-1'),
                                     aws_access_key_id=config.get('email', 'AWS_ACCESS_KEY', None),
                                     aws_secret_access_key=config.get('email', 'AWS_SECRET_KEY', None))
    msg_root = generate_email(sender, subject, message, recipients, image_png)
    con.send_raw_email(msg_root.as_string(),
                       source=msg_root['From'],
                       destinations=msg_root['To'])


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


def send_email(subject, message, sender, recipients, image_png=None):
    config = configuration.get_config()

    subject = _prefix(subject)
    if not recipients or recipients == (None,):
        return
    if (sys.stdout.isatty() or DEBUG) and (not config.getboolean('email', 'force-send', False)):
        logger.info("Not sending email when running from a tty or in debug mode")
        return

    # Clean the recipients lists to allow multiple error-email addresses, comma
    # separated in client.cfg
    recipients_tmp = []
    for r in recipients:
        recipients_tmp.extend(r.split(','))

    # Replace original recipients with the clean list
    recipients = recipients_tmp

    email_sender_type = config.get('email', 'type', None)
    if email_sender_type == "ses":
        send_email_ses(config, sender, subject, message, recipients, image_png)
    elif email_sender_type == "sendgrid":
        send_email_sendgrid(config, sender, subject, message, recipients, image_png)
    else:
        send_email_smtp(config, sender, subject, message, recipients, image_png)


def send_error_email(subject, message):
    """
    Sends an email to the configured error-email.

    If no error-email is configured, then a message is logged.
    """
    config = configuration.get_config()
    receiver = config.get('core', 'error-email', None)
    if receiver:
        sender = config.get('core', 'email-sender', DEFAULT_CLIENT_EMAIL)
        logger.info("Sending warning email to %r", receiver)
        send_email(
            subject=subject,
            message=message,
            sender=sender,
            recipients=(receiver,)
        )
    else:
        logger.info("Skipping error email. Set `error-email` in the `core` "
                    "section of the luigi config file to receive error "
                    "emails.")


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
