import sys
import logging
import socket
from luigi import configuration
logger = logging.getLogger("luigi-interface")


DEFAULT_CLIENT_EMAIL = 'luigi-client@%s' % socket.getfqdn()
DEBUG = False


def send_email(subject, message, sender, recipients, image_png=None):
    logger.debug("Emailing:\n"
                 "-------------\n"
                 "To: %s\n"
                 "From: %s\n"
                 "Subject: %s\n"
                 "Message:\n"
                 "%s\n"
                 "-------------" % (recipients, sender, subject, message))
    if not recipients or recipients == (None,):
        return
    if sys.stdout.isatty() or DEBUG:
        logger.info("Not sending email when running from a tty or in debug mode")
        return

    import smtplib
    import email
    import email.mime
    import email.mime.multipart
    import email.mime.text
    import email.mime.image

    # Clean the recipients lists to allow multiple error-email addresses, comma
    # separated in client.cfg
    recipients_tmp = []
    for r in recipients:
        recipients_tmp.extend(r.split(','))

    # Replace original recipients with the clean list
    recipients = recipients_tmp

    config = configuration.get_config()
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

    msg_root = email.mime.multipart.MIMEMultipart('related')

    msg_text = email.mime.text.MIMEText(message, 'plain')
    msg_text.set_charset('utf-8')
    msg_root.attach(msg_text)

    if image_png:
        fp = open(image_png, 'rb')
        msg_image = email.mime.image.MIMEImage(fp.read(), 'png')
        fp.close()
        msg_root.attach(msg_image)

    msg_root['Subject'] = subject
    msg_root['From'] = 'Luigi'
    msg_root['To'] = ','.join(recipients)

    smtp.sendmail(sender, recipients, msg_root.as_string())


def send_error_email(subject, message):
    """ Sends an email to the configured error-email """
    config = configuration.get_config()
    receiver = config.get('core', 'error-email', None)
    sender = config.get('core', 'email-sender', DEFAULT_CLIENT_EMAIL)
    logger.info("Sending warning email to %r" % (receiver,))
    send_email(
        subject=subject,
        message=message,
        sender=sender,
        recipients=(receiver,)
    )
