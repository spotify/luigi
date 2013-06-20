import sys
import logging
import socket
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

    smtp = smtplib.SMTP('localhost')

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
