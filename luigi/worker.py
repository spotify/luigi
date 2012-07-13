import random
from scheduler import CentralPlannerScheduler
import threading
import time
import sys
import os
import traceback
import logging
import warnings

logger = logging.getLogger('luigi-interface')


def send_email(subject, message, recipients, image_png=None):
    import smtplib
    import email
    import email.mime
    import email.mime.multipart
    import email.mime.text
    import email.mime.image

    smtp = smtplib.SMTP('localhost')
    sender = 'no-reply@spotify.com'
    # raw_message = "From: Spotify Cronutil <%s>\r\n" % sender + \
        # "To: %s\r\n\r\n" % ', '.join(recipients) + \
        # message

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
    msg_root['From'] = 'Spotify Builder'
    msg_root['To'] = ','.join(recipients)

    smtp.sendmail(sender, recipients, msg_root.as_string())


class Worker(object):
    """ Worker object communicates with a scheduler.

    Simple class that talks to a scheduler and:
    - Tells the scheduler what it has to do + its dependencies
    - Asks for stuff to do (pulls it in a loop and runs it)
    """

    def __init__(self, scheduler=CentralPlannerScheduler(), worker_id=None, erroremail=None, worker_processes=1):
        if not worker_id:
            worker_id = 'worker-%09d' % random.randrange(0, 999999999)

        self.__id = worker_id
        self.__erroremail = erroremail

        self.__scheduler = scheduler
        if isinstance(scheduler, CentralPlannerScheduler) and worker_processes != 1:
            warnings.warn("Will only use one process when running with local in-process scheduler")
            worker_processes = 1
        self.worker_processes = worker_processes
        self.__scheduled_tasks = {}

        class KeepAliveThread(threading.Thread):
            """ Peridiacally tell the scheduler that the worker still lives """
            def run(self):
                while True:
                    time.sleep(1.0)
                    try:
                        scheduler.ping(worker=worker_id)
                    except:  # httplib.BadStatusLine:
                        print 'WARNING: could not ping!'
                        raise

        k = KeepAliveThread()
        k.daemon = True
        k.start()

    def add(self, task):
        """ Returns True if the task is already complete"""
        s = str(task)
        if s in self.__scheduled_tasks:
            return False  # will never put a complete task in __scheduled_tasks

        if task.complete():
            # Not submitting finished tasks to reduce size of output tree
            # self.__scheduler.add_task(s, status='DONE', worker=self.__id)
            return True
        elif task.run == NotImplemented:
            self.__scheduled_tasks[s] = task
            logger.warning('Task %s is is not complete and run() is not implemented. Probably a missing external dependency.', s)
            self.__scheduler.add_task(s, status='BROKEN', worker=self.__id)
            logger.debug("Done marking task %s as broken", s)
        else:
            self.__scheduled_tasks[s] = task
            self.__scheduler.add_task(s, status='PENDING', worker=self.__id)
            logger.info('Scheduled %s' % s)
            for task_2 in task.deps():
                s2 = str(task_2)
                 # Schedule stuff recursively
                if not self.add(task_2):  # Not submitting dependencies to things that are complete
                    self.__scheduler.add_dep(s, s2, worker=self.__id)
        return False

    def _run_task(self, task_id):
        task = self.__scheduled_tasks[task_id]

        logger.info('[pid %s] Running   %s', os.getpid(), task_id)
        try:
            # Verify that all the tasks are fulfilled!
            ok = True
            for task_2 in task.deps():
                if not task_2.complete():
                    ok = False

            if not ok:
                raise RuntimeError('Unfulfilled dependencies at run time!')

            task.run()
            logger.info('[pid %s] Done      %s', os.getpid(), task_id)
            status, expl = 'DONE', None

        except KeyboardInterrupt:
            raise
        except:
            status = 'FAILED'
            expl = traceback.format_exc(sys.exc_info()[2])
            if self.__erroremail and not sys.stdout.isatty():
                logger.error("[pid %s] Error while running %s. Sending error email", os.getpid(), task)
                send_email("Luigi: %s FAILED" % task, expl,
                           (self.__erroremail,))
            logger.error(expl)

        self.__scheduler.status(task_id, status=status, expl=expl, worker=self.__id)

    def run(self):
        children = set()

        while True:
            while len(children) >= self.worker_processes:
                died_pid, status = os.wait()
                if died_pid in children:
                    children.remove(died_pid)
                else:
                    logger.warning("Some random process %s died" % died_pid)

            logger.debug("Asking scheduler for work...")
            pending_tasks, task_id = self.__scheduler.get_work(worker=self.__id)
            logger.debug("Got response from scheduler! (%s, %s)", pending_tasks, task_id)

            if task_id == None:
                # TODO: sleep for a bit and query server again if there are pending tasks in the future we might be able to run
                if not children:
                    break
                else:
                    died_pid, status = os.wait()
                    if died_pid in children:
                        children.remove(died_pid)
                    else:
                        logger.warning("Some random process %s died" % died_pid)
                    continue
            if self.worker_processes > 1:
                child_pid = os.fork()
                if child_pid:
                    children.add(child_pid)
                else:
                    random.seed(os.getpid())  # need to have different random seeds...
                    self._run_task(task_id)
                    os._exit(0)
            else:
                self._run_task(task_id)

        while children:
            died_pid, status = os.wait()
            if died_pid in children:
                children.remove(died_pid)
            else:
                logger.warning("Some random process %s died" % died_pid)
