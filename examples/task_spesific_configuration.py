# -*- coding: utf-8 -*-

#
# To make this run, you probably want to edit /etc/luigi/client.cfg and add something like:
# DO NOT FORGET to set keep-alive as True.
#

from time import sleep
from uuid import uuid4

import luigi


class MyTask(luigi.WrapperTask):
    """
        Wrapper class for some error and success tasks. Worker won't be shutdown unless there is
        pending tasks or failed tasks which will be retried. While keep-alive is active, workers
        are not shutdown while there is/are some pending task(s). This task also has a task level
        config which is ``upstream-status-when-all``;This configuration is about how workers behave
        to set ``MyTask`` status according to it's upstream tasks which are ``ErrorTask1``,
        ``ErrorTask2`` and ``SuccessTask``. If this config is set as ``False``, Any of upstream task
        ``FAILED`` or ``DISABLED`` will make ``MyTask`` ``FAILED`` or ``DISABLED`` which also means,
        shut the worker down immediately. While this is ``True``, all upstream tasks should be ``FAILED``
        or ``DISABLED``, worker to set ``MyTask`` status as ``FAILED`` or ``DISABLED``. ``MyTask`` will
        remain as PENDING unless all of its upstream task severities (so does not contains SUCCESS tasks)
        are ``FAILED`` or ``DISABLED``

    """
    config = {
        'upstream-status-when-all': True
    }

    def requires(self):
        return [ErrorTask1(), ErrorTask2(), SuccessTask()]

    def output(self):
        return luigi.LocalTarget(path='/tmp/_docs-%s.ldj' % uuid4())


class ErrorTask1(luigi.Task):
    """
        This error class raises error to retry the task. retry-count for this task is 5
    """

    config = {
        'disable-num-failures': 5
    }

    retry = 0

    def run(self):
        self.retry += 1
        print 'Retry count for %s is %d' % (self.task_family, self.retry)
        raise Exception('Test Exception')

    def output(self):
        return luigi.LocalTarget(path='/tmp/_docs-%s.ldj' % uuid4())


class ErrorTask2(luigi.Task):
    """
        This error class raises error to retry the task. retry-count for this task is 2
    """

    config = {
        'disable-num-failures': 2
    }
    retry = 0

    def run(self):
        self.retry += 1
        print 'Retry count for %s is %d' % (self.task_family, self.retry)
        raise Exception('Test Exception')

    def output(self):
        return luigi.LocalTarget(path='/tmp/_docs-%s.ldj' % uuid4())


class SuccessTask(luigi.Task):
    def requires(self):
        return [SuccessSubTask()]

    def run(self):
        with self.output().open('w') as output:
            output.write('SUCCESS Test Task 4\n')

    def output(self):
        return luigi.LocalTarget(path='/tmp/_docs-%s.ldj' % self.task_id)


class SuccessSubTask(luigi.Task):
    """
        This success task sleeps for a while and then it is completed successfully.
    """

    def run(self):
        sleep(30)

        with self.output().open('w') as output:
            output.write('SUCCESS Test Task 4.1\n')

    def output(self):
        return luigi.LocalTarget(path='/tmp/_docs-%s.ldj' % self.task_id)


if __name__ == '__main__':
    luigi.run(['MyTask', '--workers', '1', '--local-scheduler'])
