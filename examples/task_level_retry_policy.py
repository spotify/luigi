# -*- coding: utf-8 -*-

#
# DO NOT FORGET to set keep-alive as True.
#


"""
You can run this example like this:

    .. code:: console

            $ luigi --module examples.task_level_retry_policy examples.TaskLevelRetryPolicy --worker-keep-alive --local-scheduler --scheduler-retry-delay 5
            ...
            ... lots of spammy output
            ...
            INFO: Informed scheduler that task   examples.TaskLevelRetryPolicy__99914b932b   has status   PENDING
            INFO: Informed scheduler that task   SuccessTask1__99914b932b   has status   DONE
            INFO: Informed scheduler that task   ErrorTask2__99914b932b   has status   PENDING
            INFO: Informed scheduler that task   ErrorTask1__99914b932b   has status   PENDING
            INFO: Done scheduling tasks
            INFO: Running Worker with 1 processes
            INFO: [pid 90370] Worker Worker(salt=055067228, workers=1, host=Ahmets-MacBook-Air.local, username=ahmetdal, pid=90370) running   ErrorTask1()
            ERROR: [pid 90370] Worker Worker(salt=055067228, workers=1, host=Ahmets-MacBook-Air.local, username=ahmetdal, pid=90370) failed    ErrorTask1()
            Traceback (most recent call last):
                .....Some trace
                raise Exception('Test Exception. Retry Index %s for %s' % (self.retry, self.task_family))
            Exception: Test Exception. Retry Index 1 for ErrorTask1
            INFO: Informed scheduler that task   ErrorTask1__99914b932b   has status   FAILED
            INFO: [pid 90370] Worker Worker(salt=055067228, workers=1, host=Ahmets-MacBook-Air.local, username=ahmetdal, pid=90370) running   ErrorTask2()
            ERROR: [pid 90370] Worker Worker(salt=055067228, workers=1, host=Ahmets-MacBook-Air.local, username=ahmetdal, pid=90370) failed    ErrorTask2()
            Traceback (most recent call last):
                .....Some trace
                raise Exception('Test Exception. Retry Index %s for %s' % (self.retry, self.task_family))
            Exception: Test Exception. Retry Index 1 for ErrorTask2
            INFO: Skipping error email. Set `error-email` in the `core` section of the Luigi config file or override `owner_email` in the task to receive error emails.
            INFO: Informed scheduler that task   ErrorTask2__99914b932b   has status   FAILED
            INFO: [pid 90370] Worker Worker(salt=055067228, workers=1, host=Ahmets-MacBook-Air.local, username=ahmetdal, pid=90370) running   ErrorTask1()
            ERROR: [pid 90370] Worker Worker(salt=055067228, workers=1, host=Ahmets-MacBook-Air.local, username=ahmetdal, pid=90370) failed    ErrorTask1()
            Traceback (most recent call last):
                .....Some trace
                raise Exception('Test Exception. Retry Index %s for %s' % (self.retry, self.task_family))
            Exception: Test Exception. Retry Index 2 for ErrorTask1
            INFO: Skipping error email. Set `error-email` in the `core` section of the Luigi config file or override `owner_email` in the task to receive error emails.
            INFO: Informed scheduler that task   ErrorTask1__99914b932b   has status   FAILED
            INFO: [pid 90370] Worker Worker(salt=055067228, workers=1, host=Ahmets-MacBook-Air.local, username=ahmetdal, pid=90370) running   ErrorTask2()
            ERROR: [pid 90370] Worker Worker(salt=055067228, workers=1, host=Ahmets-MacBook-Air.local, username=ahmetdal, pid=90370) failed    ErrorTask2()
            Traceback (most recent call last):
                .....Some trace
                raise Exception('Test Exception. Retry Index %s for %s' % (self.retry, self.task_family))
            Exception: Test Exception. Retry Index 2 for ErrorTask2
            INFO: Skipping error email. Set `error-email` in the `core` section of the Luigi config file or override `owner_email` in the task to receive error emails.
            INFO: Skipping error email. Set `error-email` in the `core` section of the Luigi config file or override `owner_email` in the task to receive error emails.
            INFO: Informed scheduler that task   ErrorTask2__99914b932b   has status   FAILED
            INFO: [pid 90370] Worker Worker(salt=055067228, workers=1, host=Ahmets-MacBook-Air.local, username=ahmetdal, pid=90370) running   ErrorTask1()
            ERROR: [pid 90370] Worker Worker(salt=055067228, workers=1, host=Ahmets-MacBook-Air.local, username=ahmetdal, pid=90370) failed    ErrorTask1()
            Traceback (most recent call last):
                .....Some trace
                raise Exception('Test Exception. Retry Index %s for %s' % (self.retry, self.task_family))
            Exception: Test Exception. Retry Index 3 for ErrorTask1
            INFO: Informed scheduler that task   ErrorTask1__99914b932b   has status   FAILED
            INFO: [pid 90370] Worker Worker(salt=055067228, workers=1, host=Ahmets-MacBook-Air.local, username=ahmetdal, pid=90370) running   ErrorTask1()
            ERROR: [pid 90370] Worker Worker(salt=055067228, workers=1, host=Ahmets-MacBook-Air.local, username=ahmetdal, pid=90370) failed    ErrorTask1()
            Traceback (most recent call last):
                .....Some trace
                raise Exception('Test Exception. Retry Index %s for %s' % (self.retry, self.task_family))
            Exception: Test Exception. Retry Index 4 for ErrorTask1
            INFO: Skipping error email. Set `error-email` in the `core` section of the Luigi config file or override `owner_email` in the task to receive error emails.
            INFO: Informed scheduler that task   ErrorTask1__99914b932b   has status   FAILED
            INFO: [pid 90370] Worker Worker(salt=055067228, workers=1, host=Ahmets-MacBook-Air.local, username=ahmetdal, pid=90370) running   ErrorTask1()
            ERROR: [pid 90370] Worker Worker(salt=055067228, workers=1, host=Ahmets-MacBook-Air.local, username=ahmetdal, pid=90370) failed    ErrorTask1()
            Traceback (most recent call last):
                .....Some trace
                raise Exception('Test Exception. Retry Index %s for %s' % (self.retry, self.task_family))
            Exception: Test Exception. Retry Index 5 for ErrorTask1
            INFO: Informed scheduler that task   ErrorTask1__99914b932b   has status   FAILED
            INFO: Worker Worker(salt=055067228, workers=1, host=Ahmets-MacBook-Air.local, username=ahmetdal, pid=90370) was stopped. Shutting down Keep-Alive thread
            INFO:
            ===== Luigi Execution Summary =====

            Scheduled 4 tasks of which:
            * 1 present dependencies were encountered:
                - 1 SuccessTask1()
            * 2 failed:
                - 1 ErrorTask1()
                - 1 ErrorTask2()
            * 1 were left pending, among these:
                * 1 had failed dependencies:
                    - 1 examples.TaskLevelRetryPolicy()

            This progress looks :( because there were failed tasks

            ===== Luigi Execution Summary =====


As it seems, While ``ErrorTask1`` is retried 5 times (Exception: Test Exception. Retry Index 5 for ErrorTask1),
``ErrorTask2`` is retried 2 times (Exception: Test Exception. Retry Index 2 for ErrorTask2). Luigi keeps retrying
while keep-alive mode is active.
"""

from uuid import uuid4

import luigi


class TaskLevelRetryPolicy(luigi.WrapperTask):
    """
        Wrapper class for some error and success tasks. Worker won't be shutdown unless there is
        pending tasks or failed tasks which will be retried. While keep-alive is active, workers
        are not shutdown while there is/are some pending task(s). This task also has a task level
        config which is ``upstream-status-when-all``;This configuration is about how workers behaves
        to set ``TaskLevelRetryPolicy`` status according to it's upstream tasks which are ``ErrorTask1``,
        ``ErrorTask2`` and ``SuccessTask1``. If this config is set as ``False``, Any of upstream task
        ``FAILED`` or ``DISABLED`` will make ``TaskLevelRetryPolicy`` ``FAILED`` or ``DISABLED`` which also means,
        shut the worker down immediately. While this is ``True``, all upstream tasks should be ``FAILED``
        or ``DISABLED``, worker to set ``TaskLevelRetryPolicy`` status as ``FAILED`` or ``DISABLED``.
        ``TaskLevelRetryPolicy`` will remain as PENDING unless all of its upstream task severities (so does not
        contains SUCCESS tasks) are ``FAILED`` or ``DISABLED``

    """

    task_namespace = 'examples'

    @property
    def upstream_status_when_all(self):
        return True

    def requires(self):
        return [ErrorTask1(), ErrorTask2(), SuccessTask1()]

    def output(self):
        return luigi.LocalTarget(path='/tmp/_docs-%s.ldj' % uuid4())


class ErrorTask1(luigi.Task):
    """
        This error class raises error to retry the task. retry-count for this task is 5. It can be seen on
    """

    retry = 0

    @property
    def disable_num_failures(self):
        return 5

    def run(self):
        self.retry += 1
        raise Exception('Test Exception. Retry Index %s for %s' % (self.retry, self.task_family))

    def output(self):
        return luigi.LocalTarget(path='/tmp/_docs-%s.ldj' % uuid4())


class ErrorTask2(luigi.Task):
    """
        This error class raises error to retry the task. retry-count for this task is 2
    """

    retry = 0

    @property
    def disable_num_failures(self):
        return 2

    def run(self):
        self.retry += 1
        raise Exception('Test Exception. Retry Index %s for %s' % (self.retry, self.task_family))

    def output(self):
        return luigi.LocalTarget(path='/tmp/_docs-%s.ldj' % uuid4())


class SuccessTask1(luigi.Task):
    def requires(self):
        return [SuccessSubTask1()]

    def run(self):
        with self.output().open('w') as output:
            output.write('SUCCESS Test Task 4\n')

    def output(self):
        return luigi.LocalTarget(path='/tmp/_docs-%s.ldj' % self.task_id)


class SuccessSubTask1(luigi.Task):
    """
        This success task sleeps for a while and then it is completed successfully.
    """

    def run(self):
        with self.output().open('w') as output:
            output.write('SUCCESS Test Task 4.1\n')

    def output(self):
        return luigi.LocalTarget(path='/tmp/_docs-%s.ldj' % self.task_id)


if __name__ == '__main__':
    luigi.run()
