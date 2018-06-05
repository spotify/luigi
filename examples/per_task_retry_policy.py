# -*- coding: utf-8 -*-

"""
You can run this example like this:

    .. code:: console

            $ luigi --module examples.per_task_retry_policy examples.PerTaskRetryPolicy --worker-keep-alive \
            --local-scheduler --scheduler-retry-delay 5  --logging-conf-file test/testconfig/logging.cfg

            ...
            ... lots of spammy output
            ...
            DEBUG: ErrorTask1__99914b932b task num failures is 1 and limit is 5
            DEBUG: ErrorTask2__99914b932b task num failures is 1 and limit is 2
            DEBUG: DynamicErrorTask1__99914b932b task num failures is 1 and limit is 3
            DEBUG: ErrorTask1__99914b932b task num failures is 2 and limit is 5
            DEBUG: ErrorTask2__99914b932b task num failures is 2 and limit is 2
            DEBUG: ErrorTask2__99914b932b task num failures limit(2) is exceeded
            DEBUG: DynamicErrorTask1__99914b932b task num failures is 2 and limit is 3
            DEBUG: ErrorTask1__99914b932b task num failures is 3 and limit is 5
            DEBUG: DynamicErrorTask1__99914b932b task num failures is 3 and limit is 3
            DEBUG: DynamicErrorTask1__99914b932b task num failures limit(3) is exceeded
            DEBUG: ErrorTask1__99914b932b task num failures is 4 and limit is 5
            DEBUG: ErrorTask1__99914b932b task num failures is 5 and limit is 5
            DEBUG: ErrorTask1__99914b932b task num failures limit(5) is exceeded
            INFO:
            ===== Luigi Execution Summary =====

            Scheduled 8 tasks of which:
            * 2 ran successfully:
                - 1 SuccessSubTask1()
                - 1 SuccessTask1()
            * 3 failed:
                - 1 DynamicErrorTask1()
                - 1 ErrorTask1()
                - 1 ErrorTask2()
            * 3 were left pending, among these:
                * 1 were missing external dependencies:
                    - 1 DynamicErrorTaskSubmitter()
                * 1 had failed dependencies:
                    - 1 examples.PerTaskRetryPolicy()
                * 1 had missing dependencies:
                    - 1 examples.PerTaskRetryPolicy()
                * 1 was not granted run permission by the scheduler:
                    - 1 DynamicErrorTaskSubmitter()

            This progress looks :( because there were failed tasks

            ===== Luigi Execution Summary =====
"""

import luigi


class PerTaskRetryPolicy(luigi.Task):
    """
        Wrapper class for some error and success tasks. Worker won't be shutdown unless there is
        pending tasks or failed tasks which will be retried. While keep-alive is active, workers
        are not shutdown while there is/are some pending task(s).

    """

    task_namespace = 'examples'

    def requires(self):
        return [ErrorTask1(), ErrorTask2(), SuccessTask1(), DynamicErrorTaskSubmitter()]

    def output(self):
        return luigi.LocalTarget(path='/tmp/_docs-%s.ldj' % self.task_id)


class ErrorTask1(luigi.Task):
    """
        This error class raises error to retry the task. retry-count for this task is 5. It can be seen on
    """

    retry = 0

    retry_count = 5

    def run(self):
        self.retry += 1
        raise Exception('Test Exception. Retry Index %s for %s' % (self.retry, self.task_family))

    def output(self):
        return luigi.LocalTarget(path='/tmp/_docs-%s.ldj' % self.task_id)


class ErrorTask2(luigi.Task):
    """
        This error class raises error to retry the task. retry-count for this task is 2
    """

    retry = 0

    retry_count = 2

    def run(self):
        self.retry += 1
        raise Exception('Test Exception. Retry Index %s for %s' % (self.retry, self.task_family))

    def output(self):
        return luigi.LocalTarget(path='/tmp/_docs-%s.ldj' % self.task_id)


class DynamicErrorTaskSubmitter(luigi.Task):
    target = None

    def run(self):
        target = yield DynamicErrorTask1()

        if target.exists():
            with self.output().open('w') as output:
                output.write('SUCCESS DynamicErrorTaskSubmitter\n')

    def output(self):
        return luigi.LocalTarget(path='/tmp/_docs-%s.ldj' % self.task_id)


class DynamicErrorTask1(luigi.Task):
    """
        This dynamic error task raises error to retry the task. retry-count for this task is 3
    """

    retry = 0

    retry_count = 3

    def run(self):
        self.retry += 1
        raise Exception('Test Exception. Retry Index %s for %s' % (self.retry, self.task_family))

    def output(self):
        return luigi.LocalTarget(path='/tmp/_docs-%s.ldj' % self.task_id)


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
