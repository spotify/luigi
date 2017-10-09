"""
You can run this example like this:

    .. code:: console

            $ luigi --module examples.long_running LongRunningParent --local-scheduler \
                    [--static-requirements N | --dynamic-requirements N [--dynamic-batch-size M]]
                    [--LongRunningTask-duration T-seconds]

If that does not work, see :ref:`CommandLine`.
"""

import luigi
import os.path
import time

class LongRunningTask(luigi.Task):
    i = luigi.IntParameter()
    duration = luigi.IntParameter(default=5)

    @property
    def priority(self):
        return self.i

    def output(self):
        return luigi.LocalTarget(os.path.join('tmp', str(self.i)))

    def run(self):
        time.sleep(self.duration)

        self.output().open('w').close()

    def complete(self):
        r = super().complete()
        logger.debug("TouchTask({}).complete() @ {}: {}".format(self.i, time.time(), r))
        return r

class LongRunningParent(luigi.Task):
    """

    """
    static_requirements = luigi.IntParameter(default=0)
    dynamic_requirements = luigi.IntParameter(default=0)
    dynamic_batch_size = luigi.IntParameter(default=1)

    def requires(self):
        return [LongRunningTask(i) for i in range(self.static_requirements)]

    def run(self):
        for i in range(0, self.dynamic_requirements, self.dynamic_batch_size):
            yield [LongRunningTask(j) for j in range(0, self.dynamic_requirements)[i:i+self.dynamic_batch_size]]

    def complete(self):
        # Force the task to always run
        return False
