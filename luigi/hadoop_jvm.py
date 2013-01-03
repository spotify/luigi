
import logging
import os

import luigi
import luigi.hdfs
from luigi.hadoop import BaseHadoopJobTask, HadoopJobRunner, JobRunner

logger = logging.getLogger('luigi-interface')


class JvmHadoopJobRunner(JobRunner):

    def __init__(self):
        pass

    def run_job(self, job):
        # TODO(jcrobak): libjars, files, etc. Can refactor out of
        # hadoop.HadoopJobRunner
        if not os.path.exists(job.jar()):
            logger.error("Can't find jar: {0}, full path {1}".format(job.jar(),
                os.path.abspath(job.jar())))
            raise Exception("job jar does not exist")
        arglist = ['hadoop', 'jar', job.jar(), job.main()]

        jobconfs = job.jobconfs()

        for jc in jobconfs:
            arglist += ['-D' + jc]

        arglist += job.args()

        HadoopJobRunner.run_and_track_hadoop_job(arglist)

        # TODO support temp output locations?
        self.finish()

    def finish(self):
        pass

    def __del__(self):
        self.finish()


class JvmHadoopJobTask(BaseHadoopJobTask):

    def jar(self):
        return None

    def main(self):
        return None

    def job_runner(self):
        # We recommend that you define a subclass, override this method and set up your own config
        return JvmHadoopJobRunner()

    def args(self):
        """returns an array of args to pass to the job (after hadoop jar <jar> <main>)."""
        return []
