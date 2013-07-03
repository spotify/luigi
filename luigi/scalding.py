import logging
import os

import luigi.hadoop
import luigi.hadoop_jar
import luigi.hdfs

logger = logging.getLogger('luigi-interface')


class ScaldingJobRunner(luigi.hadoop.JobRunner):
    """JobRunner for `pyscald` commands. Used to run a ScaldingJobTask"""

    def __init__(self):
        pass

    def run_job(self, job):
        if not job.source() or not os.path.exists(job.source()):
            logger.error("Can't find jar: {0}, full path {1}".format(
                         job.source(), os.path.abspath(job.source())))
            raise Exception("job jar does not exist")
        arglist = ['pyscald', '--hdfs', job.source()]
        if job.main():
            arglist.extend(['--job-class', job.main()])
        if job.jobconfs():
            arglist.extend(['--job-conf'] + job.jobconfs())

        (tmp_files, job_args) = luigi.hadoop_jar.fix_paths(job)

        arglist += job_args

        luigi.hadoop.run_and_track_hadoop_job(arglist)

        for a, b in tmp_files:
            a.move(b)


class ScaldingJobTask(luigi.hadoop.BaseHadoopJobTask):
    """A job task for `pyscald` commands that define a scala source and
    (optional) main method.

    Requires pyscald: https://github.com/nevillelyh/pyscald

    requires() should return a dictionary where the keys are Scalding argument
    names and values are lists of paths. For example:
    {'input1': ['A', 'B'], 'input2': ['C']} => --input1 A,B --input2 C
    """

    def source(self):
        """Path to the scala source for this Hadoop Job"""
        return None

    def main(self):
        """optional main method for this Hadoop Job"""
        return None

    def job_runner(self):
        return ScaldingJobRunner()

    def atomic_output(self):
        """If True, then rewrite output arguments to be temp locations and
        atomically move them into place after the job finishes"""
        return True

    def job_args(self):
        """Extra arguments to pass to the Scalding job"""
        return []

    def args(self):
        """returns an array of args to pass to the job."""
        arglist = []
        for k, v in self.requires_hadoop().iteritems():
            arglist.extend(['--' + k, ','.join([t.output().path for t in v])])
        arglist.extend(['--output', self.output()])
        arglist.extend(self.job_args())
        return arglist
