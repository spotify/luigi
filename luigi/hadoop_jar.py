
import logging
import os
import random

import luigi.hadoop
import luigi.hdfs

logger = logging.getLogger('luigi-interface')


def fix_paths(job):
    """Coerce input arguments to use temporary files when used for output.
    Return a list of temporary file pairs (tmpfile, destination path) and
    a list of arguments. Converts each HdfsTarget to a string for the
    path."""
    tmp_files = []
    args = []
    for x in job.args():
        if isinstance(x, luigi.hdfs.HdfsTarget):  # input/output
            if x.exists() or not job.atomic_output():  # input
                args.append(x.path)
            else:  # output
                x_path_no_slash = x.path[:-1] if x.path[-1] == '/' else x.path
                y = luigi.hdfs.HdfsTarget(x_path_no_slash + '-luigi-tmp-%09d' % random.randrange(0, 1e10))
                tmp_files.append((y, x_path_no_slash))
                logger.info("Using temp path: {0} for path {1}".format(y.path, x.path))
                args.append(y.path)
        else:
            args.append(str(x))

    return (tmp_files, args)


class HadoopJarJobRunner(luigi.hadoop.JobRunner):
    """JobRunner for `hadoop jar` commands. Used to run a HadoopJarJobTask"""

    def __init__(self):
        pass

    def run_job(self, job):
        # TODO(jcrobak): libjars, files, etc. Can refactor out of
        # hadoop.HadoopJobRunner
        if not job.jar() or not os.path.exists(job.jar()):
            logger.error("Can't find jar: {0}, full path {1}".format(job.jar(),
                         os.path.abspath(job.jar())))
            raise Exception("job jar does not exist")
        arglist = [luigi.hdfs.load_hadoop_cmd(), 'jar', job.jar()]
        if job.main():
            arglist.append(job.main())

        jobconfs = job.jobconfs()

        for jc in jobconfs:
            arglist += ['-D' + jc]

        (tmp_files, job_args) = fix_paths(job)

        arglist += job_args

        luigi.hadoop.run_and_track_hadoop_job(arglist)

        for a, b in tmp_files:
            a.move(b)


class HadoopJarJobTask(luigi.hadoop.BaseHadoopJobTask):
    """A job task for `hadoop jar` commands that define a jar and (optional)
    main method"""

    def jar(self):
        """Path to the jar for this Hadoop Job"""
        return None

    def main(self):
        """optional main method for this Hadoop Job"""
        return None

    def job_runner(self):
        # We recommend that you define a subclass, override this method and set up your own config
        return HadoopJarJobRunner()

    def atomic_output(self):
        """If True, then rewrite output arguments to be temp locations and
        atomically move them into place after the job finishes"""
        return True

    def args(self):
        """returns an array of args to pass to the job (after hadoop jar <jar> <main>)."""
        return []
