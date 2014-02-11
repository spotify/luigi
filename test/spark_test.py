import subprocess
import StringIO
import unittest

from mock import patch

import luigi
import luigi.hdfs
from luigi.mock import MockFile
from luigi.contrib.spark import SparkJobError, SparkJob
from helpers import with_config


class HdfsJob(luigi.ExternalTask):
    def output(self):
        return luigi.hdfs.HdfsTarget('test')


class TestJob(SparkJob):
    def requires_hadoop(self):
        return HdfsJob()

    def jar(self):
        return 'jar'

    def job_class(self):
        return 'job_class'

    def output(self):
        return luigi.LocalTarget('output')


class SparkTest(unittest.TestCase):
    hcd = 'hcd-stub'
    ycd = 'ycd-stub'
    sj = 'sj-stub'
    sc = 'sc-sub'

    def setUp(self):
        pass

    def tearDown(self):
        pass

    @with_config({'spark': {'hadoop-conf-dir': hcd, 'yarn-conf-dir': ycd, 'spark-jar': sj, 'spark-class': sc}})
    @patch('subprocess.Popen')
    def test_run(self, mock):
        arglist_result = []

        def Popen_fake(arglist, stdout=None, stderr=None, env=None, close_fds=True):
            arglist_result.append(arglist)

            class P(object):
                def wait(self):
                    pass

                def poll(self):
                    return 0

                def communicate(self):
                    return 'end'

            p = P()
            p.returncode = 0
            p.stderr = StringIO.StringIO()
            p.stdout = StringIO.StringIO()
            return p

        h, p = luigi.hdfs.HdfsTarget, subprocess.Popen
        luigi.hdfs.HdfsTarget, subprocess.Popen = MockFile, Popen_fake
        try:
            MockFile.move = lambda *args, **kwargs: None
            job = TestJob()
            job.run()
            self.assertEquals(len(arglist_result), 1)
            self.assertEquals(arglist_result[0][0:6],
                              [self.sc, 'org.apache.spark.deploy.yarn.Client', '--jar', job.jar(), '--class',
                               job.job_class()])
        finally:
            luigi.hdfs.HdfsTarget, subprocess.Popen = h, p  # restore

    @with_config({'spark': {'hadoop-conf-dir': hcd, 'yarn-conf-dir': ycd, 'spark-jar': sj, 'spark-class': sc}})
    def test_handle_failed_job(self):
        def Popen_fake(arglist, stdout=None, stderr=None, env=None, close_fds=True):
            class P(object):
                def wait(self):
                    pass

                def poll(self):
                    return 1

                def communicate(self):
                    return 'end'


            p = P()
            p.returncode = 1
            if stdout == subprocess.PIPE:
                p.stdout = StringIO.StringIO('stdout')
            else:
                stdout.write('stdout')
            if stderr == subprocess.PIPE:
                p.stderr = StringIO.StringIO('stderr')
            else:
                stderr.write('stderr')
            return p

        p = subprocess.Popen
        subprocess.Popen = Popen_fake
        try:
            job = TestJob()
            job.run()
        except SparkJobError as e:
            self.assertEquals(e.err, ['stderr'])
        else:
            self.fail("Should have thrown SparkJobError")
        finally:
            subprocess.Popen = p
