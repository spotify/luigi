# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
Provides functionality to run a Hadoop job using a Jar
"""

import logging
import os
import pipes
import random
import warnings

import luigi.contrib.hadoop
import luigi.contrib.hdfs

logger = logging.getLogger('luigi-interface')


def fix_paths(job):
    """
    Coerce input arguments to use temporary files when used for output.

    Return a list of temporary file pairs (tmpfile, destination path) and
    a list of arguments.

    Converts each HdfsTarget to a string for the path.
    """
    tmp_files = []
    args = []
    for x in job.args():
        if isinstance(x, luigi.contrib.hdfs.HdfsTarget):  # input/output
            if x.exists() or not job.atomic_output():  # input
                args.append(x.path)
            else:  # output
                x_path_no_slash = x.path[:-1] if x.path[-1] == '/' else x.path
                y = luigi.contrib.hdfs.HdfsTarget(x_path_no_slash + '-luigi-tmp-%09d' % random.randrange(0, 1e10))
                tmp_files.append((y, x_path_no_slash))
                logger.info('Using temp path: %s for path %s', y.path, x.path)
                args.append(y.path)
        else:
            try:
                # hopefully the target has a path to use
                args.append(x.path)
            except AttributeError:
                # if there's no path then hope converting it to a string will work
                args.append(str(x))

    return (tmp_files, args)


class HadoopJarJobError(Exception):
    pass


class HadoopJarJobRunner(luigi.contrib.hadoop.JobRunner):
    """
    JobRunner for `hadoop jar` commands. Used to run a HadoopJarJobTask.
    """

    def __init__(self):
        pass

    def run_job(self, job, tracking_url_callback=None):
        if tracking_url_callback is not None:
            warnings.warn("tracking_url_callback argument is deprecated, task.set_tracking_url is "
                          "used instead.", DeprecationWarning)

        # TODO(jcrobak): libjars, files, etc. Can refactor out of
        # hadoop.HadoopJobRunner
        if not job.jar():
            raise HadoopJarJobError("Jar not defined")

        hadoop_arglist = luigi.contrib.hdfs.load_hadoop_cmd() + ['jar', job.jar()]
        if job.main():
            hadoop_arglist.append(job.main())

        jobconfs = job.jobconfs()

        for jc in jobconfs:
            hadoop_arglist += ['-D' + jc]

        (tmp_files, job_args) = fix_paths(job)

        hadoop_arglist += job_args

        ssh_config = job.ssh()
        if ssh_config:
            host = ssh_config.get("host", None)
            key_file = ssh_config.get("key_file", None)
            username = ssh_config.get("username", None)
            if not host or not key_file or not username:
                raise HadoopJarJobError("missing some config for HadoopRemoteJarJobRunner")
            arglist = ['ssh', '-i', key_file,
                       '-o', 'BatchMode=yes']  # no password prompts etc
            if ssh_config.get("no_host_key_check", False):
                arglist += ['-o', 'UserKnownHostsFile=/dev/null',
                            '-o', 'StrictHostKeyChecking=no']
            arglist.append('{}@{}'.format(username, host))
            hadoop_arglist = [pipes.quote(arg) for arg in hadoop_arglist]
            arglist.append(' '.join(hadoop_arglist))
        else:
            if not os.path.exists(job.jar()):
                logger.error("Can't find jar: %s, full path %s", job.jar(),
                             os.path.abspath(job.jar()))
                raise HadoopJarJobError("job jar does not exist")
            arglist = hadoop_arglist

        luigi.contrib.hadoop.run_and_track_hadoop_job(arglist, job.set_tracking_url)

        for a, b in tmp_files:
            a.move(b)


class HadoopJarJobTask(luigi.contrib.hadoop.BaseHadoopJobTask):
    """
    A job task for `hadoop jar` commands that define a jar and (optional) main method.
    """

    def jar(self):
        """
        Path to the jar for this Hadoop Job.
        """
        return None

    def main(self):
        """
        optional main method for this Hadoop Job.
        """
        return None

    def job_runner(self):
        # We recommend that you define a subclass, override this method and set up your own config
        return HadoopJarJobRunner()

    def atomic_output(self):
        """
        If True, then rewrite output arguments to be temp locations and
        atomically move them into place after the job finishes.
        """
        return True

    def ssh(self):
        """
        Set this to run hadoop command remotely via ssh. It needs to be a dict that looks like
        {"host": "myhost", "key_file": None, "username": None, ["no_host_key_check": False]}
        """
        return None

    def args(self):
        """
        Returns an array of args to pass to the job (after hadoop jar <jar> <main>).
        """
        return []
