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

import datetime
import logging
import os
import random
import re
import signal
import subprocess
import sys
import tempfile
import time

import luigi
import luigi.format
import luigi.hdfs
from luigi import configuration

logger = logging.getLogger('luigi-interface')

"""
Apache Spark on YARN support

Example configuration section in client.cfg:

[spark]
# assembly jar containing spark and dependencies
spark-jar: /usr/share/spark/jars/spark-assembly-0.8.1-incubating-hadoop2.2.0.jar

# spark script to invoke
spark-class: /usr/share/spark/spark-class

# directory containing the (client side) configuration files for the hadoop cluster
hadoop-conf-dir: /etc/hadoop/conf

"""


class SparkRunContext(object):

    def __init__(self):
        self.app_id = None

    def __enter__(self):
        self.__old_signal = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, self.kill_job)
        return self

    def kill_job(self, captured_signal=None, stack_frame=None):
        if self.app_id:
            done = False
            while not done:
                try:
                    logger.info('Job interrupted, killing application %s', self.app_id)
                    subprocess.call(['yarn', 'application', '-kill', self.app_id])
                    done = True
                except KeyboardInterrupt:
                    continue

        if captured_signal is not None:
            # adding 128 gives the exit code corresponding to a signal
            sys.exit(128 + captured_signal)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is KeyboardInterrupt:
            self.kill_job()
        signal.signal(signal.SIGTERM, self.__old_signal)


class SparkJobError(RuntimeError):

    def __init__(self, message, out=None, err=None):
        super(SparkJobError, self).__init__(message, out, err)
        self.message = message
        self.out = out
        self.err = err

    def __str__(self):
        info = self.message
        if self.out:
            info += "\nSTDOUT: " + str(self.out)
        if self.err:
            info += "\nSTDERR: " + str(self.err)
        return info


class SparkJob(luigi.Task):
    spark_workers = None
    spark_master_memory = None
    spark_worker_memory = None
    queue = luigi.Parameter(is_global=True, default=None, significant=False)
    temp_hadoop_output_file = None

    def requires_local(self):
        """
        Default impl - override this method if you need any local input to be accessible in init().
        """
        return []

    def requires_hadoop(self):
        return self.requires()  # default impl

    def input_local(self):
        return luigi.task.getpaths(self.requires_local())

    def input(self):
        return luigi.task.getpaths(self.requires())

    def deps(self):
        # Overrides the default implementation
        return luigi.task.flatten(self.requires_hadoop()) + luigi.task.flatten(self.requires_local())

    def jar(self):
        raise NotImplementedError("subclass should define jar containing job_class")

    def job_class(self):
        raise NotImplementedError("subclass should define Spark job_class")

    def job_args(self):
        return []

    def output(self):
        raise NotImplementedError("subclass should define HDFS output path")

    def run(self):
        original_output_path = self.output().path
        path_no_slash = original_output_path[:-2] if original_output_path.endswith('/*') else original_output_path
        path_no_slash = original_output_path[:-1] if original_output_path[-1] == '/' else path_no_slash
        tmp_output = luigi.hdfs.HdfsTarget(path_no_slash + '-luigi-tmp-%09d' % random.randrange(0, 1e10))

        args = ['org.apache.spark.deploy.yarn.Client']
        args += ['--jar', self.jar()]
        args += ['--class', self.job_class()]

        for a in self.job_args():
            if a == self.output().path:
                # pass temporary output path to job args
                logger.info('Using temp path: %s for path %s', tmp_output.path, original_output_path)
                args += ['--args', tmp_output.path]
            else:
                args += ['--args', str(a)]

        if self.spark_workers is not None:
            args += ['--num-workers', self.spark_workers]

        if self.spark_master_memory is not None:
            args += ['--master-memory', self.spark_master_memory]

        if self.spark_worker_memory is not None:
            args += ['--worker-memory', self.spark_worker_memory]

        queue = self.queue
        if queue is not None:
            args += ['--queue', queue]

        env = os.environ.copy()
        env['SPARK_JAR'] = configuration.get_config().get('spark', 'spark-jar')
        env['HADOOP_CONF_DIR'] = configuration.get_config().get('spark', 'hadoop-conf-dir')
        env['MASTER'] = 'yarn-client'
        spark_class = configuration.get_config().get('spark', 'spark-class')

        temp_stderr = tempfile.TemporaryFile()
        logger.info('Running: %s %s', spark_class, ' '.join(args))
        proc = subprocess.Popen([spark_class] + args, stdout=subprocess.PIPE,
                                stderr=temp_stderr, env=env, close_fds=True)

        return_code, final_state, app_id = self.track_progress(proc)
        if return_code == 0 and final_state != 'FAILED':
            tmp_output.move(path_no_slash)
        elif final_state == 'FAILED':
            raise SparkJobError('Spark job failed: see yarn logs for %s' % app_id)
        else:
            temp_stderr.seek(0)
            errors = "".join(temp_stderr.readlines())
            logger.error(errors)
            raise SparkJobError('Spark job failed', err=errors)

    def track_progress(self, proc):
        # The Spark client currently outputs a multiline status to stdout every second
        # while the application is running.  This instead captures status data and updates
        # a single line of output until the application finishes.
        app_id = None
        app_status = 'N/A'
        url = 'N/A'
        final_state = None
        start = time.time()
        with SparkRunContext() as context:
            while proc.poll() is None:
                s = proc.stdout.readline()
                app_id_s = re.compile('application identifier: (\w+)').search(s)
                if app_id_s:
                    app_id = app_id_s.group(1)
                    context.app_id = app_id
                app_status_s = re.compile('yarnAppState: (\w+)').search(s)
                if app_status_s:
                    app_status = app_status_s.group(1)
                url_s = re.compile('appTrackingUrl: (.+)').search(s)
                if url_s:
                    url = url_s.group(1)
                final_state_s = re.compile('distributedFinalState: (\w+)').search(s)
                if final_state_s:
                    final_state = final_state_s.group(1)
                if not app_id:
                    logger.info(s.strip())
                else:
                    elapsed_mins, elapsed_secs = divmod(datetime.timedelta(seconds=time.time() - start).seconds, 60)
                    status = '[%0d:%02d] Status: %s Tracking: %s' % (elapsed_mins, elapsed_secs, app_status, url)
                    sys.stdout.write("\r\x1b[K" + status)
                    sys.stdout.flush()
        logger.info(proc.communicate()[0])
        return proc.returncode, final_state, app_id


class Spark1xJob(luigi.Task):

    num_executors = None
    driver_memory = None
    executor_memory = None
    executor_cores = None
    deploy_mode = None
    queue = None
    spark_master = "yarn-client"

    def jar(self):
        raise NotImplementedError("subclass should define jar "
                                  "containing job_class")

    def dependency_jars(self):
        """
        Override to provide a list of dependency jars.
        """
        return []

    def job_class(self):
        raise NotImplementedError("subclass should define Spark job_class")

    def spark_options(self):
        return []

    def job_args(self):
        return []

    def output(self):
        raise NotImplementedError("subclass should define HDFS output path")

    def spark_heartbeat(self, line, spark_run_context):
        pass

    def run(self):
        spark_submit = configuration.get_config().get('spark', 'spark-submit',
                                                      'spark-submit')
        options = [
            '--class', self.job_class(),
        ]
        if self.num_executors is not None:
            options += ['--num-executors', self.num_executors]
        if self.driver_memory is not None:
            options += ['--driver-memory', self.driver_memory]
        if self.executor_memory is not None:
            options += ['--executor-memory', self.executor_memory]
        if self.executor_cores is not None:
            options += ['--executor-cores', self.executor_cores]
        if self.deploy_mode is not None:
            options += ['--deploy-mode', self.deploy_mode]
        if self.queue is not None:
            options += ['--queue', self.queue]
        if self.spark_master is not None:
            options += ['--master', self.spark_master]
        dependency_jars = self.dependency_jars()
        if dependency_jars != []:
            options += ['--jars', ','.join(dependency_jars)]
        args = [spark_submit] + options + self.spark_options() + \
            [self.jar()] + list(self.job_args())
        args = map(str, args)
        env = os.environ.copy()
        temp_stderr = tempfile.TemporaryFile()
        logger.info('Running: %s', repr(args))
        proc = subprocess.Popen(args, stdout=subprocess.PIPE,
                                stderr=temp_stderr, env=env, close_fds=True)
        return_code, final_state, app_id = self.track_progress(proc)
        if final_state == 'FAILED':
            raise SparkJobError('Spark job failed: see yarn logs for {0}'
                                .format(app_id))
        elif return_code != 0:
            temp_stderr.seek(0)
            errors = "".join(temp_stderr.readlines())
            logger.error(errors)
            raise SparkJobError('Spark job failed', err=errors)

    def track_progress(self, proc):
        """
        The Spark client currently outputs a multiline status to stdout every second while the application is running.

        This instead captures status data and updates a single line of output until the application finishes.
        """
        app_id = None
        app_status = 'N/A'
        url = 'N/A'
        final_state = None
        start = time.time()
        re_app_id = re.compile('application identifier: (\w+)')
        re_app_status = re.compile('yarnAppState: (\w+)')
        re_url = re.compile('appTrackingUrl: (.+)')
        re_final_state = re.compile('distributedFinalState: (\w+)')
        with SparkRunContext() as context:
            while proc.poll() is None:
                s = proc.stdout.readline()
                app_id_s = re_app_id.search(s)
                if app_id_s:
                    app_id = app_id_s.group(1)
                    context.app_id = app_id
                app_status_s = re_app_status.search(s)
                if app_status_s:
                    app_status = app_status_s.group(1)
                url_s = re_url.search(s)
                if url_s:
                    url = url_s.group(1)
                final_state_s = re_final_state.search(s)
                if final_state_s:
                    final_state = final_state_s.group(1)
                if not app_id:
                    logger.info(s.strip())
                else:
                    t_diff = time.time() - start
                    elapsed_mins, elapsed_secs = divmod(t_diff, 60)
                    status = ('[%0d:%02d] Status: %s Tracking: %s' %
                              (elapsed_mins, elapsed_secs, app_status, url))
                    sys.stdout.write("\r\x1b[K" + status)
                    sys.stdout.flush()
                self.spark_heartbeat(s, context)
        logger.info(proc.communicate()[0])
        return proc.returncode, final_state, app_id


class PySpark1xJob(Spark1xJob):

    num_executors = None
    driver_memory = None
    executor_memory = None
    executor_cores = None

    def program(self):
        raise NotImplementedError("subclass should define Spark .py file")

    def py_files(self):
        """
        Override to provide a list of py files.
        """
        return []

    def run(self):
        spark_submit = configuration.get_config().get('spark', 'spark-submit',
                                                      'spark-submit')
        options = ['--master', 'yarn-client']
        if self.num_executors is not None:
            options += ['--num-executors', self.num_executors]
        if self.driver_memory is not None:
            options += ['--driver-memory', self.driver_memory]
        if self.executor_memory is not None:
            options += ['--executor-memory', self.executor_memory]
        if self.executor_cores is not None:
            options += ['--executor-cores', self.executor_cores]
        py_files = self.py_files()
        if py_files != []:
            options += ['--py-files', ','.join(py_files)]
        args = [spark_submit] + options + self.spark_options() + \
            [self.program()] + list(self.job_args())
        args = map(str, args)
        env = os.environ.copy()
        temp_stderr = tempfile.TemporaryFile()
        logger.info('Running: %s', repr(args))
        proc = subprocess.Popen(args, stdout=subprocess.PIPE,
                                stderr=temp_stderr, env=env, close_fds=True)
        return_code, final_state, app_id = self.track_progress(proc)
        if final_state == 'FAILED':
            raise SparkJobError('Spark job failed: see yarn logs for %s', app_id)
        elif return_code != 0:
            temp_stderr.seek(0)
            errors = "".join(temp_stderr.readlines())
            logger.error(errors)
            raise SparkJobError('Spark job failed', err=errors)
