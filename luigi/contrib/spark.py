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
import shutil
import importlib
import tarfile
try:
    import cPickle as pickle
except ImportError:
    import pickle
import warnings

from luigi import six
import luigi
import luigi.format
import luigi.hdfs
from luigi import configuration

logger = logging.getLogger('luigi-interface')


class SparkRunContext(object):

    def __init__(self, proc):
        self.proc = proc

    def __enter__(self):
        self.__old_signal = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, self.kill_job)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is KeyboardInterrupt:
            self.kill_job()
        signal.signal(signal.SIGTERM, self.__old_signal)

    def kill_job(self, captured_signal=None, stack_frame=None):
        self.proc.kill()
        if captured_signal is not None:
            # adding 128 gives the exit code corresponding to a signal
            sys.exit(128 + captured_signal)


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


class SparkSubmitTask(luigi.Task):
    """
    Template task for running a Spark job

    Supports running jobs on Spark local, standalone, Mesos or Yarn

    See http://spark.apache.org/docs/latest/submitting-applications.html
    for more information

    """

    # Application (.jar or .py file)
    name = None
    entry_class = None
    app = None

    def app_options(self):
        """
        Subclass this method to map your task parameters to the app's arguments

        """
        return []

    @property
    def spark_submit(self):
        return configuration.get_config().get('spark', 'spark-submit', 'spark-submit')

    @property
    def master(self):
        return configuration.get_config().get("spark", "master", None)

    @property
    def deploy_mode(self):
        return configuration.get_config().get("spark", "deploy-mode", None)

    @property
    def jars(self):
        return self._list_config(configuration.get_config().get("spark", "jars", None))

    @property
    def py_files(self):
        return self._list_config(configuration.get_config().get("spark", "py-files", None))

    @property
    def files(self):
        return self._list_config(configuration.get_config().get("spark", "files", None))

    @property
    def conf(self):
        return self._dict_config(configuration.get_config().get("spark", "conf", None))

    @property
    def properties_file(self):
        return configuration.get_config().get("spark", "properties-file", None)

    @property
    def driver_memory(self):
        return configuration.get_config().get("spark", "driver-memory", None)

    @property
    def driver_java_options(self):
        return configuration.get_config().get("spark", "driver-java-options", None)

    @property
    def driver_library_path(self):
        return configuration.get_config().get("spark", "driver-library-path", None)

    @property
    def driver_class_path(self):
        return configuration.get_config().get("spark", "driver-class-path", None)

    @property
    def executor_memory(self):
        return configuration.get_config().get("spark", "executor-memory", None)

    @property
    def driver_cores(self):
        return configuration.get_config().get("spark", "driver-cores", None)

    @property
    def supervise(self):
        return bool(configuration.get_config().get("spark", "supervise", False))

    @property
    def total_executor_cores(self):
        return configuration.get_config().get("spark", "total-executor-cores", None)

    @property
    def executor_cores(self):
        return configuration.get_config().get("spark", "executor-cores", None)

    @property
    def queue(self):
        return configuration.get_config().get("spark", "queue", None)

    @property
    def num_executors(self):
        return configuration.get_config().get("spark", "num-executors", None)

    @property
    def archives(self):
        return self._list_config(configuration.get_config().get("spark", "archives", None))

    @property
    def hadoop_conf_dir(self):
        return configuration.get_config().get("spark", "hadoop-conf-dir", None)

    def get_environment(self):
        env = os.environ.copy()
        hadoop_conf_dir = self.hadoop_conf_dir
        if hadoop_conf_dir:
            env['HADOOP_CONF_DIR'] = hadoop_conf_dir
        return env

    def spark_command(self):
        command = [self.spark_submit]
        command += self._text_arg('--master', self.master)
        command += self._text_arg('--deploy-mode', self.deploy_mode)
        command += self._text_arg('--name', self.name)
        command += self._text_arg('--class', self.entry_class)
        command += self._list_arg('--jars', self.jars)
        command += self._list_arg('--py-files', self.py_files)
        command += self._list_arg('--files', self.files)
        command += self._list_arg('--archives', self.archives)
        command += self._dict_arg('--conf', self.conf)
        command += self._text_arg('--properties-file', self.properties_file)
        command += self._text_arg('--driver-memory', self.driver_memory)
        command += self._text_arg('--driver-java-options', self.driver_java_options)
        command += self._text_arg('--driver-library-path', self.driver_library_path)
        command += self._text_arg('--driver-class-path', self.driver_class_path)
        command += self._text_arg('--executor-memory', self.executor_memory)
        command += self._text_arg('--driver-cores', self.driver_cores)
        command += self._flag_arg('--supervise', self.supervise)
        command += self._text_arg('--total-executor-cores', self.total_executor_cores)
        command += self._text_arg('--executor-cores', self.executor_cores)
        command += self._text_arg('--queue', self.queue)
        command += self._text_arg('--num-executors', self.num_executors)
        return command

    def app_command(self):
        if not self.app:
            raise NotImplementedError("subclass should define an app (.jar or .py file)")
        return [self.app] + self.app_options()

    def run(self):
        args = list(map(str, self.spark_command() + self.app_command()))
        logger.info('Running: %s', repr(args))
        tmp_stdout, tmp_stderr = tempfile.TemporaryFile(), tempfile.TemporaryFile()
        proc = subprocess.Popen(args, stdout=tmp_stdout, stderr=tmp_stderr,
                                env=self.get_environment(), close_fds=True,
                                universal_newlines=True)
        try:
            with SparkRunContext(proc):
                while proc.poll() is None:
                    pass
            logger.info(proc.communicate()[0])
            tmp_stdout.seek(0)
            stdout = "".join(map(lambda s: s.decode('utf-8'), tmp_stdout.readlines()))
            logger.info("Spark job stdout:\n{0}".format(stdout))
            if proc.returncode != 0:
                tmp_stderr.seek(0)
                stderr = "".join(map(lambda s: s.decode('utf-8'), tmp_stderr.readlines()))
                raise SparkJobError('Spark job failed {0}'.format(repr(args)), out=stdout, err=stderr)
        finally:
            tmp_stderr.close()
            tmp_stdout.close()

    def _list_config(self, config):
        if config and isinstance(config, six.string_types):
            return list(map(lambda x: x.strip(), config.split(',')))

    def _dict_config(self, config):
        if config and isinstance(config, six.string_types):
            return dict(map(lambda i: i.split('='), config.split('|')))

    def _text_arg(self, name, value):
        if value:
            return [name, value]
        return []

    def _list_arg(self, name, value):
        if value and isinstance(value, (list, tuple)):
            return [name, ','.join(value)]
        return []

    def _dict_arg(self, name, value):
        command = []
        if value and isinstance(value, dict):
            for prop, value in value.items():
                command += [name, '"{0}={1}"'.format(prop, value)]
        return command

    def _flag_arg(self, name, value):
        if value:
            return [name]
        return []


class PySparkTask(SparkSubmitTask):
    """
    Template task for running an inline PySpark job

    Simply implement the ``main`` method in your subclass

    You can optionally define package names to be distributed to the cluster
    with ``py_packages`` (uses luigi's global py-packages configuration by default)

    """

    # Path to the pyspark program passed to spark-submit
    app = os.path.join(os.path.dirname(__file__), 'pyspark_runner.py')
    # Python only supports the client deploy mode, force it
    deploy_mode = "client"

    @property
    def name(self):
        return self.__class__.__name__

    @property
    def py_packages(self):
        packages = configuration.get_config().get('spark', 'py-packages', None)
        if packages:
            return map(lambda s: s.strip(), packages.split(','))

    def setup(self, conf):
        """
        Called by the pyspark_runner with a SparkConf instance that will be used to instantiate the SparkContext

        :param conf: SparkConf
        """

    def setup_remote(self, sc):
        self._setup_packages(sc)

    def main(self, sc, *args):
        """
        Called by the pyspark_runner with a SparkContext and any arguments returned by ``app_options()``

        :param sc: SparkContext
        :param args: arguments list
        """
        raise NotImplementedError("subclass should define a main method")

    def app_command(self):
        return [self.app, self.run_pickle] + self.app_options()

    def run(self):
        self.run_path = tempfile.mkdtemp(prefix=self.name)
        self.run_pickle = os.path.join(self.run_path, '.'.join([self.name.replace(' ', '_'), 'pickle']))
        with open(self.run_pickle, 'wb') as fd:
            self._dump(fd)
        try:
            super(PySparkTask, self).run()
        finally:
            shutil.rmtree(self.run_path)

    def _dump(self, fd):
        if self.__module__ == '__main__':
            d = pickle.dumps(self)
            module_name = os.path.basename(sys.argv[0]).rsplit('.', 1)[0]
            d = d.replace(b'(c__main__', "(c" + module_name)
            fd.write(d)
        else:
            pickle.dump(self, fd)

    def _setup_packages(self, sc):
        """
        This method compresses and uploads packages to the cluster

        """
        packages = self.py_packages
        if not packages:
            return
        for package in packages:
            mod = importlib.import_module(package)
            try:
                mod_path = mod.__path__[0]
            except AttributeError:
                mod_path = mod.__file__
            tar_path = os.path.join(self.run_path, package + '.tar.gz')
            tar = tarfile.open(tar_path, "w:gz")
            tar.add(mod_path, os.path.basename(mod_path))
            tar.close()
            sc.addPyFile(tar_path)


class SparkJob(luigi.Task):
    """
    .. deprecated:: 1.1.1
       Use ``SparkSubmitTask`` or ``PySparkTask`` instead.

    """
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
        warnings.warn("The use of SparkJob is deprecated. Please use SparkSubmitTask or PySparkTask.", stacklevel=2)
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
            errors = "".join((x.decode('utf8') for x in temp_stderr.readlines()))
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
        with SparkRunContext(proc) as context:
            while proc.poll() is None:
                s = proc.stdout.readline().decode('utf8')
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


class Spark1xBackwardCompat(SparkSubmitTask):
    """
    Adapts SparkSubmitTask interface to (Py)Spark1xJob interface

    """
    # Old interface
    @property
    def master(self):
        return configuration.get_config().get("spark", "master", "yarn-client")

    def output(self):
        raise NotImplementedError("subclass should define an output target")

    def spark_options(self):
        return []

    def dependency_jars(self):
        return []

    def job_args(self):
        return []

    # New interface
    @property
    def jars(self):
        return self.dependency_jars()

    def app_options(self):
        return self.job_args()

    def spark_command(self):
        return super(Spark1xBackwardCompat, self).spark_command() + self.spark_options()


class Spark1xJob(Spark1xBackwardCompat):
    """
    .. deprecated:: 1.1.1
       Use ``SparkSubmitTask`` or ``PySparkTask`` instead.

    """
    # Old interface
    def job_class(self):
        raise NotImplementedError("subclass should define Spark job_class")

    def jar(self):
        raise NotImplementedError("subclass should define jar containing job_class")

    # New interface
    @property
    def entry_class(self):
        return self.job_class()

    @property
    def app(self):
        return self.jar()

    def run(self):
        warnings.warn("The use of Spark1xJob is deprecated. Please use SparkSubmitTask or PySparkTask.", stacklevel=2)
        return super(Spark1xJob, self).run()


class PySpark1xJob(Spark1xBackwardCompat):
    """

    .. deprecated:: 1.1.1
       Use ``SparkSubmitTask`` or ``PySparkTask`` instead.

    """

    # Old interface
    def program(self):
        raise NotImplementedError("subclass should define Spark .py file")

    # New interface
    @property
    def app(self):
        return self.program()

    def run(self):
        warnings.warn("The use of PySpark1xJob is deprecated. Please use SparkSubmitTask or PySparkTask.", stacklevel=2)
        return super(PySpark1xJob, self).run()
