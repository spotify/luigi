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

import logging
import os
import sys
import tempfile
import shutil
import importlib
import tarfile
try:
    import cPickle as pickle
except ImportError:
    import pickle

from luigi import six
from luigi.contrib.external_program import ExternalProgramTask
from luigi import configuration

logger = logging.getLogger('luigi-interface')


class SparkSubmitTask(ExternalProgramTask):
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

    # Only log stderr if spark fails (since stderr is normally quite verbose)
    always_log_stderr = False

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
    def packages(self):
        return self._list_config(configuration.get_config().get("spark", "packages", None))

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

    def program_environment(self):
        return self.get_environment()

    def program_args(self):
        return self.spark_command() + self.app_command()

    def spark_command(self):
        command = [self.spark_submit]
        command += self._text_arg('--master', self.master)
        command += self._text_arg('--deploy-mode', self.deploy_mode)
        command += self._text_arg('--name', self.name)
        command += self._text_arg('--class', self.entry_class)
        command += self._list_arg('--jars', self.jars)
        command += self._list_arg('--packages', self.packages)
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

    def _list_config(self, config):
        if config and isinstance(config, six.string_types):
            return list(map(lambda x: x.strip(), config.split(',')))

    def _dict_config(self, config):
        if config and isinstance(config, six.string_types):
            return dict(map(lambda i: i.split('=', 1), config.split('|')))

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
                command += [name, '{0}={1}'.format(prop, value)]
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

    def program_args(self):
        return self.spark_command() + self.app_command()

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
        with self.no_unpicklable_properties():
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
