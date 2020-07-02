#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2012-2020 Spotify AB
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
The pyspark program.

This module will be run by spark-submit for PySparkTask jobs.

The first argument is a path to the pickled instance of the PySparkTask,
other arguments are the ones returned by PySparkTask.app_options()

"""

import abc
import logging
import os
import pickle
import sys

from luigi import configuration

# this prevents the modules in the directory of this script from shadowing global packages
sys.path.append(sys.path.pop(0))


class _SparkEntryPoint(metaclass=abc.ABCMeta):
    def __init__(self, conf):
        self.conf = conf

    @abc.abstractmethod
    def __enter__(self):
        pass

    @abc.abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class SparkContextEntryPoint(_SparkEntryPoint):
    sc = None

    def __enter__(self):
        from pyspark import SparkContext
        self.sc = SparkContext(conf=self.conf)
        return self.sc, self.sc

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.sc.stop()


class SparkSessionEntryPoint(_SparkEntryPoint):
    spark = None

    def _check_major_spark_version(self):
        from pyspark import __version__ as spark_version
        major_version = int(spark_version.split('.')[0])
        if major_version < 2:
            raise RuntimeError(
                '''
                Apache Spark {} does not support SparkSession entrypoint.
                Try to set 'pyspark_runner.use_spark_session' to 'False' and switch to old-style syntax
                '''.format(spark_version)
            )

    def __enter__(self):
        self._check_major_spark_version()
        from pyspark.sql import SparkSession
        self.spark = SparkSession \
            .builder \
            .config(conf=self.conf) \
            .enableHiveSupport() \
            .getOrCreate()

        return self.spark, self.spark.sparkContext

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.spark.stop()


class AbstractPySparkRunner(object):
    _entry_point_class = None

    def __init__(self, job, *args):
        # Append job directory to PYTHON_PATH to enable dynamic import
        # of the module in which the class resides on unpickling
        sys.path.append(os.path.dirname(job))
        with open(job, "rb") as fd:
            self.job = pickle.load(fd)
        self.args = args

    def run(self):
        from pyspark import SparkConf
        conf = SparkConf()
        self.job.setup(conf)
        with self._entry_point_class(conf=conf) as (entry_point, sc):
            self.job.setup_remote(sc)
            self.job.main(entry_point, *self.args)


def _pyspark_runner_with(name, entry_point_class):
    return type(name, (AbstractPySparkRunner,), {'_entry_point_class': entry_point_class})


PySparkRunner = _pyspark_runner_with('PySparkRunner', SparkContextEntryPoint)
PySparkSessionRunner = _pyspark_runner_with('PySparkSessionRunner', SparkSessionEntryPoint)


def _use_spark_session():
    return bool(configuration.get_config().get('pyspark_runner', "use_spark_session", False))


def _get_runner_class():
    if _use_spark_session():
        return PySparkSessionRunner
    return PySparkRunner


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARN)
    _get_runner_class()(*sys.argv[1:]).run()
