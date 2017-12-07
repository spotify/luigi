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

import luigi
import luigi.contrib.hadoop_jar
import luigi.contrib.hdfs

logger = logging.getLogger('luigi-interface')


def hadoop_examples_jar():
    config = luigi.configuration.get_config()
    examples_jar = config.get('hadoop', 'examples-jar')
    if not examples_jar:
        logger.error("You must specify hadoop:examples-jar in luigi.cfg")
        raise
    if not os.path.exists(examples_jar):
        logger.error("Can't find example jar: " + examples_jar)
        raise
    return examples_jar


DEFAULT_TERASORT_IN = '/tmp/terasort-in'
DEFAULT_TERASORT_OUT = '/tmp/terasort-out'


class TeraGen(luigi.contrib.hadoop_jar.HadoopJarJobTask):
    """
    Runs TeraGen, by default with 1TB of data (10B records)
    """

    records = luigi.Parameter(default="10000000000",
                              description="Number of records, each record is 100 Bytes")
    terasort_in = luigi.Parameter(default=DEFAULT_TERASORT_IN,
                                  description="directory to store terasort input into.")

    def output(self):
        """
        Returns the target output for this task.
        In this case, a successful execution of this task will create a file in HDFS.

        :return: the target output for this task.
        :rtype: object (:py:class:`~luigi.target.Target`)
        """
        return luigi.contrib.hdfs.HdfsTarget(self.terasort_in)

    def jar(self):
        return hadoop_examples_jar()

    def main(self):
        return "teragen"

    def args(self):
        # First arg is 10B -- each record is 100bytes
        return [self.records, self.output()]


class TeraSort(luigi.contrib.hadoop_jar.HadoopJarJobTask):
    """
    Runs TeraGent, by default using
    """

    terasort_in = luigi.Parameter(default=DEFAULT_TERASORT_IN,
                                  description="directory to store terasort input into.")
    terasort_out = luigi.Parameter(default=DEFAULT_TERASORT_OUT,
                                   description="directory to store terasort output into.")

    def requires(self):
        """
        This task's dependencies:

        * :py:class:`~.TeraGen`

        :return: object (:py:class:`luigi.task.Task`)
        """
        return TeraGen(terasort_in=self.terasort_in)

    def output(self):
        """
        Returns the target output for this task.
        In this case, a successful execution of this task will create a file in HDFS.

        :return: the target output for this task.
        :rtype: object (:py:class:`~luigi.target.Target`)
        """
        return luigi.contrib.hdfs.HdfsTarget(self.terasort_out)

    def jar(self):
        return hadoop_examples_jar()

    def main(self):
        return "terasort"

    def args(self):
        return [self.input(), self.output()]
