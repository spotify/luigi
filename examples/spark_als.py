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

import random

import luigi
import luigi.format
import luigi.hdfs
from luigi.contrib.spark import SparkJob


class UserItemMatrix(luigi.Task):

    #: the size of the data being generated
    data_size = luigi.IntParameter()

    def run(self):
        """
        Generates :py:attr:`~.UserItemMatrix.data_size` elements.
        Writes this data in \ separated value format into the target :py:func:`~/.UserItemMatrix.output`.

        The data has the following elements:

        * `user` is the default Elasticsearch id field,
        * `track`: the text,
        * `rating`: the day when the data was created.

        """
        w = open(self.output(), 'w')
        for user in xrange(self.data_size):
            track = int(random * self.data_size)
            w.write('%d\%d\%f' % (user, track, 1.0))
        w.close()

    def output(self):
        """
        Returns the target output for this task.
        In this case, a successful execution of this task will create a file in HDFS.

        :return: the target output for this task.
        :rtype: object (:py:class:`~luigi.target.Target`)
        """
        return luigi.hdfs.HdfsTarget('data-matrix', format=luigi.format.Gzip)


class SparkALS(SparkJob):
    """
    This task runs a :py:class:`luigi.contrib.spark.SparkJob` task
    over the target data returned by :py:meth:`~/.UserItemMatrix.output` and
    writes the result into its :py:meth:`~.SparkALS.output` target (a file in HDFS).

    This class uses :py:meth:`luigi.contrib.spark.SparkJob.run`.
    """

    data_size = luigi.IntParameter(default=1000)
    spark_workers = '100'
    spark_master_memory = '2g'
    spark_worker_memory = '3g'

    def requires(self):
        """
        This task's dependencies:

        * :py:class:`~.UserItemMatrix`

        :return: object (:py:class:`luigi.task.Task`)
        """
        return UserItemMatrix(self.data_size)

    def jar(self):
        # Jar containing job_class.
        return 'my-spark-assembly.jar'

    def job_class(self):
        # The name of the Spark job object.
        return 'com.spotify.spark.ImplicitALS'

    def job_args(self):
        # These are passed to the Spark main args in the defined order.
        return ['yarn-standalone', self.input().path, self.output().path]

    def output(self):
        """
        Returns the target output for this task.
        In this case, a successful execution of this task will create a file in HDFS.

        :return: the target output for this task.
        :rtype: object (:py:class:`~luigi.target.Target`)
        """
        # The corresponding Spark job outputs as GZip format.
        return luigi.hdfs.HdfsTarget(
            '%s/als-output/*' % self.item_type, format=luigi.format.Gzip)


'''
// Corresponding example Spark Job, a wrapper around the MLLib ALS job.
// This class would have to be jarred into my-spark-assembly.jar
// using sbt assembly (or package) and made available to the Luigi job
// above.

package com.spotify.spark

import org.apache.spark._
import org.apache.spark.mllib.recommendation.{Rating, ALS}
import org.apache.hadoop.io.compress.GzipCodec

object ImplicitALS {

  def main(args: Array[String]) {
    val sc = new SparkContext(args(0), "ImplicitALS")
    val input = args(1)
    val output = args(2)

    val ratings = sc.textFile(input)
      .map { l: String =>
        val t = l.split('\t')
        Rating(t(0).toInt, t(1).toInt, t(2).toFloat)
      }

    val model = ALS.trainImplicit(ratings, 40, 20, 0.8, 150)
    model
      .productFeatures
      .map { case (id, vec) =>
        id + "\t" + vec.map(d => "%.6f".format(d)).mkString(" ")
      }
      .saveAsTextFile(output, classOf[GzipCodec])

    sc.stop()
  }
}
'''
