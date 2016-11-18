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
from collections import defaultdict
from heapq import nlargest

from luigi import six

import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
import luigi.postgres


class ExternalStreams(luigi.ExternalTask):
    """
    Example of a possible external data dump

    To depend on external targets (typically at the top of your dependency graph), you can define
    an ExternalTask like this.
    """
    date = luigi.DateParameter()

    def output(self):
        """
        Returns the target output for this task.
        In this case, it expects a file to be present in HDFS.

        :return: the target output for this task.
        :rtype: object (:py:class:`luigi.target.Target`)
        """
        return luigi.contrib.hdfs.HdfsTarget(self.date.strftime('data/streams_%Y-%m-%d.tsv'))


class Streams(luigi.Task):
    """
    Faked version right now, just generates bogus data.
    """
    date = luigi.DateParameter()

    def run(self):
        """
        Generates bogus data and writes it into the :py:meth:`~.Streams.output` target.
        """
        with self.output().open('w') as output:
            for _ in range(1000):
                output.write('{} {} {}\n'.format(
                    random.randint(0, 999),
                    random.randint(0, 999),
                    random.randint(0, 999)))

    def output(self):
        """
        Returns the target output for this task.
        In this case, a successful execution of this task will create a file in the local file system.

        :return: the target output for this task.
        :rtype: object (:py:class:`luigi.target.Target`)
        """
        return luigi.LocalTarget(self.date.strftime('data/streams_%Y_%m_%d_faked.tsv'))


class StreamsHdfs(Streams):
    """
    This task performs the same work as :py:class:`~.Streams` but its output is written to HDFS.

    This class uses :py:meth:`~.Streams.run` and
    overrides :py:meth:`~.Streams.output` so redefine HDFS as its target.
    """

    def output(self):
        """
        Returns the target output for this task.
        In this case, a successful execution of this task will create a file in HDFS.

        :return: the target output for this task.
        :rtype: object (:py:class:`luigi.target.Target`)
        """
        return luigi.contrib.hdfs.HdfsTarget(self.date.strftime('data/streams_%Y_%m_%d_faked.tsv'))


class AggregateArtists(luigi.Task):
    """
    This task runs over the target data returned by :py:meth:`~/.Streams.output` and
    writes the result into its :py:meth:`~.AggregateArtists.output` target (local file).
    """

    date_interval = luigi.DateIntervalParameter()

    def output(self):
        """
        Returns the target output for this task.
        In this case, a successful execution of this task will create a file on the local filesystem.

        :return: the target output for this task.
        :rtype: object (:py:class:`luigi.target.Target`)
        """
        return luigi.LocalTarget("data/artist_streams_{}.tsv".format(self.date_interval))

    def requires(self):
        """
        This task's dependencies:

        * :py:class:`~.Streams`

        :return: list of object (:py:class:`luigi.task.Task`)
        """
        return [Streams(date) for date in self.date_interval]

    def run(self):
        artist_count = defaultdict(int)

        for t in self.input():
            with t.open('r') as in_file:
                for line in in_file:
                    _, artist, track = line.strip().split()
                    artist_count[artist] += 1

        with self.output().open('w') as out_file:
            for artist, count in six.iteritems(artist_count):
                out_file.write('{}\t{}\n'.format(artist, count))


class AggregateArtistsHadoop(luigi.contrib.hadoop.JobTask):
    """
    This task runs a :py:class:`luigi.contrib.hadoop.JobTask` task
    over each target data returned by :py:meth:`~/.StreamsHdfs.output` and
    writes the result into its :py:meth:`~.AggregateArtistsHadoop.output` target (a file in HDFS).

    This class uses :py:meth:`luigi.contrib.spark.SparkJob.run`.
    """

    date_interval = luigi.DateIntervalParameter()

    def output(self):
        """
        Returns the target output for this task.
        In this case, a successful execution of this task will create a file in HDFS.

        :return: the target output for this task.
        :rtype: object (:py:class:`luigi.target.Target`)
        """
        return luigi.contrib.hdfs.HdfsTarget(
            "data/artist_streams_%s.tsv" % self.date_interval,
            format=luigi.contrib.hdfs.PlainDir
        )

    def requires(self):
        """
        This task's dependencies:

        * :py:class:`~.StreamsHdfs`

        :return: list of object (:py:class:`luigi.task.Task`)
        """
        return [StreamsHdfs(date) for date in self.date_interval]

    def mapper(self, line):
        """
        The implementation of the map phase of the Hadoop job.

        :param line: the input.
        :return: tuple ((key, value) or, in this case, (artist, 1 stream count))
        """
        _, artist, _ = line.strip().split()
        yield artist, 1

    def reducer(self, key, values):
        """
        The implementation of the reducer phase of the Hadoop job.

        :param key: the artist.
        :param values: the stream count.
        :return: tuple (artist, count of streams)
        """
        yield key, sum(values)


class Top10Artists(luigi.Task):
    """
    This task runs over the target data returned by :py:meth:`~/.AggregateArtists.output` or
    :py:meth:`~/.AggregateArtistsHadoop.output` in case :py:attr:`~/.Top10Artists.use_hadoop` is set and
    writes the result into its :py:meth:`~.Top10Artists.output` target (a file in local filesystem).
    """

    date_interval = luigi.DateIntervalParameter()
    use_hadoop = luigi.BoolParameter()

    def requires(self):
        """
        This task's dependencies:

        * :py:class:`~.AggregateArtists` or
        * :py:class:`~.AggregateArtistsHadoop` if :py:attr:`~/.Top10Artists.use_hadoop` is set.

        :return: object (:py:class:`luigi.task.Task`)
        """
        if self.use_hadoop:
            return AggregateArtistsHadoop(self.date_interval)
        else:
            return AggregateArtists(self.date_interval)

    def output(self):
        """
        Returns the target output for this task.
        In this case, a successful execution of this task will create a file on the local filesystem.

        :return: the target output for this task.
        :rtype: object (:py:class:`luigi.target.Target`)
        """
        return luigi.LocalTarget("data/top_artists_%s.tsv" % self.date_interval)

    def run(self):
        top_10 = nlargest(10, self._input_iterator())
        with self.output().open('w') as out_file:
            for streams, artist in top_10:
                out_line = '\t'.join([
                    str(self.date_interval.date_a),
                    str(self.date_interval.date_b),
                    artist,
                    str(streams)
                ])
                out_file.write((out_line + '\n'))

    def _input_iterator(self):
        with self.input().open('r') as in_file:
            for line in in_file:
                artist, streams = line.strip().split()
                yield int(streams), artist


class ArtistToplistToDatabase(luigi.postgres.CopyToTable):
    """
    This task runs a :py:class:`luigi.postgres.CopyToTable` task
    over the target data returned by :py:meth:`~/.Top10Artists.output` and
    writes the result into its :py:meth:`~.ArtistToplistToDatabase.output` target which,
    by default, is :py:class:`luigi.postgres.PostgresTarget` (a table in PostgreSQL).

    This class uses :py:meth:`luigi.postgres.CopyToTable.run` and :py:meth:`luigi.postgres.CopyToTable.output`.
    """

    date_interval = luigi.DateIntervalParameter()
    use_hadoop = luigi.BoolParameter()

    host = "localhost"
    database = "toplists"
    user = "luigi"
    password = "abc123"  # ;)
    table = "top10"

    columns = [("date_from", "DATE"),
               ("date_to", "DATE"),
               ("artist", "TEXT"),
               ("streams", "INT")]

    def requires(self):
        """
        This task's dependencies:

        * :py:class:`~.Top10Artists`

        :return: list of object (:py:class:`luigi.task.Task`)
        """
        return Top10Artists(self.date_interval, self.use_hadoop)


if __name__ == "__main__":
    luigi.run()
