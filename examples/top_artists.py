import random
import luigi, luigi.hdfs, luigi.hadoop
import luigi.postgres
from heapq import nlargest
from collections import defaultdict

class Streams(luigi.ExternalTask):
    date = luigi.DateParameter()

    def output(self):
        return luigi.hdfs.HdfsTarget(self.date.strftime('data/streams_%Y-%m-%d.tsv'))

class FakeStreams(luigi.Task):
    date = luigi.DateParameter()

    def run(self):
        f = self.output().open('w')
        for i in xrange(1000):
            print >> f, random.randint(0, 999), random.randint(0, 999), random.randint(0, 999)
        f.close()

    def output(self):
        return luigi.LocalTarget(self.date.strftime('data/streams_%Y_%m_%d_faked.tsv'))

class AggregateArtists(luigi.Task):
    date_interval = luigi.DateIntervalParameter()
    fake_streams = luigi.BooleanParameter()

    def output(self):
        return luigi.LocalTarget("data/artist_streams_%s.tsv" % self.date_interval) # TODO: hdfs

    def requires(self):
        if self.fake_streams:
            return [FakeStreams(date) for date in self.date_interval]
        else:
            return [Streams(date) for date in self.date_interval]

    def run(self):
        artist_count = defaultdict(int)

        for input in self.input():
            with input.open('r') as in_file:
                for line in in_file:
                    timestamp, artist, track = line.strip().split()
                    artist_count[artist] += 1

        with self.output().open('w') as out_file:
            for artist, count in artist_count.iteritems():
                print >> out_file, artist, count

class AggregateArtistsHadoop(luigi.hadoop.JobTask):
    date_interval = luigi.DateIntervalParameter()
    fake_streams = luigi.BooleanParameter()

    def output(self):
        return luigi.LocalTarget("data/artist_streams_%s.tsv" % self.date_interval) # TODO: hdfs

    def requires(self):
        if self.fake_streams:
            return [FakeStreams(date) for date in self.date_interval]
        else:
            return [Streams(date) for date in self.date_interval]

    def mapper(self, line):
        timestamp, artist, track = line.strip().split()
        yield artist, 1
        
    def reducer(self, key, values):
        yield key, sum(values)

class Top10Artists(luigi.Task):
    date_interval = luigi.DateIntervalParameter()
    fake_streams = luigi.BooleanParameter()
    use_hadoop = luigi.BooleanParameter()

    def requires(self):
        if self.use_hadoop:
            return AggregateArtistsHadoop(self.date_interval, self.fake_streams)
        else:
            return AggregateArtists(self.date_interval, self.fake_streams)

    def output(self):
        return luigi.LocalTarget("data/top_artists_%s.tsv" % self.date_interval)

    def run(self):
        top_10 = nlargest(10, self._input_iterator())
        with self.output().open('w') as out_file:
            for streams, artist in top_10:
                print >> out_file, artist, streams

    def _input_iterator(self):
        with self.input().open('r') as in_file:
            for line in in_file:
                artist, streams = line.strip().split()
                yield date_interval.date_a, date_interval.date_b, dint(streams), artist

class ArtistToplistToDatabase(luigi.postgres.CopyToTable):
    date_interval = luigi.DateIntervalParameter()
    fake_streams = luigi.BooleanParameter()
    use_hadoop = luigi.BooleanParameter()

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
        return Top10Artists(self.date_interval, self.fake_streams, self.use_hadoop)

if __name__ == "__main__":
    luigi.run()
