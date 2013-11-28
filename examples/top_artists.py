import random
import luigi, luigi.hdfs, luigi.hadoop
import luigi.postgres
from heapq import nlargest
from collections import defaultdict

class ExternalStreams(luigi.ExternalTask):
    ''' Example of a possible external data dump

    To depend on external targets (typically at the top of your dependency grpah), you can define
    an ExternalTask like this.
    '''
    date = luigi.DateParameter()

    def output(self):
        return luigi.hdfs.HdfsTarget(self.date.strftime('data/streams_%Y-%m-%d.tsv'))

class Streams(luigi.Task):
    ''' Faked version right now, just generates bogus data.    
    '''
    date = luigi.DateParameter()

    def run(self):
        f = self.output().open('w')
        for i in xrange(1000):
            print >> f, random.randint(0, 999), random.randint(0, 999), random.randint(0, 999)
        f.close()

    def output(self):
        return luigi.LocalTarget(self.date.strftime('data/streams_%Y_%m_%d_faked.tsv'))

class StreamsHdfs(Streams):
    def output(self):
        return luigi.hdfs.HdfsTarget(self.date.strftime('data/streams_%Y_%m_%d_faked.tsv'))

class AggregateArtists(luigi.Task):
    date_interval = luigi.DateIntervalParameter()

    def output(self):
        return luigi.LocalTarget("data/artist_streams_%s.tsv" % self.date_interval)

    def requires(self):
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

    def output(self):
        return luigi.hdfs.HdfsTarget(
            "data/artist_streams_%s.tsv" % self.date_interval, 
            format = luigi.hdfs.PlainDir
        )

    def requires(self):
        return [StreamsHdfs(date) for date in self.date_interval]

    def mapper(self, line):
        timestamp, artist, track = line.strip().split()
        yield artist, 1
        
    def reducer(self, key, values):
        yield key, sum(values)

class Top10Artists(luigi.Task):
    date_interval = luigi.DateIntervalParameter()
    use_hadoop = luigi.BooleanParameter()

    def requires(self):
        if self.use_hadoop:
            return AggregateArtistsHadoop(self.date_interval)
        else:
            return AggregateArtists(self.date_interval)

    def output(self):
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
                out_file.write(out_line + '\n')

    def _input_iterator(self):
        with self.input().open('r') as in_file:
            for line in in_file:
                artist, streams = line.strip().split()
                yield int(streams), artist

class ArtistToplistToDatabase(luigi.postgres.CopyToTable):
    date_interval = luigi.DateIntervalParameter()
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
        return Top10Artists(self.date_interval, self.use_hadoop)


if __name__ == "__main__":
    luigi.run()
