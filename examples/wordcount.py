import luigi
import spotify.luigi

class Words(luigi.ExternalTask):
    def output(self):
        return spotify.luigi.HdfsTarget("words.avro")

class WordCount(spotify.luigi.hadoop.JobTask):
    def mapper(self, line):
        for word in line.strip().split():
            self.incr_counter("words,%s" % word, 1)
            yield word, 1
        
    def reducer(self, word, occurrences):
        yield word, sum(occurrences)

    def output(self):
        return spotify.luigi.HdfsTarget("luigitest")
        
    def requires(self):
        return Words()
    
    def on_complete(self):
        counters = self.counters()
        assert counters["words"]["hej"] == 1
        assert counters["words"]["jag"] < 2

if __name__ == "__main__":
    WordCount().run()

