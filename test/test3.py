import datetime, os
from spotify import builder3

class EndSongSource(builder3.Task):
    date = builder3.Parameter()

    def output(self):
        return builder3.File('/var/endsongsource/%s.txt' % self.date)

    def run(self):
        pass

class TrackMap(builder3.Task):
    item_type = builder3.Parameter()

    def output(self):
        return builder3.File('/var/xyz/%s.txt' % self.item_type) # NOTE: this attribute must be public!

    def run(self):
        pass # create metadata by reading from a DB or whatever

class AggregatedPlays(builder3.Task):
    date = builder3.Parameter()

    def requires(self):
        return EndSongSource(self.date)

    def output(self):
        return builder3.File('/aggregatedplays/%s' % self.date.strftime('%Y-%m-%d'))

    def run(self):
        input = EndSongSource(self.date).output()
        pass # Hadoop job...

class TopList(builder3.Task):
    date = builder3.Parameter()
    item_type = builder3.Parameter(default = 'artist')

    def requires(self):
        self.__input = TrackMap(self.item_type)
        self.__aggregated_plays = AggregatedPlays(self.date)
        return [self.__input, self.__aggregated_plays]

    def output(self):
        self.outputs = {} # Outputs can be indexed in any way...

        for top in [100, 1000]:
            self.outputs[top] = builder3.File('/var/xyz/top_%d_%s_%s.txt' % (top, self.item_type, self.date.strftime('%Y-%m-%d')))

        return self.outputs

    def run(self):
        # blabla join data etc...
        # sort...

        for top, output_target in self.outputs.iteritems():
            f = output_target.open('w')
            for item in top_list[:top]:
                f.write(self.__line_format % item)
            f.close()

@builder3.expose
class TestTask(builder3.Task):
    item_type = builder3.Parameter()
    date = builder3.DateParameter()

    def requires(self):
        # Just trying something that depends on everything
        # Simple test to make sure everything is scheduled in the right order
        return [AggregatedPlays(self.date),
                TrackMap(self.item_type),
                EndSongSource(self.date),
                TopList(self.date)]

    def output(self):
        return [builder3.File('')]

if __name__ == '__main__':
    builder3.run()
