import luigi, luigi.hdfs
import random
import string

class InputText(luigi.Task):
    ''' Generate random 'text'
    '''
    date = luigi.DateParameter()
    hdfs = luigi.BooleanParameter(default=False)

    def output(self):
        if self.hdfs:
            return luigi.hdfs.HdfsTarget(self.date.strftime('/tmp/text/%Y-%m-%d.txt'))
        else:
            return luigi.LocalTarget(self.date.strftime('/var/tmp/text/%Y-%m-%d.txt'))

    def run(self):
        f = self.output().open('w')

        def random_word():
            return ''.join([random.choice(string.ascii_letters) for i in xrange(random.randrange(1, 3))])

        def random_line():
            return [random_word() for i in xrange(random.randrange(10, 20))]

        for line in xrange(random.randrange(0, 1000)):
            f.write(' '.join(random_line()) + '\n')

        f.close()

@luigi.expose_main
class MultipleInputText(luigi.Task):
    date_interval = luigi.DateIntervalParameter()
    hdfs = luigi.BooleanParameter(default=False)

    def requires(self):
        return [InputText(date, self.hdfs) for date in self.date_interval.dates()]

if __name__ == '__main__':
    luigi.run()
