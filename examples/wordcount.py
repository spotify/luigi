import luigi


class InputText(luigi.ExternalTask):
    ''' This class represents something that was created elsewhere by an external process,
    so all we want to do is to implement the output method.
    '''
    date = luigi.DateParameter()
    def output(self):
        return luigi.LocalTarget(self.date.strftime('/var/tmp/text/%Y-%m-%d.txt'))

class WordCount(luigi.Task):
    date_interval = luigi.DateIntervalParameter()

    def requires(self):
        return [InputText(date) for date in self.date_interval.dates()]

    def output(self):
        return luigi.LocalTarget('/var/tmp/text-count/%s' % self.date_interval)

    def run(self):
        count = {}
        for file in self.input(): # The input() method is a wrapper around requires() that returns Target objects
            for line in file.open('r'): # Target objects are a file system/format abstraction and this will return a file stream object
                for word in line.strip().split():
                    count[word] = count.get(word, 0) + 1

        # output data
        f = self.output().open('w')
        for word, count in count.iteritems():
            f.write("%s\t%d\n" % (word, count))
        f.close() # Note that this is essential because file system operations are atomic

if __name__ == '__main__':
    luigi.run(main_task_cls=WordCount)
