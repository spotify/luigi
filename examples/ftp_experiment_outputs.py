import luigi
from luigi.contrib.ftp import RemoteTarget

HOST = "some_host"
USER = "user"
PWD = "some_password"
DIR = "/"


class ExperimentTask(luigi.ExternalTask):
    ''' This class represents something that was created elsewhere by an external process,
    so all we want to do is to implement the output method.
    '''
    def output(self):
        return RemoteTarget('/experiment/output1.txt', HOST, username=USER, password=PWD)

    def run(self):
        with self.output().open('w') as outfile:
            print >> outfile, "data 0 200 10 50 60"
            print >> outfile, "data 1 190 9 52 60"
            print >> outfile, "data 2 200 10 52 60"
            print >> outfile, "data 3 195 1 52 60"


class ProcessingTask(luigi.Task):
    ''' This class represents something that was created elsewhere by an external process,
    so all we want to do is to implement the output method.
    '''
    def requires(self):
        return ExperimentTask()

    def output(self):
        return luigi.LocalTarget('/tmp/processeddata.txt')

    def run(self):
        avg = 0.0
        elements = 0
        sumval = 0.0

        # Target objects are a file system/format abstraction and this will return a file stream object
        for line in self.input().open('r'):
            values = line.split(" ")
            avg += float(values[2])
            sumval += float(values[3])
            elements = elements + 1

        # average
        avg = avg / elements

        # save calculated values
        with self.output().open('w') as outfile:
            print >> outfile, avg, sumval


if __name__ == '__main__':
    luigi.run()
