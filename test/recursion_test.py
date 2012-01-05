import datetime, os
import luigi
from luigi.mock import MockFile
import unittest

File = MockFile

@luigi.expose
class Popularity(luigi.Task):
    date = luigi.DateParameter(default = datetime.date.today() - datetime.timedelta(1))

    def output(self):
        return File('/tmp/popularity/%s.txt' % self.date.strftime('%Y-%m-%d'))

    def requires(self):
        return Popularity(self.date - datetime.timedelta(1))

    def run(self):
        f = self.output().open('w')
        for line in self.input().open('r'):
            print >> f, int(line.strip()) + 1
        
        f.close()

class RecursionTest(unittest.TestCase):
    def setUp(self):
        MockFile._file_contents['/tmp/popularity/2009-01-01.txt'] = '0\n'

    def test_invoke(self):
        w = luigi.worker.Worker(locally=True)
        w.add(Popularity(datetime.date(2010, 1, 1)))
        w.run()

        self.assertEquals(MockFile._file_contents['/tmp/popularity/2010-01-01.txt'], '365\n')

    def test_cmdline(self):
        luigi.run(['--local-scheduler', 'Popularity', '--date', '2010-01-01'])

        self.assertEquals(MockFile._file_contents['/tmp/popularity/2010-01-01.txt'], '365\n')
