import datetime, os
from spotify import luigi
from spotify.luigi.mock import MockFile
from spotify.util.test import *

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

class RecursionTest(TestCase):
    def setUp(self):
        MockFile._file_contents['/tmp/popularity/2009-01-01.txt'] = '0\n'

    def test_invoke(self):
        s = luigi.scheduler.LocalScheduler()
        s.add(Popularity(datetime.date(2010, 1, 1)))
        s.run()

        self.assertEquals(MockFile._file_contents['/tmp/popularity/2010-01-01.txt'], '365\n')

    def test_cmdline(self):
        luigi.run(['--local-scheduler', 'Popularity', '--date', '2010-01-01'])

        self.assertEquals(MockFile._file_contents['/tmp/popularity/2010-01-01.txt'], '365\n')
