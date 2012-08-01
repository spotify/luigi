import luigi
from luigi.mock import MockFile
import unittest
from luigi.util import Copy

File = MockFile

class A(luigi.Task):
    date = luigi.DateParameter()

    def output(self):
        return File(self.date.strftime('/tmp/data-%Y-%m-%d.txt'))

    def run(self):
        f = self.output().open('w')
        print >>f, 'hello, world'
        f.close()

@luigi.expose
class ACopy(Copy(A)):
    def output(self):
        return File(self.date.strftime('/tmp/copy-data-%Y-%m-%d.txt'))

class UtilTest(unittest.TestCase):
    def test_a(self):
        luigi.run(['--local-scheduler', 'ACopy', '--date', '2012-01-01'])
        self.assertEqual(MockFile._file_contents['/tmp/data-2012-01-01.txt'], 'hello, world')

if __name__ == '__main__':
    luigi.run()
