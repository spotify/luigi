import unittest
import subprocess
import luigi
import luigi.hadoop, luigi.hdfs
from luigi.mock import MockFile
import StringIO

File = MockFile

class Words(luigi.Task):
    def output(self):
        return File('words')

    def run(self):
        f = self.output().open('w')
        f.write('kj kj lkj lkj ljoi j iljlk jlk jlk jk jkl jlk jlkj j ioj ioj kuh kjh\n')
        f.write('kjsfsdfkj sdjkf kljslkj flskjdfj jkkd jjfk jk jk jk jk jk jklkjf kj lkj lkj\n')
        f.close()

class TestJobTask(luigi.hadoop.JobTask):
    def job_runner(self):
        return luigi.hadoop.LocalJobRunner()

@luigi.expose
class WordCountJob(TestJobTask):
    def mapper(self, line):
        for word in line.strip().split():
            self.incr_counter('word', word, 1)
            yield word, 1

    def reducer(self, word, occurences):
        yield word, sum(occurences)

    def requires(self):
        return Words()

    def output(self):
        return File("luigitest")

@luigi.expose
class WordCountJobReal(WordCountJob):
    def job_runner(self):
        return luigi.hadoop.HadoopJobRunner(streaming_jar='test.jar')

@luigi.expose
class WordFreqJob(TestJobTask):
    def init_local(self):
        self.n = 0
        for line in self.input_local().open('r'):
            word, count = line.strip().split()
            self.n += int(count)

    def mapper(self, line):
        for word in line.strip().split():
            yield word, 1.0 / self.n

    def combiner(self, word, occurrences):
        yield word, sum(occurrences)

    def reducer(self, word, occurences):
        yield word, sum(occurences)

    def requires_local(self):
        return WordCountJob()

    def requires_hadoop(self):
        return Words()

    def output(self):
        return File("luigitest-2")

class HadoopJobTest(unittest.TestCase):
    def setUp(self):
        MockFile._file_contents = {}

    def read_output(self, p):
        count = {}
        for line in p.open('r'):
            k, v = line.strip().split()
            count[k] = v
        return count
        
    def test_run(self):
        luigi.run(['--local-scheduler', 'WordCountJob'])
        c = self.read_output(File('luigitest'))
        self.assertEquals(int(c['jk']), 6)

    def test_run_2(self):
        luigi.run(['--local-scheduler', 'WordFreqJob'])
        c = self.read_output(File('luigitest-2'))
        self.assertAlmostEquals(float(c['jk']), 6.0 / 33.0)

    def test_run_real(self):
        # Will attempt to run a real hadoop job, but we will secretly mock subprocess.Popen
        arglist_result = []
        def Popen_fake(arglist, stderr=None):
            arglist_result.append(arglist)
            class P(object):
                def wait(self): pass
            p = P()
            p.returncode = 0
            p.stderr = StringIO.StringIO()
            return p

        h, p = luigi.hdfs.HdfsTarget, subprocess.Popen            
        luigi.hdfs.HdfsTarget, subprocess.Popen = MockFile, Popen_fake
        MockFile.move = lambda *args: None

        WordCountJobReal().run()

        luigi.hdfs.HdfsTarget, subprocess.Popen = h, p # restore

        self.assertEquals(len(arglist_result), 1)
        self.assertEquals(arglist_result[0][0:3], ['hadoop', 'jar', 'test.jar'])

if __name__ == '__main__':
    HadoopJobTest.test_run_real()
