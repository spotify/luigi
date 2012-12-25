import luigi
import time
import os
import shutil


class MyExternal(luigi.ExternalTask):
    def complete(self):
        return False


class Foo(luigi.Task):
    def run(self):
        print "Running Foo"

    def requires(self):
#        yield MyExternal()
        for i in xrange(10):
            yield Bar(i)


class Bar(luigi.Task):
    num = luigi.IntParameter()

    def run(self):
        time.sleep(1)
        self.output().open('w').close()

    def output(self):
        time.sleep(1)
        return luigi.LocalTarget('/tmp/bar/%d' % self.num)


if __name__ == "__main__":
    if os.path.exists('/tmp/bar'):
        shutil.rmtree('/tmp/bar')

    luigi.run(['--task', 'Foo', '--workers', '2'], use_optparse=True)
