import luigi
import time
import os
import shutil


@luigi.expose
class Foo(luigi.Task):
    def run(self):
        print "Running Foo"

    def requires(self):
        return (Bar(i) for i in xrange(10))


@luigi.expose
class Bar(luigi.Task):
    num = luigi.IntParameter()

    def run(self):
        time.sleep(1)
        self.output().open('w').close()

    def output(self):
        return luigi.LocalTarget('/tmp/bar/%d' % self.num)


if __name__ == "__main__":
    if os.path.exists('/tmp/bar'):
        shutil.rmtree('/tmp/bar')

    luigi.run(['--task', 'Foo', '--workers', '5'], use_optparse=True)
