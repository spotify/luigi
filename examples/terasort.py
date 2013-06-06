import logging
import os

import luigi
import luigi.hadoop_jar
import luigi.hdfs

logger = logging.getLogger('luigi-interface')


def hadoop_examples_jar():
    config = luigi.configuration.get_config()
    examples_jar = config.get('hadoop', 'examples-jar')
    if not examples_jar:
        logger.error("You must specify hadoop:examples-jar in client.cfg")
        raise
    if not os.path.exists(examples_jar):
        logger.error("Can't find example jar: " + examples_jar)
        raise
    return examples_jar


DEFAULT_TERASORT_IN = '/tmp/terasort-in'
DEFAULT_TERASORT_OUT = '/tmp/terasort-out'


class TeraGen(luigi.hadoop_jar.HadoopJarJobTask):
    """Runs TeraGen, by default with 1TB of data (10B records)"""
    records = luigi.Parameter(default="10000000000",
        description="Number of records, each record is 100 Bytes")
    terasort_in = luigi.Parameter(default=DEFAULT_TERASORT_IN,
        description="directory to store terasort input into.")

    def output(self):
        return luigi.hdfs.HdfsTarget(self.terasort_in)

    def jar(self):
        return hadoop_examples_jar()

    def main(self):
        return "teragen"

    def args(self):
        # First arg is 10B -- each record is 100bytes
        return [self.records, self.output()]


class TeraSort(luigi.hadoop_jar.HadoopJarJobTask):
    """Runs TeraGent, by default using """

    terasort_in = luigi.Parameter(default=DEFAULT_TERASORT_IN,
        description="directory to store terasort input into.")
    terasort_out = luigi.Parameter(default=DEFAULT_TERASORT_OUT,
        description="directory to store terasort output into.")

    def requires(self):
        return TeraGen(terasort_in=self.terasort_in)

    def output(self):
        return luigi.hdfs.HdfsTarget(self.terasort_out)

    def jar(self):
        return hadoop_examples_jar()

    def main(self):
        return "terasort"

    def args(self):
        return [self.input(), self.output()]


if __name__ == '__main__':
    luigi.run()
