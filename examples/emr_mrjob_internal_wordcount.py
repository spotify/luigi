import luigi
from luigi import s3
from luigi.hadoop_mrjob import MrJobTask


class InputTask(luigi.ExternalTask):

    def output(self):
        return s3.S3Target("s3://bucket/input.txt")


class MRWordFrequencyCount(MrJobTask):

    num_ec2_instances = '5'

    def requires(self):
        return InputTask()

    def output_dir(self):
        return s3.S3Target("s3://bucket/mr_output")

    def mapper(self, _, line):
        yield "chars", len(line)
        yield "words", len(line.split())
        yield "lines", 1

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    luigi.run(main_task_cls=MRWordFrequencyCount, cmdline_args=['--local-scheduler'])
