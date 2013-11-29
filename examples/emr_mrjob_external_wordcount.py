'''
Example on how to integrate external MRJob tasks

All hadoop functionality is included in the `emr_mrjob_wordcount.py` script
'''

import luigi
from luigi import s3
from luigi.hadoop_mrjob import MrJobExternalTask
from emr_mrjob_wordcount import MRWordFrequencyCount


class InputTask(luigi.ExternalTask):

    def output(self):
        return s3.S3Target("s3://bucket/input.txt")


class MRJob_WordFrequencyCount(MrJobExternalTask):

    mrjob_class = MRWordFrequencyCount
    num_ec2_instances = '10'

    def requires(self):
        return InputTask()

    def output_dir(self):
        return s3.S3Target("s3://bucket/mr_output")


if __name__ == '__main__':
    luigi.run()
