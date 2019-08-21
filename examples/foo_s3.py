import luigi
from luigi.contrib.s3 import S3Target


class Foo(luigi.Task):
    task_namespace = 'examples'

    def run(self):
        print("Running Foo that outputs S3Target")

    def output(self):
        return S3Target("s3://ryft-public-sample-data/hoge")
