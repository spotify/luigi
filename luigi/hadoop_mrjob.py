'''
Add basic support to EMR using MRJob
'''
import abc
import os

import luigi
from luigi import s3
from luigi import configuration

from mrjob.job import MRJob


class MrJobExternalTask(luigi.Task):

    aws_access_key_id = ''
    aws_secret_access_key = ''
    ec2_instance_type = 'm1.small'
    num_ec2_instances = '1'

    @abc.abstractproperty
    def mrjob_class(self):
        return None

    def output(self):
        fnames = ["part_%.5i" % i for i in range(int(self.num_ec2_instances))]
        return [s3.S3Target(os.path.join(self.output_dir().path, fname)) for fname in fnames]

    def run(self):
        if not self.mrjob_class:
            raise Exception("No MRJob class specified")
        if not isinstance(self.mrjob_class(), MRJob):
            raise Exception("Class is not instance of MRJob")
        if not self.aws_access_key_id:
            self.aws_access_key_id = configuration.get_config().get('emr', 'aws_access_key_id')
            if not self.aws_access_key_id:
                raise Exception('No AWS Access Key available')
        if not self.aws_secret_access_key:
            self.aws_secret_access_key = configuration.get_config().get('emr', 'aws_secret_access_key')
            if not self.aws_secret_access_key:
                raise Exception('No AWS Secret Key available')

        # TODO: support more options:
        # http://pythonhosted.org/mrjob/guides/configs-reference.html#additional-options-for-emrjobrunner
        args = [self.input().path, '-r', 'emr']
        args += ['--ec2-instance-type', self.ec2_instance_type]
        args += ['--num-ec2-instances', self.num_ec2_instances]
        args += ['--output-dir', self.output_dir().path, '--no-output']
        job = self.mrjob_class(args=args)

        # Very dirty hack to pass the same AWS credentials from the luigi config file
        # Why? So the user doesn't need to configure boto or mrjob, only luigi
        # Problem: Need to pass to the EMRJobRunner.__init__() the kwargs `aws_access_key_id` and `aws_access_key_id`
        # The problem is those variables are not in the `MRJobLauncher.options`
        #    Note this open issue: https://github.com/Yelp/mrjob/issues/782
        # Maybe is possible to add the options at runtime, see on `launch.py` line 401:
        #    self.options, args = self.option_parser.parse_args(args)
        # This method is actually recommended on the MRJob docs on `launch.py` on `def emr_job_runner_kwargs(self)`:
        #    Re-define this if you want finer control when running jobs on EMR.
        all_options = job.emr_job_runner_kwargs()
        all_options['aws_access_key_id'] = self.aws_access_key_id
        all_options['aws_secret_access_key'] = self.aws_secret_access_key
        def overwrite_emr_job_runner_kwargs():
            return all_options
        job.emr_job_runner_kwargs = overwrite_emr_job_runner_kwargs
        # End of Hack

        with job.make_runner() as runner:
            runner.run()
