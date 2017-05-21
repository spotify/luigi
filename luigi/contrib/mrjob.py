'''
Add basic support to EMR using MRJob
'''
from __future__ import absolute_import
import os
import re
import imp
import inspect
import tempfile

import luigi
from luigi import s3
from luigi import configuration

from mrjob.job import MRJob


class MrJobExternalTask(luigi.Task):
    '''
    This class takes a external MRJob file and integrates into a luigi pipeline.

    Example: `examples/mrjob/external_wordcont.py`
    '''

    mrjob_class = None

    aws_access_key_id = ''
    aws_secret_access_key = ''
    ec2_instance_type = 'm1.small'
    num_ec2_instances = '1'

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

        '''
        Very dirty hack to pass the same AWS credentials from the luigi config file
        Why? So the user doesn't need to configure boto or mrjob, only luigi

        Problem:
        We need to pass to the EMRJobRunner.__init__() the kwargs `aws_access_key_id` and `aws_access_key_id`
        The problem is those variables are not in the `MRJobLauncher.options`
           Note this MRJob open issue: https://github.com/Yelp/mrjob/issues/782
        Maybe is possible to add the options at runtime, see on `launch.py` line 401:
           self.options, args = self.option_parser.parse_args(args)

        This method is actually recommended on the MRJob docs on `launch.py` on `def emr_job_runner_kwargs(self)`:
           Quote: Re-define this if you want finer control when running jobs on EMR.
        '''
        all_options = job.emr_job_runner_kwargs()
        all_options['aws_access_key_id'] = self.aws_access_key_id
        all_options['aws_secret_access_key'] = self.aws_secret_access_key

        def overwrite_emr_job_runner_kwargs():
            return all_options
        job.emr_job_runner_kwargs = overwrite_emr_job_runner_kwargs
        # End of Hack

        with job.make_runner() as runner:
            runner.run()


class MrJobTask(MrJobExternalTask):
    '''
    This is the main class to use with EMR.
    Is possible to define the mapper and reducer funcions as a regular luigi.hadoop class.

    This class will create an external `.py` file that MRJob will ship to EMR.

    After creating the file is going to use `MrJobExternalTask`

    Example: `examples/mrjob/internal_wordcount.py`
    '''

    def run(self):
        '''
        Going to create a temp file as a MRJob regular file based on self
        And call MRJob.run() on that file.
        '''
        lines = inspect.getsource(self.__class__).splitlines()

        # Fix the import and extend class
        source_line1 = lines[0]
        get_class_name_regex = 'class ([a-zA-Z0-9]+)\(MrJobTask\):'
        match = re.match(get_class_name_regex, source_line1)
        if match and len(match.groups()) > 0:
            class_name = match.groups()[0]
            first_lines = ['from mrjob.job import MRJob']
            first_lines += ['class {0}(MRJob):'.format(class_name)]
            lines = first_lines + lines[1:]
        else:
            raise Exception('Couldn\'t find class name')

        # Add __main__
        lines += ['if __name__ == \'__main__\':', '', '    MRWordFrequencyCount.run()', '']

        # Remove luigi specific methods
        # There is probably a cleaner way of doing this
        methods = ['requires', 'output']
        lines_to_remove = []
        removing = False
        for i, line in enumerate(lines):
            # TODO: regexp
            if not removing:
                # Reached a new method, check if have to remove
                if any([' def {0}'.format(method) in line for method in methods]):
                    removing = True
            else:
                # Reached a new method, check again
                if ' def ' in line:
                    if any([' def {0}'.format(method) in line for method in methods]):
                        removing = True
                    else:
                        removing = False

            if removing:
                lines_to_remove.append(i)
        lines = [line for i, line in enumerate(lines) if i not in lines_to_remove]

        # Create a temp file
        tf = tempfile.NamedTemporaryFile(delete=False)
        tf.write('\n'.join(lines))
        tf.close()  # Write contents

        # Import the temp file as a temp module
        temp_module = imp.load_source('temp_module', tf.name)
        # Get the MRJob class
        self.mrjob_class = temp_module.__dict__[class_name]

        super(MrJobTask, self).run()

        # Delete temp file
        os.unlink(tf.name)
