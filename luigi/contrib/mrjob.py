# -*- coding: utf-8 -*-
#
# Copyright 2010 bit.ly, 2016 enreach.me
#
# Original author Daniel Frank, modified into luigi.contrib by Jyry Suvilehto
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
""" MRJob wrapper for Luigi

This package provides a convenient Luigi wrapper for using `MRJob
<http://packages.python.org/mrjob/>`_ steps in workflows. See `here
<https://pythonhosted.org/mrjob/guides/runners.html#running-your-job-programmatically>`_
for the feature used to implement this functionality.

The wrapper doesn't attempt to be complete or to hide any complexity. The user
is expected to know their way around MRJob and to configure it separately.

.. code-block:: python

    import luigi
    from luigi.contrib.mrjob import MRJobTask
    import mrjob

    class ExternalInputTask(luigi.ExternalTask):
        def output(self):
            return luigi.LocalTarget(os.path.join(self.destination, "input"))]



    class MRJobWordCountTask(MRJobTask):
        destination = luigi.Parameter()

        job_cls = mrjob.examples.mr_word_freq_count.MRWordFreqCount

        def mrjob_opts(self):
            return {
                '-r', 'inline'
                }

        def requires(self):
            return ExternalInputTask()

        def output(self):
            return luigi.LocalTarget(
                                    os.path.join(self.destination, "output"),
                                    format=luigi.format.Nop
                                    )

"""

import abc
from itertools import chain
import os
import sys

import luigi
from luigi import Task
from luigi.hdfs import HdfsTarget
from luigi.s3 import S3Target


import logging

logger = logging.getLogger('luigi-mrjob')


def _to_opts(flag, value):
    """ parse a key-value pair to an option or options that can be in the
        command line to mrjob
    """
    if isinstance(value, bool):
        return [flag] if value else []
    elif isinstance(value, dict):
        # this is intended to be used for jobconf option
        # probably needs to be upgraded to handle other jobconf-like dicts
        return list(chain(*[(flag, '%s=%s' % (k, v)) for k, v in value.items()]))
    else:
        assert isinstance(value, str)
        return [flag, value]


def _get_mrjob_friendly_path(target):
    """ Ensure target has the s3:// or hdfs:// required by MRJob.
        May be unnecessary as at least S3Target also requires the
        path to be a parseable url.

    """
    path = target.path
    remote = False
    if isinstance(target, HdfsTarget) and not path.startswith('hdfs://'):
        path = 'hdfs://' + path
    if isinstance(target, S3Target) and not path.startswith('s3://'):
        path = 's3://' + path
    if path.startswith('s3://') or path.startswith('hdfs://'):
        remote = True
    return path, remote


class MRJobTask(Task):
    ''' Class that runs a MRJob from a Luigi workflow.
    '''
    __metaclass__ = abc.ABCMeta

    @property
    def job_cls(self):
        '''
        Override this to return the right mrjob class (not instance)
        '''
        raise NotImplementedError

    @abc.abstractmethod
    def requires(self):
        """ The that can be shimmied to  supported are as follows:

            - one input, will be passed as the command line input
            - a list of inputs, all passed as command line paths
            - a dict that contains input under key "input". others are passed
              as command line parameters. e.g.
              {"input": ExternalTask(...),
              "--custom-extra-file": ExternalTask(..)}

            These will be parsed into command line input directives for MRJob.
            The last one is intended so that it is possible for jobs to depend
            on files that won't be passed as regular input, e.g. lookup
            databases, pickled classifier objects or similar that will be
            distributed to each mapper node separately using MRJob's included
            batteries.

            Note that most MRJob runners don't really handle moving things to
            and fro between filesystems. EMR and Hadoop runners can move input
            data from local file system to S3 or HDFS respectively.
            Fortunately there's this tool called :py:mod:`luigi` that can
            help you transfer data from one place to another.
            """
        pass

    @abc.abstractmethod
    def output(self):
        """ Output formats supported by MRJob:
            - directory in local filesystem
            - directory in HDFS
            - directory in S3

            Custom implemented output formats:

            - `a single file` in local filesystem (this is a custom feature)

        The last one is implemented using
        `mrjob.Runner.MRJobRunner.stream_output()`. Because said method
        returns bytes and not strings and luigi expects strings it is best to
        just pass format.Nop as the format in this case.

        *Note:* on HDFS or S3 if the last step of the workflow fails after
        producing output then the target directory will have been created.
        MRJob is quite adamant about not proceeding if the directory exists
        already. Manual intervention is then required to delete the partial
        output. Improvements that handle this in a smart way especially on
        S3 are welcome.
        """
        pass

    def mrjob_opts(self):
        """ Override this to pass command line options. You probably want to
        pass a config file ( or make one available at the standard locations
        MRJob reads ) and the runner type.
        """
        return {}

    def handle_counters(self, counters):
        """ Override this if you want to do something with the counters
        returned by `mrjob.runner.MRJobRunner.counters()`.
        """
        return

    def __init__(self, *args, **kwargs):
        super(MRJobTask, self).__init__(*args, **kwargs)
        self.opts = self.make_mrjob_opts(*args, **kwargs)

    def _hack_std_stream_buffers(self):
        """ MRJob is very insistent about fiddling around with standard
        streams which breaks if there are stringios as input and output as
        there are in integration tests.
        """
        logger.debug("type of sys.stdin is %s" % type(sys.stdin))
        if sys.version_info < (3, 0):
            logger.debug("on python 2, not attempting hack")
        else:
            logger.debug("on python 3, attempting hack")
            # the real sys.stdin and sys.stdout have these on py3
            # but the wrappers created by testing lib don't
            if not hasattr(sys.stdin, "buffer"):
                sys.stdin.buffer = None
            if not hasattr(sys.stdout, "buffer"):
                sys.stdout.buffer = None
            if not hasattr(sys.stderr, "buffer"):
                sys.stderr.buffer = None

    def run(self):
        self._hack_std_stream_buffers()
        job = self.job_cls(args=self.opts)
        with job.make_runner() as jobrunner:
            jobrunner.run()
            # if user wanted output in a local file
            if self.output_is_local_file:
                with self.output().open('w') as output:
                    output.writelines(jobrunner.stream_output())
            self.handle_counters(jobrunner.counters())

    def make_mrjob_opts(self, *args, **kwargs):
        """ Makes a list of command line parameters to pass to the MRJob class
        when instantiating. It makes sense to extend this behaviour e.g. if
        you have some dynamic inputs that need to be passed as command line parameters.
        """
        base_opts = self.mrjob_opts()
        base_opts.update((k, v) for k, v in kwargs.items() if k.startswith('-'))
        opts_list = list(chain(*[_to_opts(k, v) for k, v in base_opts.items()]))
        # now add special params: input and ouput
        # yikes, existence of path field not really enforced
        input_ = self.input()
        file_options = []
        if isinstance(input_, dict):
            file_opts = input_
            input_ = input_['input']
            del file_opts['input']
            for option, values in file_opts.items():
                for value in values:
                    file_options.append(option)
                    file_options.append(_get_mrjob_friendly_path(value)[0])
        if not isinstance(input_, list):
            input_ = [input_]
        input_files = [
                        _get_mrjob_friendly_path(in_)[0] for in_ in
                        input_
                       ]
        output_dir, output_remote = _get_mrjob_friendly_path(self.output())

        assert input_files and output_dir, \
            'Must define output and dependencies or direct input'
        # if it looks like output should go to a directory, add --no-output
        # to avoid unnecessary streaming of potentially ginormous output
        if (not output_remote and os.path.dirname(output_dir)+os.sep == output_dir)\
                or output_remote:
            self.output_is_local_file = False
            opts_list += ['--no-output']
            return opts_list + file_options + input_files + ['-o', output_dir]
        elif not output_remote:
            # if user wants the output to be a local file, enable streaming of output
            # so we can write the file in runner
            self.output_is_local_file = True
            return opts_list + file_options + input_files


class LocalMRJobTarget(luigi.file.LocalTarget):
    """ The local analogue of :py:class:`~luigi.s3.S3FlagTarget` in case you want to
    run or test jobs locally. Unlike Hadoop, MRJob local runners don't create
    a _SUCCESS file but they do copy the files to their target more or less
    atomically so the presence of any file in the target should indicate that
    a job has been completed.


    The copy from a tempdir happens once all steps are completed, as
    atomically as moving several files in the local filesystem can take place.
    LocalMRJobTarget assumes that if one file in the directory exists, the job
    is complete.
    """

    def __init__(self, path, format=None, flag="_SUCCESS"):
        self.path = path
        if path[-1] != os.sep:
            raise ValueError("LocalMRJobTarget requires the path to be to a"
                             "directory.  It must end with a slash ( / ).")
        if format is None:
            format = luigi.format.get_default_format()
        super(LocalMRJobTarget, self).__init__(path=path, format=format,
                                               is_tmp=False)

    def exists(self):
        """ We take the presence of any file in the target dir to mean that
        the step has been concluded.
        """
        generator = self.fs.listdir(self.path)
        try:
            next(generator)
            return True
        except StopIteration:
            return False

    def open(self, mode="r"):
        if mode == "w":
            raise ValueError(("Writing to a directory with no filename is"
                              " not currently supported! (Feel free to add an"
                              " implementation though)"))
        elif mode != "r":
            raise ValueError("Unsupported mode open '%s'" % mode)
        else:
            files = self.fs.listdir(self.path)
            command = ["cat"]
            command.extend([f for f in files])
            file_reader = luigi.format.InputPipeProcessWrapper(command)
            if self.format:
                return self.format.pipe_reader(file_reader)
            else:
                return file_reader


class LocalMRJobTask(luigi.task.ExternalTask):
    """
    An external task that requires a Hadoop-like flag directory (sans flag)
    exists in the local filesystem.

    Good for testing workflows. You should be able to replace this with an
    S3FlagTask when running production flows in EMR. Keep in mind that
    S3FlagTarget doesn't appear to support open() like LocalMRJobTarget does,
    though.
    """
    path = luigi.Parameter()

    def output(self):
        return LocalMRJobTarget(self.path)
