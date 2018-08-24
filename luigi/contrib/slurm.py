# -*- coding: utf-8 -*-
#
# Copyright 2012-2017 Spotify AB
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

"""
Slurm batch system Tasks.

Adapted by Jimmy Tang <jtang@voysis.com> from the luigi/contrib/sge.py by Jake Feala (@jfeala)

Slurm is a job scheduler used to allocate compute resources on a shared cluster. Jobs are submitted using
the ``sbatch`` command and monitored using ``scontrol``. To get started, install Luigi on all nodes.

To run Luigi workflows on an Slurm cluster, subclass :class:`luigi.contrib.slurm.SlurmTask` as you
would any :class:`luigi.Task`, but override the ``work()`` method (instead of ``run()``) to define the
job code. Then, run your Luigi workflow from the master node, assigning > 1 Luigi ``workers`` in order to
distribute the tasks in parallel across the cluster.

This extension is modeled after the hadoop.py approach.

The procedure:
- Pickle the class
- Optionally tarball the Luigi module and current working directory.
- Construct a sbatch argument that runs a generic runner function with the path to the pickled class.
- Runner function loads the class from pickle (and extracts the tarball, if using) and runs the work() method.

To request resources from Slurm, set a list of sbatch parameters in the ``sbatch_params`` argument.
It is the user's responsibility to ensure that these parameters will constitute a valid sbatch command.
Example:
sbatch_params: [
    "--job-name=ExampleJob",
    "--time=1:00:00",
    "--ntasks=1",
    "--mem-per-cpu=500",
    "--cpus-per-task=4"
            ]

NB: for the most part, this plugin assumes that all machines in the cluster are using a shared file system.
The ``tarball`` parameter can be used to send the current working directory and Luigi module to the remote
machine. If you need code that is not in Luigi or the standard library, however, this will not help.
"""

import itertools
import logging
import os
import pprint
import random
import shutil
import subprocess
import sys
import time
try:
    import cPickle as pickle
except ImportError:
    import pickle

import luigi
from luigi.contrib.hadoop import create_packages_archive
from luigi.contrib import remote_runner

logger = logging.getLogger('luigi-interface')


def retry(delays=(0, 1, 5, 30, 180, 600, 3600), exception=Exception, report=lambda *args: None):
    """
    See: http://code.activestate.com/recipes/580745-retry-decorator-in-python/
    """

    def wrapper(function):
        def wrapped(*args, **kwargs):
            problems = []
            for delay in itertools.chain(delays, [None]):
                try:
                    return function(*args, **kwargs)
                except exception as problem:
                    problems.append(problem)
                    if delay is None:
                        report("retryable failed definitely:", problems)
                        raise
                    else:
                        report("retryable failed:", problem, "-- delaying for %ds" % delay)
                        time.sleep(delay)

        return wrapped

    return wrapper


@retry()
def _parse_job_state(job_id):
    """
    Parse "state" from 'scontrol show jobid=ID -o' output.

    Returns state for the scontrol output, Returns 'u' if `scontrol`
    output is empty or job_id is not found.
    """
    job_out = subprocess.check_output(['scontrol', '-o', 'show', "jobid={}".format(job_id)]).decode()
    job_line = job_out.split()
    job_map = {}
    for job in job_line:
        job_s = job.split("=")
        try:
            job_map[job_s[0]] = job_s[1]
        except Exception as e:
            logger.error("Exception: {}, No value found for {}".format(e, job_s[0]))

    return job_map.get('JobState', 'u')


def _build_submit_command(sbatch_params, cmd, outfile, errfile, sbatchfile):
    """
    Submit shell command to Slurm queue via `sbatch`.
    """
    sbatch_template = """#!/bin/bash
    {cmd}"""

    submit_cmd = ['sbatch', '--parsable', '-o', '{outfile}', '-e', '{errfile}']

    for param in sbatch_params:
        submit_cmd.extend([param])

    submit_cmd.append('{sbatchfile}')
    submit_template = ' '.join(submit_cmd)

    with open(sbatchfile, 'w') as fp:
        fp.write(sbatch_template.format(cmd=cmd))

    return submit_template.format(
        sbatch_template=sbatch_template, outfile=outfile, errfile=errfile, sbatchfile=sbatchfile
    )


@retry()
def _run_sbatch_command(submit_cmd):
    return subprocess.check_output(submit_cmd, shell=True)


class SlurmTask(luigi.Task):
    """
    Base class for executing a job on Slurm.

    Override ``work()`` (rather than ``run()``) with your job code.

    Parameters:
    - sbatch_params: List of sbatch parameters which will be added to the sbatch
        command. It is up to the user to ensure that this results in a valid sbatch
        command. See the docstring at the top of the file for an example of this.
    - shared_tmp_dir: Shared drive accessible from all nodes in the cluster.
        Task classes and dependencies are pickled to a temporary folder on
        this drive. The default is ``/home``.
    - run_locally: Run locally instead of on the cluster.
    - poll_time: The length of time to wait in order to poll the job.
    - remove_tmp_dir: Remove the temporary directory.
    - tarball: Create a tarball of the Luigi project directory and current working
        directory for execution on the remote machine. Note that no other dependencies
        will be copied over; if you're using anything other than the standard library
        or Luigi it will fail. Otherwise we assume a shared file system.
    """

    sbatch_params = luigi.ListParameter(
        significant=False,
        description="List of parameters which will be added to the sbatch command",
    )
    shared_tmp_dir = luigi.Parameter(default='/home', significant=False)
    run_locally = luigi.BoolParameter(
        significant=False, description="Run locally instead of on the cluster"
    )
    poll_time = luigi.IntParameter(
        default=15,
        significant=False,
        description="Specify the wait time to poll scontrol for the job status",
    )
    remove_tmp_dir = luigi.BoolParameter(
        default=True,
        significant=False,
        description="Delete the temporary directory used (for debugging)",
    )
    tarball = luigi.BoolParameter(
        default=False,
        significant=False,
        description="Tarball the working directory and Luigi module and extract on the remote machine",
    )

    def __init__(self, *args, **kwargs):
        super(SlurmTask, self).__init__(*args, **kwargs)

        # Call input() here so upstream Task objects are pickled (exposed via input() method below).
        try:
            upstream = luigi.Task.input(self)
        except Exception as e:
            upstream = None
            logger.error("Exception: {}".format(e))

        self.my_upstream = upstream

    def input(self):
        """
        Overwrite self.input() so the contents are pickled alongside the class
        (see init method).
        """
        return self.my_upstream

    def __str__(self):
        return '\n'.join([
            pprint.pformat(vars(self), indent=2)
        ])

    def _fetch_task_failures(self):
        """
        Read the file containing the task stderr.
        """
        if not os.path.exists(self.errfile):
            logger.info('No error file')
            return []

        with open(self.errfile, 'r') as f:
            errors = f.readlines()

        if errors == []:
            return errors

        if errors[0].strip() == 'stdin: is not a tty':
            errors.pop(0)

        return errors

    def _fetch_task_out(self):
        """
        Read the file containing the task stdout.
        """
        if not os.path.exists(self.outfile):
            logger.info('No output file')
            return []

        with open(self.outfile, 'r') as f:
            output = f.readlines()

        return output

    def _fetch_logger_output(self):
        """
        Read the file containing the task logger output.
        """
        logger_file = os.path.join(self.tmp_dir, "runner.log")
        if not os.path.exists(logger_file):
            logger.info('No logger file')
            return []

        with open(logger_file, 'r') as f:
            output = f.readlines()

        return output

    def _init_local(self):
        """
        Set up a temporary directory with the code to be run.
        """
        # Set up temp folder in shared directory (trim to max filename length)
        base_tmp_dir = self.shared_tmp_dir
        random_id = '%016x' % random.getrandbits(64)
        self.tmp_dir = os.path.join(base_tmp_dir, '{}-{}'.format(self.task_id, random_id))
        max_filename_length = os.fstatvfs(0).f_namemax
        self.tmp_dir = self.tmp_dir[:max_filename_length]
        logger.info('tmp_dir: {}'.format(self.tmp_dir))
        os.makedirs(self.tmp_dir)

        # Dump the code to be run into a pickle file
        logging.debug('Dumping pickled class to {}'.format(self.tmp_dir))
        self._dump(self.tmp_dir)

        if self.tarball:
            # Grab Luigi and the code in the current path.
            logger.debug("Creating tarball")
            packages = [luigi] + [__import__(self.__module__, None, None, 'dummy')]
            create_packages_archive(packages, os.path.join(self.tmp_dir, "packages.tar"))

    def run(self):
        self.init_vars()
        if self.run_locally:
            self.work()
        else:
            self._init_local()
            self._run_job()

    def init_vars(self):
        """
        This can be used to initialise variables that won't be available in the Slurm environment,
        e.g. information from other Luigi tasks. Save them in object variables so that they are
        serialised before work() is called.
        """
        pass

    def work(self):
        """
        Override this method, rather than ``run()``,  for your actual work.
        """
        pass

    def _dump(self, out_dir=''):
        """
        Dump instance to file.
        """
        with self.no_unpicklable_properties():
            self.job_file = os.path.join(out_dir, 'job-instance.pickle')

            if self.__module__ == '__main__':
                d = pickle.dumps(self, 0).decode()
                module_name = os.path.basename(sys.argv[0]).rsplit('.', 1)[0]
                d = d.replace('(c__main__', "(c" + module_name)
                open(self.job_file, "w").write(d)
            else:
                pickle.dump(self, open(self.job_file, "wb"))

    def _run_job(self):
        # Build a sbatch argument that will run remote_runner.py on the directory we've specified
        runner_path = remote_runner.__file__
        if runner_path.endswith("pyc"):
            runner_path = runner_path[:-1]

        job_cmd = 'python {0} "{1}" "{2}" "{3}"'.format(
                runner_path,
                self.tmp_dir,
                os.getcwd(),
                os.path.join(self.tmp_dir, "runner.log"))
        if not self.tarball:
            job_cmd += ' "--no-tarball"'

        # Build sbatch file and submit command
        self.outfile = os.path.join(self.tmp_dir, 'job.out')
        self.errfile = os.path.join(self.tmp_dir, 'job.err')
        sbatchfile = os.path.join(self.tmp_dir, '{}.sbatch'.format(self.task_family))
        submit_cmd = _build_submit_command(
            self.sbatch_params, job_cmd, self.outfile, self.errfile, sbatchfile
        )
        logger.debug('sbatch command: {}'.format(submit_cmd))

        # Submit the job and get the job ID.
        cwd = os.getcwd()
        os.chdir(self.tmp_dir)
        output = _run_sbatch_command(submit_cmd)
        os.chdir(cwd)
        self.job_id = output.decode().strip()
        logger.debug("Submitted job to Slurm with job id: {}".format(self.job_id))

        successful = self._track_job()

        if not successful:
            raise RuntimeError('Slurm job did not complete')

        if self.remove_tmp_dir:
            logger.info('Removing temporary directory {}'.format(self.tmp_dir))
            if os.path.exists(self.tmp_dir):
                shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def _print_logger_output(self, output):
        print(output)

    def _track_job(self):
        """
        Track the job state in the Slurm scheduler and log the output when it terminates.
        """
        successful = False
        start = time.time()
        while True:
            job_status = _parse_job_state(self.job_id)
            if job_status == 'RUNNING' or job_status == 'COMPLETING':
                logger.info(
                    'Job is running ({:0.1f} seconds elapsed)...'.format(float(time.time() - start))
                )
            elif job_status == 'PENDING':
                logger.info(
                    'Job is pending ({:0.1f} seconds elapsed)...'.format(float(time.time() - start))
                )
            elif 'FAILED' in job_status:
                logger.error(
                    'Job has FAILED:\n'
                    + '\n'.join(self._fetch_task_failures())
                    + '\n'.join(self._fetch_task_out())
                )
                self._print_logger_output("\n".join(self._fetch_logger_output()))
                break
            elif 'CANCELLED' in job_status:
                logger.error(
                    'Job has been CANCELLED:\n'
                    + '\n'.join(self._fetch_task_failures())
                    + '\n'.join(self._fetch_task_out())
                )
                break
                self._print_logger_output("".join(self._fetch_logger_output()))
            elif job_status == 'COMPLETED' or job_status == 'u':
                # Then the job could either be failed or done.
                successful = True  # fail properly if you want to stop, don't just write to stderr!
                errors = self._fetch_task_failures()
                if not errors:
                    logger.info('Job is done')
                else:
                    for error in errors:
                        logger.error(error)
                self._print_logger_output("".join(self._fetch_logger_output()))
                break
            else:
                logger.info('Job status is UNKNOWN!')
                logger.info('Status is : {}'.format(job_status))
                raise Exception(
                    "job status isn't one of ['RUNNING', 'PENDING', 'COMPLETED', 'FAILED', 'CANCELLED', 'u']: %s"
                    % job_status
                )
            time.sleep(self.poll_time)

        return successful


class LocalSlurmTask(SlurmTask):
    """
    A local version of SlurmTask, for easier debugging.

    This version skips the ``sbatch`` steps and simply runs ``work()``
    on the local node, so you don't need to be on a Slurm cluster to
    use your Task in a test workflow.
    """

    def run(self):
        self.work()
