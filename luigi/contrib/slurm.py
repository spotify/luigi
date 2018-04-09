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

Adapted by Jimmy Tang <jtang@voysis.com> from the sge.py by Jake Feala (@jfeala)
Further adapted by Nathan Tsoi <nathan@vertile.com>

Adapted by Jake Feala (@jfeala) from
`LSF extension <https://github.com/dattalab/luigi/blob/lsf/luigi/lsf.py>`_
by Alex Wiltschko (@alexbw)
Maintained by Jake Feala (@jfeala)

Slurm is a job scheduler used to allocate compute resources on a
shared cluster. Jobs are submitted using the ``sbatch`` command and monitored
using ``scontrol``. To get started, install luigi on all nodes.

To run luigi workflows on an Slurm cluster, subclass
:class:`luigi.contrib.slurm.SlurmTask` as you would any :class:`luigi.Task`,
but override the ``work()`` method, instead of ``run()``, to define the job
code. Then, run your Luigi workflow from the master node, assigning > 1
``workers`` in order to distribute the tasks in parallel across the cluster.

The following is an example usage (and can also be found in ``slurm_tests.py``)

.. code-block:: python

    import logging
    import luigi
    import os
    from luigi.contrib.slurm import SlurmTask

    logger = logging.getLogger('luigi-interface')


    class TestJobTask(SlurmTask):

        i = luigi.Parameter()

        def work(self):
            logger.info('Running test job...')
            with open(self.output().path, 'w') as f:
                f.write('this is a test')

        def output(self):
            return luigi.LocalTarget(os.path.join('/home', 'testfile_' + str(self.i)))


    if __name__ == '__main__':
        tasks = [TestJobTask(i=str(i), ntasks=i+1) for i in range(3)]
        luigi.build(tasks, local_scheduler=True, workers=3)


The ``ntasks`` parameter allows you to define different compute
resource requirements for each task. In this example, the third Task
asks for 3 CPU slots. If your cluster only contains nodes with 2
CPUs, this task will hang indefinitely in the queue. See the docs for
:class:`luigi.contrib.slurm.SlurmTask` for other Slurm parameters. As
for any task, you can also set these in your luigi configuration file
as shown below.

    [SlurmTask]
    shared-tmp-dir = /home
    ntasks = 2

"""

# This extension is modeled after the hadoop.py approach.
#
# Implementation notes
# The procedure:
# - Pickle the current task
# - Construct a sbatch argument that runs a generic runner function with the path to the pickled class
# - Runner function loads the class from pickle
# - Runner function hits the work button on it

import itertools
import luigi
import os
import pprint
import logging
import shutil
import subprocess
import sys
import time
import random

try:
    import cPickle as pickle
except ImportError:
    import pickle

from luigi.contrib import slurm_runner

POLL_TIME = 15  # decided to hard-code rather than configure here
MEM_RETRY_MAX_RETRIES = 10 # some hard limit to the maximum number of memory-related error retries
MEM_RETRY_MAX_MEM = 128000 # hard memory limit per task on retry

class slurm(luigi.Config):
    ntasks = luigi.IntParameter(default=1, significant=False)
    mem = luigi.IntParameter(default=4000, significant=False)
    mem_retries = luigi.IntParameter(default=3, significant=False,
        description="retries n times, doubling the memory each run if a task "
        "fails on an out of memory error")
    gres = luigi.Parameter(default='', significant=False)
    partition = luigi.Parameter(default='', significant=False)
    time = luigi.Parameter(default='', significant=False)
    shared_tmp_dir = luigi.Parameter(default='/home', significant=False)
    work_dir = luigi.Parameter(default='', significant=False,
        description="Location of our environment, must be a directory "
        "shared across all executors.")
    job_name_format = luigi.Parameter(
        significant=False, default='', description="A string that can be "
        "formatted with class variables to name the job with sbatch.")
    run_locally = luigi.BoolParameter(
        significant=False,
        description="run locally instead of on the cluster")
    poll_time = luigi.IntParameter(
        significant=False, default=POLL_TIME,
        description="specify the wait time to poll scontrol for the job status")
    dont_remove_tmp_dir = luigi.BoolParameter(
        significant=False,
        description="don't delete the temporary directory used (for debugging)")

# see http://code.activestate.com/recipes/580745-retry-decorator-in-python/
def retry(delays=(0, 1, 5, 30, 180, 600, 3600),
          exception=Exception,
          report=lambda *args: None):
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
                        report("retryable failed:", problem,
                               "-- delaying for %ds" % delay)
                        time.sleep(delay)
        return wrapped
    return wrapper


logger = logging.getLogger('luigi-interface')
logger.propagate = 0


@retry()
def _parse_job_state(job_id):
    """Parse "state" from 'scontrol show jobid=ID -o' output

    Returns state for the scontrol output, Returns 'u' if
    `scontrol` output is empty or job_id is not found.

    """
    job_out = subprocess.check_output(['scontrol', '-o', 'show', "jobid={}".format(job_id)]).decode()
    job_line = job_out.split()
    job_map = {}
    for job in job_line:
        job_s = job.split("=")
        try:
            job_map[job_s[0]] = job_s[1]
        except Exception as e:
            print("No value found for " + job_s[0])
            print(e)

    return job_map.get('JobState', 'u')


def _build_submit_command(cmd, job_name, outfile, errfile, ntasks, mem, gres, partition, time, sbatchfile):
    """Submit shell command to Slurm, queue via `sbatch`"""
    sbatch_template = """#!/bin/bash
    {cmd}"""
    submit_cmd = ['sbatch', '--parsable',
                  '-o', '{outfile}',
                  '-e', '{errfile}',
                  '--ntasks', '{ntasks}',
                  '--mem', '{mem}',
                  '-J', '{job_name}',
                  ]
    if gres != '':
        submit_cmd.extend(['--gres',  '{gres}'])
    if partition != '':
        submit_cmd.extend(['--partition',  '{partition}'])
    if time != '':
        submit_cmd.extend(['--time', '{time}'])
    submit_cmd.append('{sbatchfile}')
    submit_template = ' '.join(submit_cmd)

    with open(sbatchfile, "w") as fp:
        fp.write(sbatch_template.format(cmd=cmd))

    return submit_template.format(
        sbatch_template=sbatch_template, job_name=job_name, outfile=outfile, errfile=errfile,
        ntasks=ntasks, mem=mem, sbatchfile=sbatchfile, gres=gres, partition=partition, time=time)


@retry()
def _sbatch(submit_cmd):
    output = subprocess.check_output(submit_cmd, shell=True)
    return output


class SlurmTask(luigi.Task):

    """
    Base class for executing a job on Slurm

    Override ``work()`` (rather than ``run()``) with your job code.

    Parameters:

    - ntasks: Number of CPUs (or "slots") to allocate for the Task.
    - mem: The amount of memory to allocate for the Task.
    - gres: The gres resources to allocate for the Task.
    - time: The time to allocate for the Task.
    - partition: The partition allocate for the Task.
    - shared_tmp_dir: Shared drive accessible from all nodes in the cluster.
          Run method is pickled to a temporary folder in this path.
    - job_name_format: String that can be passed in to customize the job name
        string passed to sbatch; e.g. "Task123_{task_family}_{ntasks}...".
    - job_name: Exact job name to pass to sbatch.
    - run_locally: Run locally instead of on the cluster.
    - poll_time: the length of time to wait in order to poll the job
    - dont_remove_tmp_dir: Instead of deleting the temporary directory, keep it.

    """

    slurm_config = slurm()

    job_name = luigi.Parameter(
        significant=False, default='',
        description="Explicit job name given via sbatch.")

    def __init__(self, *args, **kwargs):
        super(SlurmTask, self).__init__(*args, **kwargs)
        if self.job_name != '':
            # use explicitly provided job name
            pass
        elif self.job_name_format != '':
            # define the job name with the provided format
            self.job_name = self.job_name_format.format(
                task_family=self.task_family, **self.__dict__)
        else:
            # default to the task family
            self.job_name = self.task_family

        if not hasattr(self, 'mem') or self.mem is None:
            self.mem = self.slurm_config.mem

        if not hasattr(self, 'mem_retries') or self.mem_retries is None:
            self.mem_retries = self.slurm_config.mem_retries

        if not hasattr(self, '_mem_retry') or self._mem_retry is None:
            self._mem_retry = 0


    def __str__(self):
        return '\n'.join([
            pprint.pformat(vars(self.slurm_config), indent=2),
            pprint.pformat(vars(self), indent=2)
        ])

    @property
    def ntasks(self):
        return self.slurm_config.ntasks

    @property
    def gres(self):
        return self.slurm_config.gres

    @property
    def partition(self):
        return self.slurm_config.partition

    @property
    def time(self):
        return self.slurm_config.time

    @property
    def shared_tmp_dir(self):
        return self.slurm_config.shared_tmp_dir

    @property
    def work_dir(self):
        return self.slurm_config.work_dir

    @property
    def job_name_format(self):
        return self.slurm_config.job_name_format

    @property
    def run_locally(self):
        return self.slurm_config.run_locally

    @property
    def poll_time(self):
        return self.slurm_config.poll_time

    @property
    def dont_remove_tmp_dir(self):
        return self.slurm_config.dont_remove_tmp_dir

    def _fetch_task_failures(self):
        if not os.path.exists(self.errfile):
            logger.info('No error file')
            return []
        with open(self.errfile, "r") as f:
            errors = f.readlines()
        if errors == []:
            return errors
        if errors[0].strip() == 'stdin: is not a tty':
            errors.pop(0)
        return errors

    def _fetch_task_out(self):
        if not os.path.exists(self.outfile):
            logger.info('No output file')
            return []
        with open(self.outfile, "r") as f:
            output = f.readlines()
        return output

    def _init_local(self):
        # Set up temp folder in shared directory (trim to max filename length)
        base_tmp_dir = self.shared_tmp_dir
        random_id = '%016x' % random.getrandbits(64)
        folder_name = self.task_id + '-' + random_id
        self.tmp_dir = os.path.join(base_tmp_dir, folder_name)
        max_filename_length = os.fstatvfs(0).f_namemax
        self.tmp_dir = self.tmp_dir[:max_filename_length]
        logger.info("Tmp dir: %s", self.tmp_dir)
        os.makedirs(self.tmp_dir)

        # Dump the code to be run into a pickle file
        self._dump(self.tmp_dir)

    def _dump(self, out_dir=''):
        """Dump instance to file."""
        with self.no_unpicklable_properties():
            self.job_file = os.path.join(out_dir, 'job.pickle')
            if self.__module__ == '__main__':
                d = pickle.dumps(self, 0).decode()
                module_name = os.path.basename(sys.argv[0]).rsplit('.', 1)[0]
                d = d.replace('(c__main__', "(c" + module_name)
                open(self.job_file, "w").write(d)
            else:
                pickle.dump(self, open(self.job_file, "wb"))

    def run(self):
        self.init_vars()
        if self.run_locally:
            self.work()
        else:
            self._init_local()
            # retry on memory errors only, avoiding a while(1){} loop in favor of a hard limit
            for i in range(0, MEM_RETRY_MAX_RETRIES):
                try:
                    self._run_job()
                except OutOfMemoryError as e:
                    # give up after configured number of retries
                    if self._mem_retry > self.mem_retries:
                        raise e
                    # double memory allocation, up to some hard limit
                    self.mem = self.mem * 2
                    if self.mem > MEM_RETRY_MAX_MEM:
                        raise e
                    # bump the retry count
                    self._mem_retry += 1
                # done running the job
                break
            # The procedure:
            # - Pickle the run method
            # - Construct a sbatch argument that runs a generic runner function with the path to the run method
            # - Runner function loads the run method
            # - Runner class untars the dependencies
            # - Runner function hits the button on the class's work() method

    def init_vars(self):
        """
        Initialise vars here that won't be available in the slurm environment,
        e.g. information from other luigi tasks.
        Save them in object variables so that they are serialised before work() is called.
        """
        pass

    def work(self):
        """Override this method, rather than ``run()``,  for your actual work."""
        pass

    def _run_job(self):
        # Build a sbatch argument that will run sge_runner.py on the directory we've specified
        runner_path = slurm_runner.__file__
        if runner_path.endswith("pyc"):
            runner_path = runner_path[:-3] + "py"
        job_str = 'cd "{}"; python {} --tmp-dir "{}"'.format(
            (self.work_dir if len(self.work_dir) else os.getcwd()), runner_path, self.tmp_dir
        )

        # Build sbatch file and submit command
        self.outfile = os.path.join(self.tmp_dir, 'job.out')
        self.errfile = os.path.join(self.tmp_dir, 'job.err')
        sbatchfile = os.path.join(self.tmp_dir, '{}.sbatch'.format(self.task_family))
        submit_cmd = _build_submit_command(job_str, self.task_family, self.outfile,
                                           self.errfile, self.ntasks, self.mem,
                                           self.gres, self.partition, self.time, sbatchfile)
        logger.debug('sbatch command: {}'.format(submit_cmd))

        # Submit the job and grab job ID
        cwd = os.getcwd()
        os.chdir(self.tmp_dir)
        output = _sbatch(submit_cmd)
        os.chdir(cwd)
        self.job_id = output.decode().strip()
        logger.debug("Submitted job to slurm with job id: {}".format(self.job_id))

        successful, stderr, stdout, elapsed = self._track_job()

        # Now delete the temporaries, if they're there.
        if not self.dont_remove_tmp_dir:
            logger.info('Removing temporary directory {}'.format(self.tmp_dir))
            if (os.path.exists(self.tmp_dir)):
                shutil.rmtree(self.tmp_dir, ignore_errors=True)

        # stop here if the job was not successful
        if not successful:
            raise SlurmError('Slurm job has FAILED:', stdout, stderr, elapsed)

    def _track_job(self):
        successful = False
        start = time.time()
        while True:
            # Sleep for a little bit
            time.sleep(self.poll_time)

            # See what the job's up to
            # ASSUMPTION
            job_status = _parse_job_state(self.job_id)
            elapsed = float(time.time() - start)
            if job_status == 'RUNNING' or job_status == 'COMPLETING':
                logger.info('Job is running ({:0.1f} seconds elapsed)...'.format(elapsed))
            elif job_status == 'PENDING':
                logger.info('Job is pending ({:0.1f} seconds elapsed)...'.format(elapsed))
            elif 'FAILED' in job_status:
                logger.error('Job has FAILED')
                break
            elif 'CANCELLED' in job_status:
                logger.error('Job has been CANCELLED')
                break
            elif job_status == 'COMPLETED' or job_status == 'u':
                # Then the job could either be failed or done.
                successful = True  # fail properly if you want to stop, don't just write to stderr!
                errors = self._fetch_task_failures()
                if not errors:
                    logger.info('Job is done')
                else:
                    for error in errors:
                        logger.error(error)
                break
            elif job_status == 'OUT_OF_MEMORY':
                logger.error('Job ran OUT_OF_MEMORY')
                raise OutOfMemoryError(
                    "job status isn't one of ['RUNNING', 'PENDING', 'COMPLETED', 'FAILED', 'CANCELLED', 'u']: {}".format(job_status),
                    '\n'.join(self._fetch_task_out()),
                    '\n'.join(self._fetch_task_failures())
                )
                break
            else:
                logger.info('Job status is UNKNOWN!')
                logger.info('Status is : {}'.format(job_status))
                raise SlurmError(
                    "job status isn't one of ['RUNNING', 'PENDING', 'COMPLETED', 'FAILED', 'CANCELLED', 'u']: {}".format(job_status),
                    '\n'.join(self._fetch_task_out()),
                    '\n'.join(self._fetch_task_failures())
                )
        stdout = '\n'.join(self._fetch_task_out())
        stderr = '\n'.join(self._fetch_task_failures())
        return (successful, stderr, stdout, elapsed)


class LocalSlurmTask(SlurmTask):
    """A local version of SlurmTask, for easier debugging.

    This version skips the ``sbatch`` steps and simply runs ``work()``
    on the local node, so you don't need to be on a Slurm cluster to
    use your Task in a test workflow.
    """

    def run(self):
        self.work()


class SlurmError(RuntimeError):
    def __init__(self, message, out=None, err=None, elapsed=None):
        super(SlurmError, self).__init__(message, out, err)
        self.message = message
        self.out = out
        self.err = err
        self.elapsed = elapsed

    def __str__(self):
        info = "Task Time Elapsed {}:\n{}".format(self.elapsed, self.message)
        if self.out:
            info += "\nSTDOUT: " + str(self.out)
        if self.err:
            info += "\nSTDERR: " + str(self.err)
        return info

class OutOfMemoryError(SlurmError): pass
