# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
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
import shutil
import stat
import glob
import time

"""HTCondor batch system Tasks.

Adapted by Luke Kreczko (@kreczko) from
`SGE extension <https://github.com/spotify/luigi/blob/master/luigi/contrib/sge.py>`_
by Jake Feala (@jfeala)

HTCondor (http://research.cs.wisc.edu/htcondor/) is a job scheduler used to allocate compute resources on a
shared cluster. Jobs are submitted using the ``condor_submit`` command and monitored
using ``condor_q``. To get started, install luigi on all nodes.

To run luigi workflows on an HTCondor cluster, subclass
:class:`luigi.contrib.htcondor.HTCondorJobTask` as you would any :class:`luigi.Task`,
but override the ``work()`` method, instead of ``run()``, to define the job
code. Then, run your Luigi workflow from the master node, assigning > 1
``workers`` in order to distribute the tasks in parallel across the cluster.

The following is an example usage (and can also be found in ``sge_tests.py``)

.. code-block:: python

    import logging
    import luigi
    import os
    from luigi.contrib.sge import HTCondorJobTask

    logger = logging.getLogger('luigi-interface')


    class TestJobTask(HTCondorJobTask):

        i = luigi.Parameter()

        def work(self):
            logger.info('Running test job...')
            with open(self.output().path, 'w') as f:
                f.write('this is a test')

        def output(self):
            return luigi.LocalTarget(os.path.join('/tmp', 'testfile_' + str(self.i)))


    if __name__ == '__main__':
        tasks = [TestJobTask(i=str(i), n_cpu=i+1) for i in range(3)]
        luigi.build(tasks, local_scheduler=True, workers=3)

"""


# This extension is modeled after the hadoop.py approach.
#
# Implementation notes
# The procedure:
# - Pickle the class
# - Construct a qsub argument that runs a generic runner function with the path to the pickled class
# - Runner function loads the class from pickle
# - Runner function hits the work button on it

import luigi
import luigi.hadoop
from luigi import six
import subprocess
import os
import logging
import random
import pwd
import pickle
import sys

from luigi.contrib import sge_runner

logger = logging.getLogger('luigi-interface')
logger.propagate = 0

DEFAULT_JOB_PARAMETERS = {
    'Universe': 'vanilla',
    'request_cpus': 1,
    'request_memory': 20,  # MB
    'when_to_transfer_output': 'ON_EXIT_OR_EVICT',
}

RUN_SCRIPT = """#!/bin/bash
BASE=${{_CONDOR_SCRATCH_DIR}}

if [ -f $BASE/pre.sh ]; then
  . $BASE/pre.sh
fi

python runner.py $BASE $BASE {tarball_param}

if [ -f $BASE/post.sh ]; then
  . $BASE/post.sh
fi

"""


def _parse_condorq_state(condorq_out, job_id):
    """Parse "state" column from `condor_q` output for given job_id

    Returns state for the *first* job matching job_id. Returns 'u' if
    `condor_q` output is empty or job_id is not found.
     ID      OWNER            SUBMITTED     RUN_TIME ST PRI SIZE CMD
    3.2   user2        11/20 09:55   0+01:40:38 R  0   976.6 batchScript.sh
    Possible job states:
    0   Unexpanded     U
    1   Idle           I
    2   Running        R
    3   Removed        X
    4   Completed      C
    5   Held           H
    6   Submission_err E
    """
    logger.debug('Parsing condor_q output \n {0}'.format(condorq_out))
    if condorq_out.strip() == '':
        return 'unknown'
    lines = condorq_out.split('\n')
    for line in lines:
        line = line.strip()
        # skip past header
        if line.startswith('--') or line.startswith('ID'):
            continue
        if line:
            # job, user, submit_day, submit_time, run_time, state, prio, size, cmd
            job, _, _, _, _, state = line.split()[0:6]
            if float(job) == float(job_id):
                return state.upper()
    return 'unknown'


def _parse_condor_submit_job_id(condor_submit_out):
    """Parse job id from condor_submit output string.

    Assume format:

        Submitting job(s).
        1 job(s) submitted to cluster 8.

    """
    return float(condor_submit_out.split()[-1])


def _build_job_description(job_params, N):
    submit_params = DEFAULT_JOB_PARAMETERS.copy()
    submit_params.update(job_params)
    lines = ['{0}={1}'.format(k, v) for k, v in six.iteritems(submit_params)]
    lines = "\n".join(lines)
    lines += '\nqueue {0}'.format(N)

    return lines


def _build_condor_submit_command(job_params):
    template = """echo "{job_params}" | condor_submit"""
    return template.format(
        job_params=job_params,
    )


def _copy_script_to_tmp_dir(script, tmp_dir, dst_file_name=""):
    if script and os.path.exists(script):
        file_name = os.path.basename(
            script) if not dst_file_name else dst_file_name
        tmp_script = os.path.join(tmp_dir, file_name)
        shutil.copy(script, tmp_script)


class HTCondorJobTask(luigi.Task):
    n_cpu = luigi.IntParameter(default=1, significant=False)
    memory = luigi.IntParameter(
        default=300,
        significant=False, description="Amount of memory to be requested"
    )
    universe = luigi.Parameter(default='vanilla', significant=False)
    copies = luigi.IntParameter(
        default='1', significant=False,
        description='How many copies of this task to submit as a bundle')
    run_locally = luigi.BoolParameter(
        significant=False,
        description="run locally instead of on the cluster")
    poll_time = luigi.IntParameter(
        significant=False, default=10,
        description="specify the wait time to poll condor_q for the job status")
    dont_remove_tmp_dir = luigi.BoolParameter(
        significant=False,
        description="don't delete the temporary directory used (for debugging)")
    no_tarball = luigi.BoolParameter(
        significant=False,
        description="don't tarball (and extract) the luigi project files")
    base_tmp_dir = luigi.Parameter(
        default='/tmp', significant=False, description="base location for the temporary job logs")
    pre_script = luigi.Parameter(
        default='',
        significant=False,
        description="shell script to be run before the payload"
    )
    post_script = luigi.Parameter(
        default='',
        significant=False,
        description="shell script to be run after the payload"
    )

    def __init__(self, *args, **kwargs):
        super(HTCondorJobTask, self).__init__(*args, **kwargs)

    def _fetch_task_failures(self):
        if not os.path.exists(self.errfile):
            logger.info('No error file')
            return []
        with open(self.errfile, "r") as f:
            errors = f.readlines()
        return errors

    def _init_local(self):
        # Set up temp folder (trim to max filename length)
        user_id = pwd.getpwuid(os.getuid()).pw_uid
        random_id = '%016x' % random.getrandbits(64)
        folder_name = '{0}_{1}-{2}'.format(user_id, self.task_id, random_id)
        self.tmp_dir = os.path.join(self.base_tmp_dir, folder_name)
        max_filename_length = os.fstatvfs(0).f_namemax
        self.tmp_dir = self.tmp_dir[:max_filename_length]
        logger.info("Tmp dir: %s", self.tmp_dir)
        os.makedirs(self.tmp_dir)

        # Dump the code to be run into a pickle file
        logging.debug("Dumping pickled class")
        self._dump(self.tmp_dir)

        if not self.no_tarball:
            # Make sure that all the class's dependencies are tarred and available
            # This is not necessary if luigi is importable from the cluster
            # node
            logging.debug("Tarballing dependencies")
            # Grab luigi and the module containing the code to be run
            packages = [luigi] + \
                [__import__(self.__module__, None, None, 'dummy')]
            luigi.hadoop.create_packages_archive(
                packages, os.path.join(self.tmp_dir, "packages.tar"))

    def run(self):
        if self.run_locally:
            self.work()
        else:
            self._init_local()
            self._run_job()
            # The procedure:
            # - Pickle the class
            # - Tarball the dependencies
            # - Construct a condor_submit argument that runs a generic runner function with the path to the pickled class
            # - Runner function loads the class from pickle
            # - Runner class untars the dependencies
            # - Runner function hits the button on the class's work() method

    def work(self):
        """Override this method, rather than ``run()``,  for your actual work."""
        pass

    def _dump(self, out_dir=''):
        """Dump instance to file."""
        with self.no_unpicklable_properties():
            self.job_file = os.path.join(out_dir, 'job-instance.pickle')
            if self.__module__ == '__main__':
                d = pickle.dumps(self)
                module_name = os.path.basename(sys.argv[0]).rsplit('.', 1)[0]
                d = d.replace('(c__main__', "(c" + module_name)
                open(self.job_file, "w").write(d)
            else:
                pickle.dump(self, open(self.job_file, "w"))

    def _run_job(self):

        # copy runner into job folder
        # add job_file to input files
        # enable transfer of input files
        # create script that
        #  - calls the user pre-script
        #  - python <runner> <pickle>
        #  - calls the user post-script
        #
        _copy_script_to_tmp_dir(self.pre_script, self.tmp_dir, 'pre.sh')
        _copy_script_to_tmp_dir(self.post_script, self.tmp_dir, 'post.sh')
        runner_path = sge_runner.__file__
        _copy_script_to_tmp_dir(runner_path, self.tmp_dir, 'runner.py')

        tarball_param = ""
        if self.no_tarball:
            tarball_param = "--no-tarball"

        run_sh = os.path.join(self.tmp_dir, 'run.sh')
        with open(run_sh, 'w') as f:
            f.write(RUN_SCRIPT.format(tarball_param=tarball_param))
        st = os.stat(run_sh)
        os.chmod(run_sh, st.st_mode | stat.S_IEXEC)

        self.outfile = os.path.join(self.tmp_dir, 'job.out')
        self.errfile = os.path.join(self.tmp_dir, 'job.err')
        job_params = {
            'Executable': run_sh,
            'transfer_input_files': ','.join(glob.glob(self.tmp_dir + '/*')),
            'output': self.outfile,
            'error': self.errfile,
            'log': os.path.join(self.tmp_dir, 'job.log'),
            'request_cpus': self.n_cpu,
            'request_memory': self.memory,
        }

        # build job description (mostly for debugging)
        job_desc = _build_job_description(job_params, self.copies)
        job_desc_file = os.path.join(self.tmp_dir, 'job.desc')
        with open(job_desc_file, 'w') as f:
            f.write(job_desc)
        # Build condor_submit command
        submit_cmd = _build_condor_submit_command(job_desc)
        logger.debug('condor_submit command: \n' + submit_cmd)

        # Submit the job and grab job ID.
        output = subprocess.check_output(submit_cmd, shell=True)
        self.job_id = _parse_condor_submit_job_id(output)
        logger.debug(
            "Submitted job to condor_submit with response:\n" + output)

        self._track_job()

        # Now delete the temporaries, if they're there.
        # unless transfer_output files is on.
        if (self.tmp_dir and os.path.exists(self.tmp_dir) and not self.dont_remove_tmp_dir):
            logger.info('Removing temporary directory %s' % self.tmp_dir)
            subprocess.call(["rm", "-rf", self.tmp_dir])

    def _track_job(self):
        """ Known job statuses
        0   Unexpanded     U
        1   Idle           I
        2   Running        R
        3   Removed        X
        4   Completed      C
        5   Held           H
        6   Submission_err E
        < transferring input
        > transferring output
        """
        known_statuses = ['U', 'E', 'R', 'X', 'C', 'H', 'E', '<', '>']
        while True:
            # Sleep for a little bit
            time.sleep(self.poll_time)
            condorq_out = subprocess.check_output(
                ['condor_q', str(self.job_id)])
            job_status = _parse_condorq_state(condorq_out, self.job_id)
            if job_status == 'unknown':
                # try condor_history for finished jobs
                condorq_out = subprocess.check_output(
                    ['condor_history', str(self.job_id), '-limit 1'])
                job_status = _parse_condorq_state(condorq_out, self.job_id)
                if job_status == 'unknown':
                    logger.error('Job status is UNKNOWN!')
                    raise Exception("job status isn't one of [{0}]: {1}".format(
                        ','.join(known_statuses), job_status))
            logger.debug('Job status is : {0}'.format(job_status))
            if job_status == 'X':
                # just about to be completed
                continue

            if job_status in ['R', '<', '>']:
                logger.info('Job is running')
            elif job_status == 'H':
                logger.info('Job is on hold - something went wrong.')
            elif job_status == 'I':
                logger.info('Job is pending')
            elif job_status == 'E':
                logger.error(
                    'Job has failed:\n' + '\n'.join(self._fetch_task_failures()))
                break
            elif job_status == 'C':
                logger.info('Job is done')
                break
