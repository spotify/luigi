# -*- coding: utf-8 -*-

"""
.. Copyright 2012-2015 Spotify AB
   Copyright 2018
   Copyright 2018 EMBL-European Bioinformatics Institute

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

import os
import subprocess
import time
import sys
import logging
import random
import shutil
try:
    # Dill is used for handling pickling and unpickling if there is a deference
    # in server setups between the LSF submission node and the nodes in the
    # cluster
    import dill as pickle
except ImportError:
    import pickle

import luigi
import luigi.configuration
from luigi.contrib.hadoop import create_packages_archive
from luigi.contrib import lsf_runner
from luigi.task_status import PENDING, FAILED, DONE, RUNNING, UNKNOWN

"""
LSF batch system Tasks.
=======================

What's LSF? see http://en.wikipedia.org/wiki/Platform_LSF
and https://wiki.med.harvard.edu/Orchestra/IntroductionToLSF

See: https://github.com/spotify/luigi/issues/1936

This extension is modeled after the hadoop.py approach.
I'll be making a few assumptions, and will try to note them.

Going into it, the assumptions are:

- You schedule your jobs on an LSF submission node.
- The 'bjobs' command on an LSF batch submission system returns a standardized format.
- All nodes have access to the code you're running.
- The sysadmin won't get pissed if we run a 'bjobs' check every thirty
  seconds or so per job (there are ways of coalescing the bjobs calls if that's not cool).

The procedure:

- Pickle the class
- Construct a bsub argument that runs a generic runner function with the path to the pickled class
- Runner function loads the class from pickle
- Runner function hits the work button on it

"""

LOGGER = logging.getLogger('luigi-interface')


def track_job(job_id):
    """
    Tracking is done by requesting each job and then searching for whether the job
    has one of the following states:
    - "RUN",
    - "PEND",
    - "SSUSP",
    - "EXIT"
    based on the LSF documentation
    """
    cmd = "bjobs -noheader -o stat {}".format(job_id)
    track_job_proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, shell=True)
    status = track_job_proc.communicate()[0].strip('\n')
    return status


def kill_job(job_id):
    """
    Kill a running LSF job
    """
    subprocess.call(['bkill', job_id])


class LSFJobTask(luigi.Task):
    """
    Takes care of uploading and executing an LSF job
    """

    n_cpu_flag = luigi.IntParameter(default=2, significant=False)
    shared_tmp_dir = luigi.Parameter(default='/tmp', significant=False)
    resource_flag = luigi.Parameter(default='mem=8192', significant=False)
    memory_flag = luigi.Parameter(default='8192', significant=False)
    queue_flag = luigi.Parameter(default='queue_name', significant=False)
    runtime_flag = luigi.IntParameter(default=60)
    job_name_flag = luigi.Parameter(default='')
    poll_time = luigi.FloatParameter(
        significant=False, default=5,
        description="specify the wait time to poll bjobs for the job status")
    save_job_info = luigi.BoolParameter(default=False)
    output = luigi.Parameter(default='')
    extra_bsub_args = luigi.Parameter(default='')

    job_status = None

    def fetch_task_failures(self):
        """
        Read in the error file from bsub
        """
        error_file = os.path.join(self.tmp_dir, "job.err")
        if os.path.isfile(error_file):
            with open(error_file, "r") as f_err:
                errors = f_err.readlines()
        else:
            errors = ''
        return errors

    def fetch_task_output(self):
        """
        Read in the output file
        """
        # Read in the output file
        if os.path.isfile(os.path.join(self.tmp_dir, "job.out")):
            with open(os.path.join(self.tmp_dir, "job.out"), "r") as f_out:
                outputs = f_out.readlines()
        else:
            outputs = ''
        return outputs

    def _init_local(self):

        base_tmp_dir = self.shared_tmp_dir

        random_id = '%016x' % random.getrandbits(64)
        task_name = random_id + self.task_id
        # If any parameters are directories, if we don't
        # replace the separators on *nix, it'll create a weird nested directory
        task_name = task_name.replace("/", "::")

        # Max filename length
        max_filename_length = os.fstatvfs(0).f_namemax
        self.tmp_dir = os.path.join(base_tmp_dir, task_name[:max_filename_length])

        LOGGER.info("Tmp dir: %s", self.tmp_dir)
        os.makedirs(self.tmp_dir)

        # Dump the code to be run into a pickle file
        LOGGER.debug("Dumping pickled class")
        self._dump(self.tmp_dir)

        # Make sure that all the class's dependencies are tarred and available
        LOGGER.debug("Tarballing dependencies")
        # Grab luigi and the module containing the code to be run
        packages = [luigi, __import__(self.__module__, None, None, 'dummy')]
        create_packages_archive(packages, os.path.join(self.tmp_dir, "packages.tar"))

        # Now, pass onto the class's specified init_local() method.
        self.init_local()

    def init_local(self):
        """
        Implement any work to setup any internal datastructure etc here.
        You can add extra input using the requires_local/input_local methods.
        Anything you set on the object will be pickled and available on the compute nodes.
        """
        pass

    def run(self):
        """
        The procedure:
        - Pickle the class
        - Tarball the dependencies
        - Construct a bsub argument that runs a generic runner function with the path to the pickled class
        - Runner function loads the class from pickle
        - Runner class untars the dependencies
        - Runner function hits the button on the class's work() method
        """
        self._init_local()
        self._run_job()

    def work(self):
        """
        Subclass this for where you're doing your actual work.

        Why not run(), like other tasks? Because we need run to always be
        something that the Worker can call, and that's the real logical place to
        do LSF scheduling.
        So, the work will happen in work().
        """
        pass

    def _dump(self, out_dir=''):
        """
        Dump instance to file.
        """
        self.job_file = os.path.join(out_dir, 'job-instance.pickle')
        if self.__module__ == '__main__':
            dump_inst = pickle.dumps(self)
            module_name = os.path.basename(sys.argv[0]).rsplit('.', 1)[0]
            dump_inst = dump_inst.replace('(c__main__', "(c" + module_name)
            open(self.job_file, "w").write(dump_inst)

        else:
            pickle.dump(self, open(self.job_file, "w"))

    def _run_job(self):
        """
        Build a bsub argument that will run lsf_runner.py on the directory we've specified.
        """

        args = []

        if isinstance(self.output(), list):
            log_output = os.path.split(self.output()[0].path)
        else:
            log_output = os.path.split(self.output().path)

        args += ["bsub", "-q", self.queue_flag]
        args += ["-n", str(self.n_cpu_flag)]
        args += ["-M", str(self.memory_flag)]
        args += ["-R", "rusage[%s]" % self.resource_flag]
        args += ["-W", str(self.runtime_flag)]
        if self.job_name_flag:
            args += ["-J", str(self.job_name_flag)]
        args += ["-o", os.path.join(log_output[0], "job.out")]
        args += ["-e", os.path.join(log_output[0], "job.err")]
        if self.extra_bsub_args:
            args += self.extra_bsub_args.split()

        # Find where the runner file is
        runner_path = os.path.abspath(lsf_runner.__file__)

        args += [runner_path]
        args += [self.tmp_dir]

        # That should do it. Let the world know what we're doing.
        LOGGER.info("### LSF SUBMISSION ARGS: %s",
                    " ".join([str(a) for a in args]))

        # Submit the job
        run_job_proc = subprocess.Popen(
            [str(a) for a in args],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, cwd=self.tmp_dir)
        output = run_job_proc.communicate()[0]

        # ASSUMPTION
        # The result will be of the format
        # Job <123> is submitted ot queue <myqueue>
        # So get the number in those first brackets.
        # I cannot think of a better workaround that leaves logic on the Task side of things.
        LOGGER.info("### JOB SUBMISSION OUTPUT: %s", str(output))
        self.job_id = int(output.split("<")[1].split(">")[0])
        LOGGER.info(
            "Job %ssubmitted as job %s",
            self.job_name_flag + ' ',
            str(self.job_id)
        )

        self._track_job()

        # If we want to save the job temporaries, then do so
        # We'll move them to be next to the job output
        if self.save_job_info:
            LOGGER.info("Saving up temporary bits")

            # dest_dir = self.output().path
            shutil.move(self.tmp_dir, "/".join(log_output[0:-1]))

        # Now delete the temporaries, if they're there.
        self._finish()

    def _track_job(self):
        time0 = 0
        while True:
            # Sleep for a little bit
            time.sleep(self.poll_time)

            # See what the job's up to
            # ASSUMPTION
            lsf_status = track_job(self.job_id)
            if lsf_status == "RUN":
                self.job_status = RUNNING
                LOGGER.info("Job is running...")
                if time0 == 0:
                    time0 = int(round(time.time()))
            elif lsf_status == "PEND":
                self.job_status = PENDING
                LOGGER.info("Job is pending...")
            elif lsf_status == "DONE" or lsf_status == "EXIT":
                # Then the job could either be failed or done.
                errors = self.fetch_task_failures()
                if not errors:
                    self.job_status = DONE
                    LOGGER.info("Job is done")
                    time1 = int(round(time.time()))

                    # Return a near estimate of the run time to with +/- the
                    # self.poll_time
                    job_name = str(self.job_id)
                    if self.job_name_flag:
                        job_name = "%s %s" % (self.job_name_flag, job_name)
                    LOGGER.info(
                        "### JOB COMPLETED: %s in %s seconds",
                        job_name,
                        str(time1-time0)
                    )
                else:
                    self.job_status = FAILED
                    LOGGER.error("Job has FAILED")
                    LOGGER.error("\n\n")
                    LOGGER.error("Traceback: ")
                    for error in errors:
                        LOGGER.error(error)
                break
            elif lsf_status == "SSUSP":
                self.job_status = PENDING
                LOGGER.info("Job is suspended (basically, pending)...")

            else:
                self.job_status = UNKNOWN
                LOGGER.info("Job status is UNKNOWN!")
                LOGGER.info("Status is : %s", lsf_status)
                break

    def _finish(self):
        LOGGER.info("Cleaning up temporary bits")
        if self.tmp_dir and os.path.exists(self.tmp_dir):
            LOGGER.info('Removing directory %s', self.tmp_dir)
            shutil.rmtree(self.tmp_dir)

    def __del__(self):
        pass
        # self._finish()


class LocalLSFJobTask(LSFJobTask):
    """
    A local version of JobTask, for easier debugging.
    """

    def run(self):
        self.init_local()
        self.work()
