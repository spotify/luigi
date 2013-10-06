# LSF batch system Tasks.
# What's LSF? see http://en.wikipedia.org/wiki/Platform_LSF 
# and https://wiki.med.harvard.edu/Orchestra/IntroductionToLSF
# 
# This extension is modeled after the hadoop.py approach.
# I'll be making a few assumptions, and will try to note them.
# Going into it, the assumptions are:
# - You schedule your jobs on an LSF submission node. 
# - the 'bjobs' command on an LSF batch submission system returns a standardized format.
# - All nodes have access to the code you're running. 
# - The sysadmin won't get pissed if we run a 'bjobs' check every thirty 
#   seconds or so per job (there are ways of coalescing the bjobs calls if that's not cool). 
#
# Implementation notes:


# The procedure:
# - Pickle the class
# - Construct a bsub argument that runs a generic runner function with the path to the pickled class
# - Runner function loads the class from pickle
# - Runner function hits the work button on it

import configuration
import os
import subprocess
import time
import luigi
import luigi.hadoop
import lsf_runner
import sys
import logging
import random
import shutil
import cPickle as pickle
from task_status import PENDING, FAILED, DONE, RUNNING, UNKNOWN
logger = logging.getLogger('luigi-interface')


def attach(*packages):
    logger.info("Attaching packages does nothing in LSF batch submission. All packages are expected to exist on the compute node.")
    pass

def track_job(job_id):
    # You see how specific this is to the particular output of bjobs?
    # I've never set up an LSF cluster, so I don't know whether or not the output of bjobs
    # is set by the sysadmins. 
    # It could probably be generalized by running bjobs {job_id}, then grepping for
    # "RUN", "PEND", "SSUSP", or "EXIT"
    # because I've seen those referenced in official documentation. 

    # ASSUMPTION
    cmd = "bjobs %d | awk 'FNR==2 {print $3}'" % job_id
    p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
    status = p.communicate()[0].strip('\n')
    return status



def kill_job(job_id):
    subprocess.call(['bkill', job_id])

class LSFJobError(Exception):
    pass

class JobTask(luigi.Task):
    """Takes care of uploading and executing an LSF job"""

    n_cpu_flag = luigi.IntParameter(default_from_config={"section":"lsf", "name":"n-cpu-flag"})
    resource_flag = luigi.Parameter(default_from_config={"section":"lsf", "name":"resource-flag"})
    queue_flag = luigi.Parameter(default_from_config={"section":"lsf", "name":"queue-flag"})
    runtime_flag = luigi.Parameter(default_from_config={"section":"lsf", "name":"runtime-flag"})
    poll_time = luigi.FloatParameter(default_from_config={"section":"lsf", "name":'job-status-timeout'})
    save_job_info = luigi.BooleanParameter(default_from_config={"section": "lsf", "name": "save-job-info"})

    def fetch_task_failures(self):
        error_file = os.path.join(self.tmp_dir, "job.err")
        if os.path.isfile(error_file):
            with open(error_file, "r") as f:
                errors = f.readlines()
        else:
            errors = ''
        return errors

    def fetch_task_output(self):
        # Read in the output file 
        base_tmp_dir = configuration.get_config().get('lsf', 'shared-tmp-dir')
        with open(os.path.join(self.tmp_dir, "job.out"), "r") as f:
            outputs = f.readlines()
        return outputs

    def _init_local(self):

        base_tmp_dir = configuration.get_config().get('lsf', 'shared-tmp-dir')

        task_name = self.task_id+'%016x' % random.getrandbits(64)
        self.tmp_dir = os.path.join(base_tmp_dir, task_name)

        logger.debug("Tmp dir: %s", self.tmp_dir)
        os.makedirs(self.tmp_dir)

        # Dump the code to be run into a pickle file
        logging.debug("Dumping pickled class")
        self._dump(self.tmp_dir)

        # Make sure that all the class's dependencies are tarred and available
        logging.debug("Tarballing dependencies")
        # Grab luigi and the module containing the code to be run
        packages = [luigi] + [__import__(self.__module__, None, None, 'dummy')]
        luigi.hadoop.create_packages_archive(packages, os.path.join(self.tmp_dir, "packages.tar"))

        # Now, pass onto the class's specified init_local() method. 
        self.init_local()

    def init_local(self):
        ''' Implement any work to setup any internal datastructure etc here.
        You can add extra input using the requires_local/input_local methods.
        Anything you set on the object will be pickled and available on the compute nodes.
        '''
        pass

    def run(self):
        self._init_local()
        self._run_job()
        # The procedure:
        # - Pickle the class
        # - Tarball the dependencies
        # - Construct a bsub argument that runs a generic runner function with the path to the pickled class
        # - Runner function loads the class from pickle
        # - Runner class untars the dependencies
        # - Runner function hits the button on the class's work() method

    def work(self):
        # Subclass this for where you're doing your actual work.
        # 
        # Why? Because we need run to always be something that the Worker can call,
        # and that's the real logical place to do LSF scheduling. 
        # So, the work will happen in work().
        pass

    def _dump(self, out_dir=''):
        """Dump instance to file."""
        self.job_file = os.path.join(out_dir, 'job-instance.pickle')
        if self.__module__ == '__main__':
            d = pickle.dumps(self)
            module_name = os.path.basename(sys.argv[0]).rsplit('.', 1)[0]
            d = d.replace('(c__main__', "(c" + module_name)
            open(self.job_file, "w").write(d)

        else:
            pickle.dump(self, open(self.job_file, "w"))


    def _run_job(self):

        # Build a bsub argument that will run lsf_runner.py on the directory we've specified.
        args = []

        args += ["bsub", "-q", self.queue_flag]
        args += ["-W", self.runtime_flag]
        args += ["-n", str(self.n_cpu_flag)]
        args += ["-R", "rusage[%s]"%"mem=15000"]
        args += ["-o", "job.out"]
        args += ["-e", "job.err"]
        args += ["python"]

        # Find where our file is
        runner_path = lsf_runner.__file__
        # assume source is next to compiled
        if runner_path.endswith("pyc"):
            runner_path = runner_path[:-3] + "py"

        args += [runner_path]
        args += [self.tmp_dir]

        # That should do it. Let the world know what we're doing.
        logger.info(" ".join(args))

        # Submit the job
        p = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, cwd=self.tmp_dir)
        output = p.communicate()[0]
        # ASSUMPTION
        # The result will be of the format
        # Job <123> is submitted ot queue <myqueue>
        # So get the number in those first brackets. 
        # I cannot think of a better workaround that leaves logic on the Task side of things.
        self.job_id = int(output.split("<")[1].split(">")[0])
        logger.info("Job submitted as job {job_id}".format(job_id=self.job_id))

        self._track_job()

        # If we want to save the job temporaries, then do so
        # We'll move them to be next to the job output
        if self.save_job_info:
            dest_dir = self.output().path
            shutil.move(self.tmp_dir, os.path.split(dest_dir)[0])

        # Now delete the temporaries, if they're there.
        self._finish()

    def _track_job(self):
        while True:
            # Sleep for a little bit
            time.sleep(self.poll_time)

            # See what the job's up to
            # ASSUMPTION
            lsf_status = track_job(self.job_id)
            if lsf_status == "RUN":
                job_status = RUNNING
                logger.debug("Job is running...")
            elif lsf_status == "PEND":
                job_status = PENDING
                logger.debug("Job is pending...")
            elif lsf_status == "EXIT":
                # Then the job could either be failed or done.
                errors = self.fetch_task_failures()
                if errors == '':
                    job_status = DONE
                    logger.debug("Job is done")
                else:
                    job_status = FAILED
                    logger.debug("Job has FAILED")
                    logger.debug("\n\n")
                    logger.debug("Traceback: ")
                    for error in errors:
                        logger.debug(error)
                break
            elif lsf_status == "SSUSP": # I think that's what it is...
                job_status = PENDING
                logger.debug("Job is suspended (basically, pending)...")

            else:
                job_status = UNKNOWN
                logger.debug("Job status is UNKNOWN!")
                break
                raise Exception, "What the heck, the job_status isn't in my list, but it is %s" % lsf_status

            print "Job status is %s" % job_status


    def _finish(self):

        logger.debug("Cleaning up temporary bits")
        if self.tmp_dir and os.path.exists(self.tmp_dir):
            logger.debug('Removing directory %s' % self.tmp_dir)
            shutil.rmtree(self.tmp_dir)

    def __del__(self):
        pass
        # self._finish()











