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
import lsf_runner
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

    resource_flag = configuration.get_config().get("lsf", "resource-flag", "mem=15000")
    n_cpu_flag = configuration.get_config().get("lsf", "n-cpu-flag", 1)
    queue_flag = configuration.get_config().get("lsf", "queue-flag", "short")
    runtime_flag = configuration.get_config().get("lsf", "runtime-flag", 720)
    poll_time = configuration.get_config().get('lsf', 'job-status-timeout', 30)

    def __init__(self, lsf_args=[]):

        base_tmp_dir = configuration.get_config().get('lsf', 'shared-tmp-dir')
        self.tmp_dir = os.path.join(base_tmp_dir, self.task_id+'%016x' % random.getrandbits(64))
        logger.debug("Tmp dir: %s", self.tmp_dir)
        os.makedirs(self.tmp_dir)

        self.job_file = os.path.join(self.tmp_dir, "job-instance.pickle")

    def fetch_task_output(self):
        with open(os.path.join(self.tmp_dir, "job.err"), "r") as f:
            errors = f.readlines()
        return errors

    def fetch_task_output(self):
        # Read in the output file 
        base_tmp_dir = configuration.get_config().get('lsf', 'shared-tmp-dir')
        with open(os.path.join(base_tmp_dir, "job.out"), "r") as f:
            outputs = f.readlines()
        return outputs

    def init_local(self):
        ''' Implement any work to setup any internal datastructure etc here.
        You can add extra input using the requires_local/input_local methods.
        Anything you set on the object will be pickled and available on the compute nodes.
        '''
        pass

    def run(self):
        self.init_local()
        self._run_job()
        # The procedure:
        # - Pickle the class
        # - Construct a bsub argument that runs a generic runner function with the path to the pickled class
        # - Runner function loads the class from pickle
        # - Runner function hits the run button on it

    def work(self):
        # Subclass this for where you're doing your actual work.
        # 
        # Why? Because we need run to always be something that the Worker can call,
        # and that's the real logical place to do LSF scheduling. 
        # So, the work will happen in work().
        pass

    def _run_job(self):
        # Pickle ourselves.
        with open(self.job_file, "w") as f:
            pickle.dump(self, f)

        # Build a bsub argument that will run lsf_runner.py on the directory we've specified.
        args = []

        args += ["bsub", "-q", self.queue_flag]
        args += ["-W", self.runtime_flag]
        args += ["-n", self.n_cpu_flag]
        args += ["-R", "rusage[%s]"%self.resource_flag]
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
        p = subprocess.Popen(arglist, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        output = p.communicate()[0]
        # ASSUMPTION
        # The result will be of the format
        # Job <123> is submitted ot queue <myqueue>
        # So get the number in those first brackets. 
        # I cannot think of a better workaround that leaves logic on the Task side of things.
        self.job_id = int(output.split("<")[1].split(">")[0])
        logger.info("Job submitted as job {job_id}".format(job_id=self.job_id))

        self._track_job()

        # TODO
        # Move the contents of tmp_dir to the output_dir()
        try:
            dest_dir = self.output().path
            for filename in os.listdir(self.tmp_dir):
                shutil.move(os.path.join(self.tmp_dir, filename), dest_dir)
            self._finish()
        except:
            logger.debug("Couldn't move contents of %s to the output folder. Does an output exist?" % self.tmp_dir)

    def _track_job(self):
        while True:
            # Sleep for a little bit
            time.sleep(self.poll_time)

            # See what the job's up to
            # ASSUMPTION
            lsf_status = track_job(job_id)
            if lsf_status == "RUN":
                job_status = RUNNING
            elif lsf_status == "PEND"
                job_status = PENDING
            elif lsf_status == "EXIT"
                # Then the job could either be failed or done.
                errors = fetch_task_failures(job_name)
                if errors == '':
                    job_status = DONE
                else:
                    job_status = FAILED
                break
            elif lsf_status == "SSUSP": # I think that's what it is...
                job_status = PENDING

            else:
                job_status = UNKNOWN
                break
                raise Exception, "What the heck, the job_status isn't in my list, but it is %s" % lsf_status

            print "Job status is %s" % job_status


    def _finish(self):
        if self.tmp_dir and os.path.exists(self.tmp_dir):
            logger.debug('Removing directory %s' % self.tmp_dir)
            shutil.rmtree(self.tmp_dir)

    def __del__(self):
        self._finish()











