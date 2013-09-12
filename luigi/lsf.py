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
# - Runner function hits the run button on it

import configuration
import os
import subprocess

def track_job(job_id):
    # You see how specific this is to the particular output of bjobs?
    # I've never set up an LSF cluster, so I don't know whether or not the output of bjobs
    # is set by the sysadmins. 
    # It could probably be generalized by running bjobs {job_id}, then grepping for
    # "RUN", "PEND", "SSUSP", or "EXIT"
    # because I've seen those referenced in official documentation. 
    cmd = "bjobs %d | awk 'FNR==2 {print $3}'" % job_id
    p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
    status = p.communicate()[0].strip('\n')
    return status

def run_and_track_lsf_job(arglist):



def fetch_task_failures(job_name):
    # Read in the error file 
    base_tmp_dir = configuration.get_config().get('lsf', 'shared-tmp-dir')
    with open(os.path.join(base_tmp_dir, job_name+".err"), "r") as f:
        errors = f.readlines()
    return errors

def fetch_task_output(job_name):
    # Read in the output file 
    base_tmp_dir = configuration.get_config().get('lsf', 'shared-tmp-dir')
    with open(os.path.join(base_tmp_dir, job_name+".out"), "r") as f:
        outputs = f.readlines()
    return outputs

def kill_job(job_id):
    subprocess.call(['bkill', job_id])

class LSFJobError(Exception):
    pass