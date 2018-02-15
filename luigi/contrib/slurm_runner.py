# -*- coding: utf-8 -*-
#
# Adapted from luigi.contrib.sge_runner, to fix bugs with reading pickle files
#
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

"""
Slurm job runner, adapted from the SunGrid Engine runner

The main() function of this module will be executed on the
compute node by the submitted job. It accepts as a single
argument the shared temp folder containing the package archive
and pickled task to run, and carries out these steps:

- extract tarfile of package dependencies and place on the path
- unpickle SlurmJobTask instance created on the master node
- run SlurmJobTask.work()

On completion, SlurmJobTask on the master node will detect that
the job has left the queue, delete the temporary folder, and
return from SlurmJobTask.run()
"""

import argparse
import os
import sys
try:
    import cPickle as pickle
except ImportError:
    import pickle
import logging


def main(args=sys.argv):
    """Run the work() method from the class instance in the file "job-instance.pickle".
    """
    try:
        parser = argparse.ArgumentParser(description='slurm runner')
        # Set up logging.
        logging.basicConfig(level=logging.WARN)
        parser.add_argument('--tmp-dir', dest='tmp_dir', type=str)
        args = parser.parse_args()

        sys.path.append(os.getcwd())
        job = None
        with open(os.path.join(args.tmp_dir, 'job.pickle'), 'rb') as f:
            job = pickle.load(f)
        # Do the work contained
        job.work()

    except Exception as e:
        # Dump encoded data that we will try to fetch using mechanize
        print(e)
        raise


if __name__ == '__main__':
    main()
