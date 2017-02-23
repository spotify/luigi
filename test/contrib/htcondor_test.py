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

import subprocess
import os
from glob import glob
import unittest
import logging
import luigi
from luigi.contrib.htcondor import HTCondorJobTask

OUTPUT_DIR = '/tmp'

logger = logging.getLogger('luigi-interface')


def on_sge_master():
    try:
        subprocess.check_output('condor_q', shell=True)
        return True
    except subprocess.CalledProcessError:
        return False


class TestJobTask(HTCondorJobTask):

    '''Simple HTcondor job: write a test file to /tmp and waits a minute'''

    i = luigi.Parameter()

    def work(self):
        logger.info('Running test job...')
        with open(self.output().path, 'w') as f:
            f.write('this is a test\n')

    def output(self):
        output_file = os.path.join(OUTPUT_DIR, 'testfile_' + str(self.i))
        return luigi.LocalTarget(output_file)


class TestHTCondorJob(unittest.TestCase):

    '''Test from HTCondor scheduler node'''

    def test_run_job(self):
        if on_sge_master():
            outfile = os.path.join(OUTPUT_DIR, 'testfile_1')
            tasks = [
                TestJobTask(i=str(i), n_cpu=1, run_locally=True)
                for i in range(3)
            ]
            luigi.build(tasks, local_scheduler=True, workers=3)
            self.assertTrue(os.path.exists(outfile))

    def tearDown(self):
        for fpath in glob(os.path.join(OUTPUT_DIR, 'testfile_*')):
            try:
                print('removing')
                os.remove(fpath)
            except OSError:
                pass
