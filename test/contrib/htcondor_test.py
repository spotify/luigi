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
from mock import patch

import luigi
from luigi.contrib.htcondor import HTCondorJobTask, _parse_condorq_state, \
    _parse_condor_submit_job_id

OUTPUT_DIR = '/tmp'

logger = logging.getLogger('luigi-interface')


CONDORQ_OUTPUT = """-- Schedd: example.com : <10.129.5.4:24821?...
 ID      OWNER            SUBMITTED     RUN_TIME ST PRI SIZE CMD
1.0   user1         11/9  10:51  11+00:44:12 R  0   0.3  condor_dagman -p 0
2.0   user1         11/9  10:54  0:00:00 I  0   0.0 condor_worker.py -
3.0   user2        11/15 10:32   5+01:03:38 R  0   2929.7 Ganga_0_FTandCPfit
3.1   user2        11/20 09:55   0+01:40:39 <  0   976.6 batchScript.sh
3.2   user2        11/20 09:55   0+01:40:38 R  0   976.6 batchScript.sh
8.0   user2        11/20 09:55   0+01:40:38 C  0   976.6 batchScript.sh
"""


def on_sge_master():
    try:
        subprocess.check_output('condor_q', shell=True)
        return True
    except subprocess.CalledProcessError:
        return False


class TestHTcondorWrappers(unittest.TestCase):

    def test_track_job(self):
        '''`track_job` returns the state using qstat'''
        self.assertEqual(_parse_condorq_state(CONDORQ_OUTPUT, 1.0), 'R')
        self.assertEqual(_parse_condorq_state(CONDORQ_OUTPUT, 2.0), 'I')
        self.assertEqual(_parse_condorq_state(CONDORQ_OUTPUT, 3.1), '<')
        self.assertEqual(_parse_condorq_state('', 1), 'unknown')
        self.assertEqual(_parse_condorq_state('', 4), 'unknown')

    def test_job_id(self):
        condor_submit_output = """Submitting job(s).
        1 job(s) submitted to cluster 8."""
        job_id = _parse_condor_submit_job_id(condor_submit_output)
        self.assertEqual(job_id, 8)


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

    @patch('subprocess.check_output')
    def test_run_job_with_dump(self, mock_check_output):
        mock_check_output.side_effect = [
            'Submitting job(s).\n1 job(s) submitted to cluster 8.',
            CONDORQ_OUTPUT,
        ]
        task = TestJobTask(i=1, n_cpu=1)
        luigi.build([task], local_scheduler=True)
        self.assertEqual(mock_check_output.call_count, 2)

    def tearDown(self):
        for fpath in glob(os.path.join(OUTPUT_DIR, 'testfile_*')):
            try:
                print('removing')
                os.remove(fpath)
            except OSError:
                pass
