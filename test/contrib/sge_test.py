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
import os.path
from glob import glob
import unittest
import logging
from mock import patch

import luigi
from luigi.contrib.sge import SGEJobTask, _parse_qstat_state

import pytest

DEFAULT_HOME = '/home'

logger = logging.getLogger('luigi-interface')


QSTAT_OUTPUT = """job-ID  prior   name       user         state submit/start at     queue                          slots ja-task-ID
-----------------------------------------------------------------------------------------------------------------
     1 0.55500 job1 root         r     07/09/2015 16:56:45 all.q@node001                      1
     2 0.55500 job2 root         qw    07/09/2015 16:56:42                                    1
     3 0.00000 job3 root         t    07/09/2015 16:56:45                                    1
"""


def on_sge_master():
    try:
        subprocess.check_output('qstat', shell=True)
        return True
    except subprocess.CalledProcessError:
        return False


@pytest.mark.contrib
class TestSGEWrappers(unittest.TestCase):

    def test_track_job(self):
        """`track_job` returns the state using qstat"""
        self.assertEqual(_parse_qstat_state(QSTAT_OUTPUT, 1), 'r')
        self.assertEqual(_parse_qstat_state(QSTAT_OUTPUT, 2), 'qw')
        self.assertEqual(_parse_qstat_state(QSTAT_OUTPUT, 3), 't')
        self.assertEqual(_parse_qstat_state('', 1), 'u')
        self.assertEqual(_parse_qstat_state('', 4), 'u')


class TestJobTask(SGEJobTask):

    """Simple SGE job: write a test file to NSF shared drive and waits a minute"""

    i = luigi.Parameter()

    def work(self):
        logger.info('Running test job...')
        with open(self.output().path, 'w') as f:
            f.write('this is a test\n')

    def output(self):
        return luigi.LocalTarget(os.path.join(DEFAULT_HOME, 'testfile_' + str(self.i)))


@pytest.mark.contrib
class TestSGEJob(unittest.TestCase):

    """Test from SGE master node"""

    def test_run_job(self):
        if on_sge_master():
            outfile = os.path.join(DEFAULT_HOME, 'testfile_1')
            tasks = [TestJobTask(i=str(i), n_cpu=1) for i in range(3)]
            luigi.build(tasks, local_scheduler=True, workers=3)
            self.assertTrue(os.path.exists(outfile))

    @patch('subprocess.check_output')
    def test_run_job_with_dump(self, mock_check_output):
        mock_check_output.side_effect = [
            'Your job 12345 ("test_job") has been submitted',
            ''
        ]
        task = TestJobTask(i="1", n_cpu=1, shared_tmp_dir='/tmp')
        luigi.build([task], local_scheduler=True)
        self.assertEqual(mock_check_output.call_count, 2)

    def tearDown(self):
        for fpath in glob(os.path.join(DEFAULT_HOME, 'test_file_*')):
            try:
                os.remove(fpath)
            except OSError:
                pass
