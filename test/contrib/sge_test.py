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
import os.path
import unittest
import logging

import luigi
from luigi.contrib.sge import SGEJobTask, parse_qstat_state, clean_task_id, DEFAULT_HOME

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


class TestSGEWrappers(unittest.TestCase):

    def test_track_job(self):
        '''`track_job` returns the state using qstat'''
        self.assertEqual(parse_qstat_state(QSTAT_OUTPUT, 1), 'r')
        self.assertEqual(parse_qstat_state(QSTAT_OUTPUT, 2), 'qw')
        self.assertEqual(parse_qstat_state(QSTAT_OUTPUT, 3), 't')
        self.assertEqual(parse_qstat_state('', 1), 'u')
        self.assertEqual(parse_qstat_state('', 4), 'u')

    def test_clean_task_id(self):
        task_id = 'SomeTask(param_1=0, param_2=/path/to/file)'
        cleaned_id = 'SomeTask-param_1-0--param_2--path-to-file-'
        self.assertEqual(clean_task_id(task_id), cleaned_id)


class TestJobTask(SGEJobTask):

    '''Simple SGE job: write a test file to NSF shared drive and waits a minute'''

    i = luigi.Parameter()

    def work(self):
        logger.info('Running test job...')
        with open(self.output().path, 'w') as f:
            f.write('this is a test\n')

    def output(self):
        return luigi.LocalTarget(os.path.join(DEFAULT_HOME, 'testfile_' + str(self.i)))


class TestSGEJob(unittest.TestCase):

    '''Test from SGE master node'''

    def test_run_job(self):
        if on_sge_master():
            outfile = os.path.join(DEFAULT_HOME, 'testfile_1')
            tasks = [TestJobTask(i=str(i), n_cpu=i + 1) for i in range(2)]
            luigi.build(tasks, local_scheduler=True)
            self.assertTrue(os.path.exists(outfile))


if __name__ == '__main__':
    unittest.main()
