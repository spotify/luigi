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

import hashlib
import mock
import os
import shutil
import subprocess
import tempfile
from helpers import unittest

import luigi
import luigi.lock
import luigi.notifications

luigi.notifications.DEBUG = True


class TestCmd(unittest.TestCase):

    def test_getpcmd(self):
        p = subprocess.Popen(["sleep", "1"])
        self.assertTrue(
            luigi.lock.getpcmd(p.pid) in ["sleep 1", '[sleep]']
        )
        p.kill()

real_exists = os.path.exists
def _make_path_exists_mock(returns):
    """ Returns the mock function that takes a list of return values to use on
        subsequent calls.  Once all values from returns are returned the mock
        function reverts back to the real function.
    """
    def _mock_function(path):
        return returns.pop(0) if returns else real_exists(path)
    return _mock_function

class CreatePidDirTest(unittest.TestCase):
    def setUp(self):
        self.pid_dir = os.path.join(tempfile.gettempdir(), 'luigi-test')
        # Create the file so the mkdir call will fail in the function.
        if not os.path.exists(self.pid_dir):
            os.mkdir(self.pid_dir)

    def tearDown(self):
        if os.path.exists(self.pid_dir):
            shutil.rmtree(self.pid_dir)

    @mock.patch('os.path.exists', side_effect=_make_path_exists_mock([False,False]))
    def test_race_condition_works_third_time(self, os_exist_function):
        acquired = luigi.lock.acquire_for(self.pid_dir)
        self.assertTrue(acquired)

    @mock.patch('os.path.exists', side_effect=_make_path_exists_mock([False,False,False]))
    def test_race_condition_fails(self, os_exist_function):
        self.assertRaises(OSError, luigi.lock.acquire_for, self.pid_dir)


class LockTest(unittest.TestCase):

    def setUp(self):
        self.pid_dir = tempfile.mkdtemp()
        self.pid, self.cmd, self.pid_file = luigi.lock.get_info(self.pid_dir)

    def tearDown(self):
        if os.path.exists(self.pid_file):
            os.remove(self.pid_file)
        os.rmdir(self.pid_dir)

    """
    # Doesn't work, and seems unnecessary for us.
    def test_get_info(self):
        p = subprocess.Popen(["yes", "à我ф"], stdout=subprocess.PIPE)
        pid, cmd, pid_file = luigi.lock.get_info(self.pid_dir, p.pid)
        p.kill()
        self.assertEqual(cmd, 'yes à我ф')
    """

    def test_acquiring_free_lock(self):
        acquired = luigi.lock.acquire_for(self.pid_dir)
        self.assertTrue(acquired)

    def test_acquiring_taken_lock(self):
        with open(self.pid_file, 'w') as f:
            f.write('%d\n' % (self.pid, ))

        acquired = luigi.lock.acquire_for(self.pid_dir)
        self.assertFalse(acquired)

    def test_acquiring_partially_taken_lock(self):
        with open(self.pid_file, 'w') as f:
            f.write('%d\n' % (self.pid, ))

        acquired = luigi.lock.acquire_for(self.pid_dir, 2)
        self.assertTrue(acquired)

        s = os.stat(self.pid_file)
        self.assertEqual(s.st_mode & 0o777, 0o777)

    def test_acquiring_lock_from_missing_process(self):
        fake_pid = 99999
        with open(self.pid_file, 'w') as f:
            f.write('%d\n' % (fake_pid, ))

        acquired = luigi.lock.acquire_for(self.pid_dir)
        self.assertTrue(acquired)

        s = os.stat(self.pid_file)
        self.assertEqual(s.st_mode & 0o777, 0o777)
