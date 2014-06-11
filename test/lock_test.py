# Copyright (c) 2012 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import unittest
import luigi
import luigi.lock
import luigi.notifications
import tempfile
import os
import hashlib
import subprocess

luigi.notifications.DEBUG = True


class TestCmd(unittest.TestCase):
    def test_getpcmd(self):
        p = subprocess.Popen(["sleep", "1"])
        self.assertEquals(
            luigi.lock.getpcmd(p.pid),
            "sleep 1"
        )
        p.kill()


class LockTest(unittest.TestCase):

    def setUp(self):
        self.pid_dir = tempfile.mkdtemp()
        self.pid = os.getpid()
        self.cmd = luigi.lock.getpcmd(self.pid)
        self.pidfile = os.path.join(self.pid_dir, hashlib.md5(self.cmd).hexdigest()) + '.pid'

    def tearDown(self):

        os.remove(self.pidfile)
        os.rmdir(self.pid_dir)

    def test_acquiring_free_lock(self):
        acquired = luigi.lock.acquire_for(self.pid_dir)
        self.assertTrue(acquired)

    def test_acquiring_taken_lock(self):
        with open(self.pidfile, 'w') as f:
            f.write('%d\n' % (self.pid, ))

        acquired = luigi.lock.acquire_for(self.pid_dir)
        self.assertFalse(acquired)

    def test_acquiring_partially_taken_lock(self):
        with open(self.pidfile, 'w') as f:
            f.write('%d\n' % (self.pid, ))

        acquired = luigi.lock.acquire_for(self.pid_dir, 2)
        self.assertTrue(acquired)

    def test_acquiring_lock_from_missing_process(self):
        with open(self.pidfile, 'w') as f:
            f.write('%d\n' % (self.pid + 1, ))

        acquired = luigi.lock.acquire_for(self.pid_dir)
        self.assertTrue(acquired)
