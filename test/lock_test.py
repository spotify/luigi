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

luigi.notifications.DEBUG = True

class LockTest(unittest.TestCase):

    def setUp(self):
        self.pid_dir = tempfile.mkdtemp()

    def tearDown(self):
        my_pid = os.getpid()
        my_cmd = luigi.lock.getpcmd(my_pid)
        pidfile = os.path.join(self.pid_dir, hashlib.md5(my_cmd).hexdigest()) + '.pid'

        os.remove(pidfile)
        os.rmdir(self.pid_dir)

    def test_acquiring_free_lock(self):
        acquired = luigi.lock.acquire_for(self.pid_dir)
        self.assertTrue(acquired)

    def test_acquiring_taken_lock(self):
        my_pid = os.getpid()
        my_cmd = luigi.lock.getpcmd(my_pid)

        pidfile = os.path.join(self.pid_dir, hashlib.md5(my_cmd).hexdigest()) + '.pid'

        f = open(pidfile, 'w')
        f.write('%d\n' % (my_pid, ))
        f.close()

        acquired = luigi.lock.acquire_for(self.pid_dir)
        self.assertFalse(acquired)
