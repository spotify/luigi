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

import os
import signal
import subprocess
import unittest
import time


class ServerDaemonTest(unittest.TestCase):
    def setUp(self):
        self.logfile = './luigi-server.log'
        self.pidfile = './luigi-server.pid'

        def rm(f):
            try:
                os.remove(f)
            except OSError:
                pass

        self.addCleanup(rm, self.logfile)
        self.addCleanup(rm, self.pidfile)

        def kill(pidfile):
            try:
                with open(pidfile) as f:
                    pid = f.read().strip()
                os.kill(int(pid), signal.SIGTERM)
            except OSError:
                pass

        self.addCleanup(kill, self.pidfile)

    def test_start_server_background(self):
        cmd = 'luigid --background --pidfile={pf} --logfile={lf}'
        subprocess.check_call(cmd.format(pf=self.pidfile, lf=self.logfile),
                              shell=True)
        # FIXME give daemon a chance to spin up
        time.sleep(.1)
        self.assertTrue(os.path.isfile(self.pidfile), 'bad pidfile')
        self.assertTrue(os.path.isfile(self.logfile), 'bad logfile')
