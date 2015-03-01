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
from helpers import unittest

from luigi.contrib.ssh import RemoteContext


class TestMockedRemoteContext(unittest.TestCase):

    def test_subprocess_delegation(self):
        """ Test subprocess call structure using mock module """
        orig_Popen = subprocess.Popen
        self.last_test = None

        def Popen(cmd, **kwargs):
            self.last_test = cmd

        subprocess.Popen = Popen
        context = RemoteContext(
            "some_host",
            username="luigi",
            key_file="/some/key.pub"
        )
        context.Popen(["ls"])
        self.assertTrue("ssh" in self.last_test)
        self.assertTrue("-i" in self.last_test)
        self.assertTrue("/some/key.pub" in self.last_test)
        self.assertTrue("luigi@some_host" in self.last_test)
        self.assertTrue("ls" in self.last_test)

        subprocess.Popen = orig_Popen

    def test_check_output_fail_connect(self):
        """ Test check_output to a non-existing host """
        context = RemoteContext("__NO_HOST_LIKE_THIS__", connect_timeout=1)
        self.assertRaises(
            subprocess.CalledProcessError,
            context.check_output, ["ls"]
        )
