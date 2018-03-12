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

import os
from helpers import unittest

from luigi.format import InputPipeProcessWrapper


BASH_SCRIPT = """
#!/bin/bash

trap "touch /tmp/luigi_sigpipe.marker; exit 141" SIGPIPE


for i in {1..3}
do
    sleep 0.1
    echo "Welcome $i times"
done
"""

FAIL_SCRIPT = BASH_SCRIPT + """
exit 1
"""


class TestSigpipe(unittest.TestCase):

    def setUp(self):
        with open("/tmp/luigi_test_sigpipe.sh", "w") as fp:
            fp.write(BASH_SCRIPT)

    def tearDown(self):
        os.remove("/tmp/luigi_test_sigpipe.sh")
        if os.path.exists("/tmp/luigi_sigpipe.marker"):
            os.remove("/tmp/luigi_sigpipe.marker")

    def test_partial_read(self):
        p1 = InputPipeProcessWrapper(["bash", "/tmp/luigi_test_sigpipe.sh"])
        self.assertEqual(p1.readline().decode('utf8'), "Welcome 1 times\n")
        p1.close()
        self.assertTrue(os.path.exists("/tmp/luigi_sigpipe.marker"))

    def test_full_read(self):
        p1 = InputPipeProcessWrapper(["bash", "/tmp/luigi_test_sigpipe.sh"])
        counter = 1
        for line in p1:
            self.assertEqual(line.decode('utf8'), "Welcome %i times\n" % counter)
            counter += 1
        p1.close()
        self.assertFalse(os.path.exists("/tmp/luigi_sigpipe.marker"))


class TestSubprocessException(unittest.TestCase):

    def setUp(self):
        with open("/tmp/luigi_test_sigpipe.sh", "w") as fp:
            fp.write(FAIL_SCRIPT)

    def tearDown(self):
        os.remove("/tmp/luigi_test_sigpipe.sh")
        if os.path.exists("/tmp/luigi_sigpipe.marker"):
            os.remove("/tmp/luigi_sigpipe.marker")

    def test_partial_read(self):
        p1 = InputPipeProcessWrapper(["bash", "/tmp/luigi_test_sigpipe.sh"])
        self.assertEqual(p1.readline().decode('utf8'), "Welcome 1 times\n")
        p1.close()
        self.assertTrue(os.path.exists("/tmp/luigi_sigpipe.marker"))

    def test_full_read(self):
        def run():
            p1 = InputPipeProcessWrapper(["bash", "/tmp/luigi_test_sigpipe.sh"])
            counter = 1
            for line in p1:
                self.assertEqual(line.decode('utf8'), "Welcome %i times\n" % counter)
                counter += 1
            p1.close()

        self.assertRaises(RuntimeError, run)
