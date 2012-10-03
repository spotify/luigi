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

import sys
import os
import glob
import unittest


def suite():
    suite = unittest.TestSuite()
    for filename in glob.glob('test/*_test.py') + glob.glob('python/test/*_test.py'):
        f = os.path.splitext(os.path.basename(filename))[0]
        module = __import__(f)
        suite.addTest(unittest.defaultTestLoader.loadTestsFromModule(module))
    return suite


class run(unittest.TestProgram):
    """Runs tests and counts errors."""
    def __init__(self):
        #sys.path.append(test)
        unittest.TestProgram.__init__(self, '__main__', 'suite')

    def usageExit(self, msg=None):
        if msg:
            print msg
        print self.USAGE % self.__dict__
        sys.exit(-2)

    def runTests(self):
        if self.testRunner is None:
            self.testRunner = unittest.TextTestRunner(verbosity=self.verbosity)
        result = self.testRunner.run(self.test)
        error_count = len(result.errors) + len(result.failures)
        sys.exit(error_count)

if __name__ == '__main__':
    run()
