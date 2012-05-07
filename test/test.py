#!/usr/bin/env python
# Copyright (c) 2011 Spotify Ltd
# -*- coding: utf-8 -*-

import sys
import os
import glob
import unittest


# Code below was ruthlessly stolen from spotify.util.test
# in order to remove all dependencies on spotify.*

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
