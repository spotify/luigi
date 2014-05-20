import doctest

import luigi.task


def load_tests(loader, tests, ignore):
    tests.addTests(doctest.DocTestSuite(luigi.task))
    return tests
