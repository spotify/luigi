import unittest

from luigi.tools.range import most_common


class MostCommonTest(unittest.TestCase):
    def setUp(self):
        self.runs = [
            ([1], (1, 1)),
            ([1, 1], (1, 2)),
            ([1, 1, 2], (1, 2)),
            ([1, 1, 2, 2, 2], (2, 3))
        ]

    def test_runs(self):
        for args, result in self.runs:
            actual = most_common(args)
            expected = result
            self.assertEqual(expected, actual)
