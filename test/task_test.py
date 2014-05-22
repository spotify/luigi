import doctest
import unittest

import luigi.task


class TargetTest(unittest.TestCase):
	
	def test_tasks_doctest(self):
		doctest.testmod(luigi.task)
