import doctest
import unittest

import luigi.task


class TaskTest(unittest.TestCase):
	
	def test_tasks_doctest(self):
		doctest.testmod(luigi.task)
