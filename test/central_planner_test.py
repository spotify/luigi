import datetime, os
from luigi.central_planner import CentralPlannerScheduler
import unittest

class CentralPlannerTest(unittest.TestCase):
    def setUp(self):
        self.sch = CentralPlannerScheduler()

    def test_dep(self):
        self.sch.add_task('B')
        self.sch.add_task('A')
        self.sch.add_dep('B', 'A')

        self.assertEqual(self.sch.get_work(), (False, 'A'))
        self.sch.status('A', 'DONE')
        self.assertEqual(self.sch.get_work(), (False, 'B'))
        self.sch.status('B', 'DONE')
        self.assertEqual(self.sch.get_work(), (True, None))

    def test_two_clients(self):
        # Client X wants to build A -> B
        # Client Y wants to build A -> C
        self.sch.add_task('A', client='X')
        self.sch.add_task('A', client='Y')
        self.sch.add_task('B', client='X')
        self.sch.add_task('C', client='Y')
        self.sch.add_dep('B', 'A')
        self.sch.add_dep('C', 'A')

        self.assertEqual(self.sch.get_work(client='X'), (False, 'A'))
        self.assertEqual(self.sch.get_work(client='Y'), (False, None)) # Client Y is pending on A to be done
        self.sch.status('A', 'DONE')
        self.assertEqual(self.sch.get_work(client='Y'), (False, 'C'))
        self.assertEqual(self.sch.get_work(client='X'), (False, 'B'))

