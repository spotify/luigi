from helpers import unittest

import luigi
from multiprocessing import Queue
from luigi.worker import Worker
from luigi.worker import ForkHandlerType
from luigi.scheduler import CentralPlannerScheduler
import time


class ForkHandlerTest(unittest.TestCase):
    def setUp(self):

        @Worker.fork_handler()
        def both():
            '''
            to be called twice - one as master process fork handler
            secon as a child process fork handler
            thus in both queues we will have ['both', 'both'] after calling handlers
            '''
            self.worker_queue.put("both")
            self.child_queue.put("both")

        @Worker.fork_handler(handler_type=ForkHandlerType.Master)
        def worker():
            self.worker_queue.put("master")

        @Worker.fork_handler(handler_type=ForkHandlerType.Child)
        def child():
            self.child_queue.put("child")

        self.worker_queue = Queue()
        self.child_queue = Queue()

    def test_single_threaded_worker_doesnot_execute_fork_handler(self):
        class Stub(luigi.Task):
            def complete(self):
                return False

            def run(self):
                pass

        s = CentralPlannerScheduler()
        w = Worker(scheduler=s, worker_processes=1)
        w.add(Stub())
        w.run()

        time.sleep(0.1)

        self.assertTrue(self.worker_queue.empty())
        self.assertTrue(self.child_queue.empty())

    def test_worker_executes_fork_handler(self):
        class Stub(luigi.Task):
            def complete(self):
                return False

            def run(self):
                pass

        s = CentralPlannerScheduler()
        w = Worker(scheduler=s, worker_processes=2)
        w.add(Stub())
        w.run()

        time.sleep(0.1)

        worker_res = [self.worker_queue.get(), self.worker_queue.get(), self.worker_queue.get()]
        self.assertIn("both", worker_res)
        self.assertIn("master", worker_res)

        child_res = [self.child_queue.get(), self.child_queue.get(), self.child_queue.get()]
        self.assertIn("both", child_res)
        self.assertIn("child", child_res)

        self.assertTrue(self.worker_queue.empty())
        self.assertTrue(self.child_queue.empty())
