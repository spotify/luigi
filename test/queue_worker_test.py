import luigi
import luigi.queue_worker
import unittest


class ExampleTask(luigi.Task):
    pass


class TaskWithArgs(luigi.Task):
    string_arg = luigi.Parameter()


class QueueWorkerTest(unittest.TestCase):

    def test_queue_worker_requires(self):
        queue_worker = luigi.queue_worker.QueueWorker(args=['ExampleTask'])
        self.assertEquals(queue_worker.requires(), [ExampleTask()])

    def test_queue_worker_missing_args(self):
        queue_worker = luigi.queue_worker.QueueWorker(args=['TaskWithArgs'])
        self.assertRaises(Exception, queue_worker.requires)

    def test_queue_worker_with_args(self):
        queue_worker = luigi.queue_worker.QueueWorker(args=['TaskWithArgs', '--string-arg', 'foo'])
        self.assertEquals(queue_worker.requires(), [TaskWithArgs(string_arg='foo')])
