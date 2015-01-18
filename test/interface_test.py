from mock import Mock
import luigi
import luigi.date_interval
import unittest
from luigi.interface import Interface, WorkerSchedulerFactory, EnvironmentParamsContainer
import luigi.notifications
from luigi.worker import Worker

luigi.notifications.DEBUG = True


class InterfaceTest(unittest.TestCase):
    def setUp(self):
        self.worker = Worker()
        self.worker.stop = Mock()

        self.worker_scheduler_factory = WorkerSchedulerFactory()
        self.worker_scheduler_factory.create_worker = Mock(return_value=self.worker)
        self.worker_scheduler_factory.create_local_scheduler = Mock()

        class NoOpTask(luigi.Task):
            param = luigi.Parameter()

        self.task_a = NoOpTask("a")
        self.task_b = NoOpTask("b")

    def test_interface_run_positive_path(self):
        self.worker.add = Mock(side_effect=[True, True])
        self.worker.run = Mock(return_value=True)

        self.assertTrue(self._run_interface())

    def test_interface_run_with_add_failure(self):
        self.worker.add = Mock(side_effect=[True, False])
        self.worker.run = Mock(return_value=True)

        self.assertFalse(self._run_interface())

    def test_interface_run_with_run_failure(self):
        self.worker.add = Mock(side_effect=[True, True])
        self.worker.run = Mock(return_value=False)

        self.assertFalse(self._run_interface())

    def _run_interface(self):
        return Interface.run([self.task_a, self.task_b], self.worker_scheduler_factory, {'no_lock': True})


if __name__ == '__main__':
    unittest.main()
