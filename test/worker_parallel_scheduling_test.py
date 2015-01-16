import pickle
import time
import unittest
import mock

import luigi

from luigi.worker import Worker


class SlowCompleteWrapper(luigi.WrapperTask):
    def requires(self):
        return [SlowCompleteTask(i) for i in range(4)]


class SlowCompleteTask(luigi.Task):
    n = luigi.IntParameter()

    def complete(self):
        time.sleep(0.1)
        return True


class OverlappingSelfDependenciesTask(luigi.Task):
    n = luigi.IntParameter()
    k = luigi.IntParameter()

    def complete(self):
        return self.n < self.k or self.k == 0

    def requires(self):
        return [OverlappingSelfDependenciesTask(self.n-1, k) for k in range(self.k+1)]


class ExceptionCompleteTask(luigi.Task):
    def complete(self):
        assert False


class ExceptionRequiresTask(luigi.Task):
    def requires(self):
        assert False


class UnpicklableExceptionTask(luigi.Task):
    def complete(self):
        class UnpicklableException(Exception):
            pass
        raise UnpicklableException()


class ParallelSchedulingTest(unittest.TestCase):
    def setUp(self):
        self.sch = mock.Mock()
        self.w = Worker(scheduler=self.sch, worker_id='x')

    def added_tasks(self, status):
        return [args[1] for args, kw in self.sch.add_task.call_args_list if kw['status'] == status]

    def test_multiprocess_scheduling_with_overlapping_dependencies(self):
        self.w.add(OverlappingSelfDependenciesTask(5, 2), True)
        self.assertEqual(15, self.sch.add_task.call_count)
        self.assertEqual(set((
            'OverlappingSelfDependenciesTask(n=1, k=1)',
            'OverlappingSelfDependenciesTask(n=2, k=1)',
            'OverlappingSelfDependenciesTask(n=2, k=2)',
            'OverlappingSelfDependenciesTask(n=3, k=1)',
            'OverlappingSelfDependenciesTask(n=3, k=2)',
            'OverlappingSelfDependenciesTask(n=4, k=1)',
            'OverlappingSelfDependenciesTask(n=4, k=2)',
            'OverlappingSelfDependenciesTask(n=5, k=2)',
        )), set(self.added_tasks('PENDING')))
        self.assertEqual(set((
            'OverlappingSelfDependenciesTask(n=0, k=0)',
            'OverlappingSelfDependenciesTask(n=0, k=1)',
            'OverlappingSelfDependenciesTask(n=1, k=0)',
            'OverlappingSelfDependenciesTask(n=1, k=2)',
            'OverlappingSelfDependenciesTask(n=2, k=0)',
            'OverlappingSelfDependenciesTask(n=3, k=0)',
            'OverlappingSelfDependenciesTask(n=4, k=0)',
        )), set(self.added_tasks('DONE')))

    @mock.patch('luigi.notifications.send_error_email')
    def test_raise_exception_in_complete(self, send):
        self.w.add(ExceptionCompleteTask(), multiprocess=True)
        send.assert_called_once()
        self.assertEqual(0, self.sch.add_task.call_count)
        self.assertTrue('assert False' in send.call_args[0][1])

    @mock.patch('luigi.notifications.send_error_email')
    def test_raise_unpicklable_exception_in_complete(self, send):
        # verify exception can't be pickled
        self.assertRaises(Exception, UnpicklableExceptionTask().complete)
        try:
            UnpicklableExceptionTask().complete()
        except Exception as ex:
            pass
        self.assertRaises(pickle.PicklingError, pickle.dumps, ex)

        # verify this can run async
        self.w.add(UnpicklableExceptionTask(), multiprocess=True)
        send.assert_called_once()
        self.assertEqual(0, self.sch.add_task.call_count)
        self.assertTrue('raise UnpicklableException()' in send.call_args[0][1])

    @mock.patch('luigi.notifications.send_error_email')
    def test_raise_exception_in_requires(self, send):
        self.w.add(ExceptionRequiresTask(), multiprocess=True)
        send.assert_called_once()
        self.assertEqual(0, self.sch.add_task.call_count)


if __name__ == '__main__':
    unittest.main()
