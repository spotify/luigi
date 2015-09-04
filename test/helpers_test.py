import luigi
import luigi.date_interval
import luigi.interface
import luigi.notifications
from helpers import LuigiTestCase, RunOnceTask


class LuigiTestCaseTest(LuigiTestCase):

    def test_1(self):
        class MyClass(luigi.Task):
            pass

        self.assertTrue(self.run_locally(['MyClass']))

    def test_2(self):
        class MyClass(luigi.Task):
            pass

        self.assertTrue(self.run_locally(['MyClass']))


class RunOnceTaskTest(LuigiTestCase):

    def test_complete_behavior(self):
        """
        Verify that RunOnceTask works as expected.

        This task will fail if it was a normal ``luigi.Task``, because
        RequiringTask wouldn't run becaue missing depedency at runtime.
        """
        class MyTask(RunOnceTask):
            pass

        class RequiringTask(luigi.Task):
            counter = 0

            def requires(self):
                yield MyTask()

            def run(self):
                RequiringTask.counter += 1

        self.run_locally(['RequiringTask'])
        self.assertEqual(1, RequiringTask.counter)
