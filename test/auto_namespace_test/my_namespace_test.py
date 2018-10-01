import luigi
from helpers import LuigiTestCase


class MyNamespaceTest(LuigiTestCase):
    def test_auto_namespace_scope(self):
        class MyTask(luigi.Task):
            pass
        self.assertTrue(self.run_locally(['auto_namespace_test.my_namespace_test.MyTask']))
        self.assertEqual(MyTask.get_task_namespace(), 'auto_namespace_test.my_namespace_test')
