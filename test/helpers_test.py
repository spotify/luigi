import luigi
import luigi.date_interval
import luigi.interface
import luigi.notifications
from helpers import LuigiTestCase


class LuigiTestCaseTest(LuigiTestCase):

    def test_1(self):
        class MyClass(luigi.Task):
            pass

        self.assertTrue(self.run_locally(['MyClass']))

    def test_2(self):
        class MyClass(luigi.Task):
            pass

        self.assertTrue(self.run_locally(['MyClass']))
