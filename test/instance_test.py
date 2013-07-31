# Copyright (c) 2012 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import luigi
import luigi.date_interval
import unittest
import luigi.notifications
luigi.notifications.DEBUG = True


class InstanceTest(unittest.TestCase):
    def test_simple(self):
        class DummyTask(luigi.Task):
            x = luigi.Parameter()

        dummy_1 = DummyTask(1)
        dummy_2 = DummyTask(2)
        dummy_1b = DummyTask(1)

        self.assertNotEqual(dummy_1, dummy_2)
        self.assertEqual(dummy_1, dummy_1b)

    def test_dep(self):
        test = self

        class A(luigi.Task):
            def __init__(self):
                self.has_run = False
                super(A, self).__init__()

            def run(self):
                self.has_run = True

        class B(luigi.Task):
            x = luigi.Parameter()

            def requires(self):
                return A()  # This will end up referring to the same object

            def run(self):
                test.assertTrue(self.requires().has_run)

        w = luigi.worker.Worker()
        w.add(B(1))
        w.add(B(2))
        w.run()
        w.stop()

    def test_external_instance_cache(self):
        class A(luigi.Task):
            pass

        class OtherA(luigi.ExternalTask):
            task_family = "A"

        oa = OtherA()
        a = A()
        self.assertNotEqual(oa, a)

    def test_date(self):
        ''' Adding unit test because we had a problem with this '''
        class DummyTask(luigi.Task):
            x = luigi.DateIntervalParameter()

        dummy_1 = DummyTask(luigi.date_interval.Year(2012))
        dummy_2 = DummyTask(luigi.date_interval.Year(2013))
        dummy_1b = DummyTask(luigi.date_interval.Year(2012))

        self.assertNotEqual(dummy_1, dummy_2)
        self.assertEqual(dummy_1, dummy_1b)

if __name__ == '__main__':
    unittest.main()
