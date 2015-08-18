# -*- coding: utf-8 -*-
#
# Copyright 2015-2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import mock
from helpers import unittest, LuigiTestCase

from luigi import six

import luigi
import luigi.worker
import luigi.execution_summary
import threading
import datetime


class ExecutionSummaryTest(LuigiTestCase):

    def setUp(self):
        super(ExecutionSummaryTest, self).setUp()
        self.scheduler = luigi.scheduler.CentralPlannerScheduler(prune_on_get_work=False)
        self.worker = luigi.worker.Worker(scheduler=self.scheduler)

    def run_task(self, task):
        self.worker.add(task)  # schedule
        self.worker.run()  # run

    def summary_dict(self):
        return luigi.execution_summary._summary_dict(self.worker)

    def summary(self):
        return luigi.execution_summary.summary(self.worker)

    def test_all_statuses(self):
        class Bar(luigi.Task):
            num = luigi.IntParameter()

            def run(self):
                if self.num == 0:
                    raise ValueError()

            def complete(self):
                if self.num == 1:
                    return True
                return False

        class Foo(luigi.Task):
            def run(self):
                pass

            def requires(self):
                for i in range(5):
                    yield Bar(i)

        self.run_task(Foo())
        d = self.summary_dict()
        self.assertEqual({Bar(num=1)}, d['already_done'])
        self.assertEqual({Bar(num=2), Bar(num=3), Bar(num=4)}, d['completed'])
        self.assertEqual({Bar(num=0)}, d['failed'])
        self.assertEqual({Foo()}, d['upstream_failure'])
        self.assertFalse(d['upstream_missing_dependency'])
        self.assertFalse(d['run_by_other_worker'])
        self.assertFalse(d['still_pending_ext'])
        s = self.summary()
        self.assertIn('\n* 3 ran successfully:\n    - 3 Bar(num=', s)
        self.assertIn('\n* 1 present dependencies were encountered:\n    - 1 Bar(num=1)\n', s)
        self.assertIn('\n* 1 failed:\n    - 1 Bar(num=0)\n* 1 were left pending, among these:\n    * 1 had failed dependencies:\n        - 1 Foo()\n\nThis progress looks :( because there were failed tasks', s)
        self.assertNotIn('\n\n\n', s)

    def test_upstream_not_running(self):
        class ExternalBar(luigi.ExternalTask):
            num = luigi.IntParameter()

            def complete(self):
                if self.num == 1:
                    return True
                return False

        class Bar(luigi.Task):
            num = luigi.IntParameter()

            def run(self):
                if self.num == 0:
                    raise ValueError()

        class Foo(luigi.Task):
            def run(self):
                pass

            def requires(self):
                for i in range(5):
                    yield ExternalBar(i)
                    yield Bar(i)

        self.run_task(Foo())
        d = self.summary_dict()
        self.assertEqual({ExternalBar(num=1)}, d['already_done'])
        self.assertEqual({Bar(num=1), Bar(num=2), Bar(num=3), Bar(num=4)}, d['completed'])
        self.assertEqual({Bar(num=0)}, d['failed'])
        self.assertEqual({Foo()}, d['upstream_failure'])
        self.assertEqual({Foo()}, d['upstream_missing_dependency'])
        self.assertFalse(d['run_by_other_worker'])
        self.assertEqual({ExternalBar(num=0), ExternalBar(num=2), ExternalBar(num=3), ExternalBar(num=4)}, d['still_pending_ext'])
        s = self.summary()
        self.assertIn('\n* 1 present dependencies were encountered:\n    - 1 ExternalBar(num=1)\n', s)
        self.assertIn('\n* 4 ran successfully:\n    - 4 Bar(num=1...4)\n', s)
        self.assertIn('\n* 1 failed:\n    - 1 Bar(num=0)\n', s)
        self.assertIn('\n* 5 were left pending, among these:\n    * 4 were missing external dependencies:\n        - 4 ExternalBar(num=', s)
        self.assertIn('\n    * 1 had failed dependencies:\n        - 1 Foo()\n    * 1 had missing external dependencies:\n        - 1 Foo()\n\nThis progress looks :( because there were failed tasks\n', s)
        self.assertNotIn('\n\n\n', s)

    def test_already_running(self):
        lock1 = threading.Lock()
        lock2 = threading.Lock()

        class ParentTask(luigi.Task):
            def __init__(self, *args, **kwargs):
                super(ParentTask, self).__init__(*args, **kwargs)
                self.comp = False

            def complete(self):
                return self.comp

            def run(self):
                self.comp = True

            def requires(self):
                yield LockTask()

        class LockTask(luigi.Task):
            def __init__(self, *args, **kwargs):
                super(LockTask, self).__init__(*args, **kwargs)
                self.comp = False

            def complete(self):
                return self.comp

            def run(self):
                lock2.release()
                lock1.acquire()
                self.comp = True

        lock1.acquire()
        lock2.acquire()
        other_worker = luigi.worker.Worker(scheduler=self.scheduler, worker_id="other_worker")
        other_worker.add(ParentTask())
        t1 = threading.Thread(target=other_worker.run)
        t1.start()
        lock2.acquire()
        self.run_task(ParentTask())
        lock1.release()
        t1.join()
        d = self.summary_dict()
        self.assertEqual({LockTask()}, d['run_by_other_worker'])
        self.assertEqual({ParentTask()}, d['upstream_run_by_other_worker'])
        s = self.summary()
        self.assertIn('\nScheduled 2 tasks of which:\n* 2 were left pending, among these:\n    * 1 were being run by another worker:\n        - 1 LockTask()\n    * 1 had dependencies that were being run by other worker:\n        - 1 ParentTask()\n', s)
        self.assertIn('\n\nThe other workers were:\n    - other_worker ran 1 tasks\n\nDid not run any tasks\nThis progress looks :) because there were no failed tasks or missing external dependencies\n', s)
        self.assertNotIn('\n\n\n', s)

    def test_larger_tree(self):

        class Dog(luigi.Task):
            def __init__(self, *args, **kwargs):
                super(Dog, self).__init__(*args, **kwargs)
                self.comp = False

            def complete(self):
                return self.comp

            def run(self):
                self.comp = True

            def requires(self):
                yield Cat(2)

        class Cat(luigi.Task):
            num = luigi.IntParameter()

            def __init__(self, *args, **kwargs):
                super(Cat, self).__init__(*args, **kwargs)
                self.comp = False

            def run(self):
                if self.num == 2:
                    raise ValueError()
                self.comp = True

            def complete(self):
                if self.num == 1:
                    return True
                else:
                    return self.comp

        class Bar(luigi.Task):
            num = luigi.IntParameter()

            def __init__(self, *args, **kwargs):
                super(Bar, self).__init__(*args, **kwargs)
                self.comp = False

            def complete(self):
                return self.comp

            def run(self):
                self.comp = True

            def requires(self):
                if self.num == 0:
                    yield ExternalBar()
                    yield Cat(0)
                if self.num == 1:
                    yield Cat(0)
                    yield Cat(1)
                if self.num == 2:
                    yield Dog()

        class Foo(luigi.Task):
            def run(self):
                pass

            def requires(self):
                for i in range(3):
                    yield Bar(i)

        class ExternalBar(luigi.ExternalTask):

            def complete(self):
                return False

        self.run_task(Foo())
        d = self.summary_dict()

        self.assertEqual({Cat(num=1)}, d['already_done'])
        self.assertEqual({Cat(num=0), Bar(num=1)}, d['completed'])
        self.assertEqual({Cat(num=2)}, d['failed'])
        self.assertEqual({Dog(), Bar(num=2), Foo()}, d['upstream_failure'])
        self.assertEqual({Bar(num=0), Foo()}, d['upstream_missing_dependency'])
        self.assertFalse(d['run_by_other_worker'])
        self.assertEqual({ExternalBar()}, d['still_pending_ext'])
        s = self.summary()
        self.assertNotIn('\n\n\n', s)

    def test_with_dates(self):
        """ Just test that it doesn't crash with date params """

        start = datetime.date(1998, 3, 23)

        class Bar(luigi.Task):
            date = luigi.DateParameter()

            def __init__(self, *args, **kwargs):
                super(Bar, self).__init__(*args, **kwargs)
                self.comp = False

            def run(self):
                self.comp = True

            def complete(self):
                return self.comp

        class Foo(luigi.Task):

            def run(self):
                pass

            def requires(self):
                for i in range(10):
                    new_date = start + datetime.timedelta(days=i)
                    yield Bar(date=new_date)

        self.run_task(Foo())
        d = self.summary_dict()
        exp_set = {Bar(start + datetime.timedelta(days=i)) for i in range(10)}
        exp_set.add(Foo())
        self.assertEqual(exp_set, d['completed'])
        s = self.summary()
        self.assertIn('date=1998-0', s)
        self.assertIn('Scheduled 11 tasks', s)
        self.assertIn('Luigi Execution Summary', s)
        self.assertNotIn('00:00:00', s)
        self.assertNotIn('\n\n\n', s)

    def test_with_ranges_minutes(self):

        start = datetime.datetime(1998, 3, 23, 1, 50)

        class Bar(luigi.Task):
            time = luigi.DateMinuteParameter()

            def __init__(self, *args, **kwargs):
                super(Bar, self).__init__(*args, **kwargs)
                self.comp = False

            def run(self):
                self.comp = True

            def complete(self):
                return self.comp

        class Foo(luigi.Task):

            def run(self):
                pass

            def requires(self):
                for i in range(300):
                    new_time = start + datetime.timedelta(minutes=i)
                    yield Bar(time=new_time)

        self.run_task(Foo())
        d = self.summary_dict()
        exp_set = {Bar(start + datetime.timedelta(minutes=i)) for i in range(300)}
        exp_set.add(Foo())
        self.assertEqual(exp_set, d['completed'])
        s = self.summary()
        self.assertIn('Bar(time=1998-03-23T0150...1998-03-23T0649)', s)
        self.assertNotIn('\n\n\n', s)

    def test_with_ranges_one_param(self):

        class Bar(luigi.Task):
            num = luigi.IntParameter()

            def __init__(self, *args, **kwargs):
                super(Bar, self).__init__(*args, **kwargs)
                self.comp = False

            def run(self):
                self.comp = True

            def complete(self):
                return self.comp

        class Foo(luigi.Task):

            def run(self):
                pass

            def requires(self):
                for i in range(11):
                    yield Bar(i)

        self.run_task(Foo())
        d = self.summary_dict()
        exp_set = {Bar(i) for i in range(11)}
        exp_set.add(Foo())
        self.assertEqual(exp_set, d['completed'])
        s = self.summary()
        self.assertIn('Bar(num=0...10)', s)
        self.assertNotIn('\n\n\n', s)

    def test_with_ranges_multiple_params(self):

        class Bar(luigi.Task):
            num1 = luigi.IntParameter()
            num2 = luigi.IntParameter()
            num3 = luigi.IntParameter()

            def __init__(self, *args, **kwargs):
                super(Bar, self).__init__(*args, **kwargs)
                self.comp = False

            def run(self):
                self.comp = True

            def complete(self):
                return self.comp

        class Foo(luigi.Task):

            def run(self):
                pass

            def requires(self):
                for i in range(5):
                    yield Bar(5, i, 25)

        self.run_task(Foo())
        d = self.summary_dict()
        exp_set = {Bar(5, i, 25) for i in range(5)}
        exp_set.add(Foo())
        self.assertEqual(exp_set, d['completed'])
        s = self.summary()
        self.assertIn('- 5 Bar(num1=5, num2=0...4, num3=25)', s)
        self.assertNotIn('\n\n\n', s)

    def test_with_two_tasks(self):

        class Bar(luigi.Task):
            num = luigi.IntParameter()
            num2 = luigi.IntParameter()

            def __init__(self, *args, **kwargs):
                super(Bar, self).__init__(*args, **kwargs)
                self.comp = False

            def run(self):
                self.comp = True

            def complete(self):
                return self.comp

        class Foo(luigi.Task):

            def run(self):
                pass

            def requires(self):
                for i in range(2):
                    yield Bar(i, 2 * i)

        self.run_task(Foo())
        d = self.summary_dict()
        self.assertEqual({Foo(), Bar(num=0, num2=0), Bar(num=1, num2=2)}, d['completed'])
        s = self.summary()
        self.assertIn(') and Bar(num=', s)
        self.assertIn('- Bar(num=', s)
        self.assertNotIn("Did not run any tasks", s)
        self.assertNotIn('\n\n\n', s)

    def test_really_long_param_name(self):

        class Bar(luigi.Task):
            This_is_a_really_long_parameter_that_we_should_not_print_out_because_people_will_get_annoyed = luigi.IntParameter()

            def __init__(self, *args, **kwargs):
                super(Bar, self).__init__(*args, **kwargs)
                self.comp = False

            def run(self):
                self.comp = True

            def complete(self):
                return self.comp

        class Foo(luigi.Task):

            def run(self):
                pass

            def requires(self):
                yield Bar(0)

        self.run_task(Foo())
        s = self.summary()
        self.assertIn('Bar(...)', s)
        self.assertNotIn("Did not run any tasks", s)
        self.assertNotIn('\n\n\n', s)

    def test_multiple_params_multiple_same_task_family(self):

        class Bar(luigi.Task):
            num = luigi.IntParameter()
            num2 = luigi.IntParameter()

            def __init__(self, *args, **kwargs):
                super(Bar, self).__init__(*args, **kwargs)
                self.comp = False

            def run(self):
                self.comp = True

            def complete(self):
                return self.comp

        class Foo(luigi.Task):

            def run(self):
                pass

            def requires(self):
                for i in range(4):
                    yield Bar(i, 2 * i)

        self.run_task(Foo())
        s = self.summary()
        self.assertIn('- Bar(', s)
        self .assertIn(' and 3 other Bar', s)
        self.assertNotIn("Did not run any tasks", s)
        self.assertNotIn('\n\n\n', s)

    def test_happy_smiley_face_normal(self):

        class Bar(luigi.Task):
            num = luigi.IntParameter()
            num2 = luigi.IntParameter()

            def __init__(self, *args, **kwargs):
                super(Bar, self).__init__(*args, **kwargs)
                self.comp = False

            def run(self):
                self.comp = True

            def complete(self):
                return self.comp

        class Foo(luigi.Task):

            def run(self):
                pass

            def requires(self):
                for i in range(4):
                    yield Bar(i, 2 * i)

        self.run_task(Foo())
        s = self.summary()
        self.assertIn('\nThis progress looks :) because there were no failed tasks or missing external dependencies', s)
        self.assertNotIn("Did not run any tasks", s)
        self.assertNotIn('\n\n\n', s)

    def test_happy_smiley_face_other_workers(self):
        lock1 = threading.Lock()
        lock2 = threading.Lock()

        class ParentTask(luigi.Task):
            def __init__(self, *args, **kwargs):
                super(ParentTask, self).__init__(*args, **kwargs)
                self.comp = False

            def complete(self):
                return self.comp

            def run(self):
                self.comp = True

            def requires(self):
                yield LockTask()

        class LockTask(luigi.Task):
            def __init__(self, *args, **kwargs):
                super(LockTask, self).__init__(*args, **kwargs)
                self.comp = False

            def complete(self):
                return self.comp

            def run(self):
                lock2.release()
                lock1.acquire()
                self.comp = True

        lock1.acquire()
        lock2.acquire()
        other_worker = luigi.worker.Worker(scheduler=self.scheduler, worker_id="other_worker")
        other_worker.add(ParentTask())
        t1 = threading.Thread(target=other_worker.run)
        t1.start()
        lock2.acquire()
        self.run_task(ParentTask())
        lock1.release()
        t1.join()
        s = self.summary()
        self.assertIn('\nThis progress looks :) because there were no failed tasks or missing external dependencies', s)
        self.assertNotIn('\n\n\n', s)

    def test_sad_smiley_face(self):

        class ExternalBar(luigi.ExternalTask):

            def complete(self):
                return False

        class Bar(luigi.Task):
            num = luigi.IntParameter()

            def run(self):
                if self.num == 0:
                    raise ValueError()

        class Foo(luigi.Task):
            def run(self):
                pass

            def requires(self):
                for i in range(5):
                    yield Bar(i)
                yield ExternalBar()

        self.run_task(Foo())
        s = self.summary()
        self.assertIn('\nThis progress looks :( because there were failed tasks', s)
        self.assertNotIn("Did not run any tasks", s)
        self.assertNotIn('\n\n\n', s)

    def test_neutral_smiley_face(self):

        class ExternalBar(luigi.ExternalTask):

            def complete(self):
                return False

        class Foo(luigi.Task):

            def run(self):
                pass

            def requires(self):
                yield ExternalBar()

        self.run_task(Foo())
        s = self.summary()
        self.assertIn('\nThis progress looks :| because there were missing external dependencies', s)
        self.assertNotIn('\n\n\n', s)

    def test_did_not_run_any_tasks(self):

        class ExternalBar(luigi.ExternalTask):
            num = luigi.IntParameter()

            def complete(self):
                if self.num == 5:
                    return True
                return False

        class Foo(luigi.Task):

            def run(self):
                pass

            def requires(self):
                for i in range(10):
                    yield ExternalBar(i)

        self.run_task(Foo())
        d = self.summary_dict()
        self.assertEqual({ExternalBar(5)}, d['already_done'])
        self.assertEqual({ExternalBar(i) for i in range(10) if i != 5}, d['still_pending_ext'])
        self.assertEqual({Foo()}, d['upstream_missing_dependency'])
        s = self.summary()
        self.assertIn('\n\nDid not run any tasks\nThis progress looks :| because there were missing external dependencies', s)
        self.assertNotIn('\n\n\n', s)

    def test_example(self):

        class MyExternal(luigi.ExternalTask):

            def complete(self):
                return False

        class Boom(luigi.Task):
            this_is_a_really_long_I_mean_way_too_long_and_annoying_parameter = luigi.IntParameter()

            def run(self):
                pass

            def requires(self):
                for i in range(5, 200):
                    yield Bar(i)

        class Foo(luigi.Task):
            num = luigi.IntParameter()
            num2 = luigi.IntParameter()

            def run(self):
                pass

            def requires(self):
                yield MyExternal()
                yield Boom(0)

        class Bar(luigi.Task):
            num = luigi.IntParameter()

            def complete(self):
                return True

            def run(self):
                pass

        class DateTask(luigi.Task):
            date = luigi.DateParameter()
            num = luigi.IntParameter()

            def run(self):
                pass

            def requires(self):
                yield MyExternal()
                yield Boom(0)

        class EntryPoint(luigi.Task):

            def run(self):
                pass

            def requires(self):
                for i in range(10):
                    yield Foo(100, 2 * i)
                for i in range(10):
                    yield DateTask(datetime.date(1998, 3, 23) + datetime.timedelta(days=i), 5)

        self.run_task(EntryPoint())
        s = self.summary()
        self.assertIn('\n\nScheduled 218 tasks of which:\n', s)
        self.assertIn('\n* 195 present dependencies were encountered:\n', s)
        self.assertIn('\n* 1 ran successfully:\n', s)
        self.assertIn('\n    - 1 Boom(...)\n', s)
        self.assertIn('\n* 22 were left pending, among these:\n', s)
        self.assertIn('\n    * 1 were missing external dependencies:\n', s)
        self.assertIn('\n        - 1 MyExternal()\n', s)
        self.assertIn('\n    * 21 had missing external dependencies:\n', s)
        self.assertIn('\n        - 1 EntryPoint()\n', s)
        self.assertIn('\n        - Foo(num=', s)
        self.assertIn(') and 9 other Foo\n', s)
        self.assertIn('\n        - 10 DateTask(date=1998-03-23...1998-04-01, num=5)\n', s)
        self.assertIn('\nThis progress looks :| because there were missing external dependencies\n\n', s)
        self.assertNotIn('\n\n\n', s)
