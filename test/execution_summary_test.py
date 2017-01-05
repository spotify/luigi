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

from helpers import LuigiTestCase, RunOnceTask, with_config

import luigi
import luigi.worker
import luigi.execution_summary
import threading
import datetime
import mock
from enum import Enum


class ExecutionSummaryTest(LuigiTestCase):

    def setUp(self):
        super(ExecutionSummaryTest, self).setUp()
        self.scheduler = luigi.scheduler.Scheduler(prune_on_get_work=False)
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
        summary = self.summary()

        expected = ['',
                    '===== Luigi Execution Summary =====',
                    '',
                    'Scheduled 6 tasks of which:',
                    '* 1 present dependencies were encountered:',
                    '    - 1 Bar(num=1)',
                    '* 3 ran successfully:',
                    '    - 3 Bar(num=2,3,4)',
                    '* 1 failed:',
                    '    - 1 Bar(num=0)',
                    '* 1 were left pending, among these:',
                    '    * 1 had failed dependencies:',
                    '        - 1 Foo()',
                    '',
                    'This progress looks :( because there were failed tasks',
                    '',
                    '===== Luigi Execution Summary =====',
                    '']
        result = summary.split('\n')
        self.assertEqual(len(result), len(expected))
        for i, line in enumerate(result):
            self.assertEqual(line, expected[i])

    def test_batch_complete(self):
        ran_tasks = set()

        class MaxBatchTask(luigi.Task):
            param = luigi.IntParameter(batch_method=max)

            def run(self):
                ran_tasks.add(self.param)

            def complete(self):
                return any(self.param <= ran_param for ran_param in ran_tasks)

        class MaxBatches(luigi.WrapperTask):
            def requires(self):
                return map(MaxBatchTask, range(5))

        self.run_task(MaxBatches())
        d = self.summary_dict()
        expected_completed = {
            MaxBatchTask(0),
            MaxBatchTask(1),
            MaxBatchTask(2),
            MaxBatchTask(3),
            MaxBatchTask(4),
            MaxBatches(),
        }
        self.assertEqual(expected_completed, d['completed'])

    def test_batch_fail(self):
        class MaxBatchFailTask(luigi.Task):
            param = luigi.IntParameter(batch_method=max)

            def run(self):
                assert self.param < 4

            def complete(self):
                return False

        class MaxBatches(luigi.WrapperTask):
            def requires(self):
                return map(MaxBatchFailTask, range(5))

        self.run_task(MaxBatches())
        d = self.summary_dict()
        expected_failed = {
            MaxBatchFailTask(0),
            MaxBatchFailTask(1),
            MaxBatchFailTask(2),
            MaxBatchFailTask(3),
            MaxBatchFailTask(4),
        }
        self.assertEqual(expected_failed, d['failed'])

    def test_check_complete_error(self):
        class Bar(luigi.Task):
            def run(self):
                pass

            def complete(self):
                raise Exception
                return True

        class Foo(luigi.Task):
            def requires(self):
                yield Bar()

        self.run_task(Foo())
        d = self.summary_dict()
        self.assertEqual({Foo()}, d['still_pending_not_ext'])
        self.assertEqual({Foo()}, d['upstream_scheduling_error'])
        self.assertEqual({Bar()}, d['scheduling_error'])
        self.assertFalse(d['not_run'])
        self.assertFalse(d['already_done'])
        self.assertFalse(d['completed'])
        self.assertFalse(d['failed'])
        self.assertFalse(d['upstream_failure'])
        self.assertFalse(d['upstream_missing_dependency'])
        self.assertFalse(d['run_by_other_worker'])
        self.assertFalse(d['still_pending_ext'])
        summary = self.summary()
        expected = ['',
                    '===== Luigi Execution Summary =====',
                    '',
                    'Scheduled 2 tasks of which:',
                    '* 1 failed scheduling:',
                    '    - 1 Bar()',
                    '* 1 were left pending, among these:',
                    "    * 1 had dependencies whose scheduling failed:",
                    '        - 1 Foo()',
                    '',
                    'Did not run any tasks',
                    'This progress looks :( because there were tasks whose scheduling failed',
                    '',
                    '===== Luigi Execution Summary =====',
                    '']
        result = summary.split('\n')
        self.assertEqual(len(result), len(expected))
        for i, line in enumerate(result):
            self.assertEqual(line, expected[i])

    def test_not_run_error(self):
        class Bar(luigi.Task):
            def complete(self):
                return True

        class Foo(luigi.Task):
            def requires(self):
                yield Bar()

        def new_func(*args, **kwargs):
            return None

        with mock.patch('luigi.scheduler.Scheduler.add_task', new_func):
            self.run_task(Foo())

        d = self.summary_dict()
        self.assertEqual({Foo()}, d['still_pending_not_ext'])
        self.assertEqual({Foo()}, d['not_run'])
        self.assertEqual({Bar()}, d['already_done'])
        self.assertFalse(d['upstream_scheduling_error'])
        self.assertFalse(d['scheduling_error'])
        self.assertFalse(d['completed'])
        self.assertFalse(d['failed'])
        self.assertFalse(d['upstream_failure'])
        self.assertFalse(d['upstream_missing_dependency'])
        self.assertFalse(d['run_by_other_worker'])
        self.assertFalse(d['still_pending_ext'])
        summary = self.summary()
        expected = ['',
                    '===== Luigi Execution Summary =====',
                    '',
                    'Scheduled 2 tasks of which:',
                    '* 1 present dependencies were encountered:',
                    '    - 1 Bar()',
                    '* 1 were left pending, among these:',
                    "    * 1 was not granted run permission by the scheduler:",
                    '        - 1 Foo()',
                    '',
                    'Did not run any tasks',
                    'This progress looks :| because there were tasks that were not granted run permission by the scheduler',
                    '',
                    '===== Luigi Execution Summary =====',
                    '']
        result = summary.split('\n')
        self.assertEqual(len(result), len(expected))
        for i, line in enumerate(result):
            self.assertEqual(line, expected[i])

    def test_deps_error(self):
        class Bar(luigi.Task):
            def run(self):
                pass

            def complete(self):
                return True

        class Foo(luigi.Task):
            def requires(self):
                raise Exception
                yield Bar()

        self.run_task(Foo())
        d = self.summary_dict()
        self.assertEqual({Foo()}, d['scheduling_error'])
        self.assertFalse(d['upstream_scheduling_error'])
        self.assertFalse(d['not_run'])
        self.assertFalse(d['already_done'])
        self.assertFalse(d['completed'])
        self.assertFalse(d['failed'])
        self.assertFalse(d['upstream_failure'])
        self.assertFalse(d['upstream_missing_dependency'])
        self.assertFalse(d['run_by_other_worker'])
        self.assertFalse(d['still_pending_ext'])
        summary = self.summary()
        expected = ['',
                    '===== Luigi Execution Summary =====',
                    '',
                    'Scheduled 1 tasks of which:',
                    '* 1 failed scheduling:',
                    '    - 1 Foo()',
                    '',
                    'Did not run any tasks',
                    'This progress looks :( because there were tasks whose scheduling failed',
                    '',
                    '===== Luigi Execution Summary =====',
                    '']
        result = summary.split('\n')
        self.assertEqual(len(result), len(expected))
        for i, line in enumerate(result):
            self.assertEqual(line, expected[i])

    @with_config({'execution_summary': {'summary-length': '1'}})
    def test_config_summary_limit(self):
        class Bar(luigi.Task):
            num = luigi.IntParameter()

            def run(self):
                pass

            def complete(self):
                return True

        class Biz(Bar):
            pass

        class Bat(Bar):
            pass

        class Wut(Bar):
            pass

        class Foo(luigi.Task):
            def requires(self):
                yield Bat(1)
                yield Wut(1)
                yield Biz(1)
                for i in range(4):
                    yield Bar(i)

            def complete(self):
                return False

        self.run_task(Foo())
        d = self.summary_dict()
        self.assertEqual({Bat(1), Wut(1), Biz(1), Bar(0), Bar(1), Bar(2), Bar(3)}, d['already_done'])
        self.assertEqual({Foo()}, d['completed'])
        self.assertFalse(d['failed'])
        self.assertFalse(d['upstream_failure'])
        self.assertFalse(d['upstream_missing_dependency'])
        self.assertFalse(d['run_by_other_worker'])
        self.assertFalse(d['still_pending_ext'])
        summary = self.summary()
        expected = ['',
                    '===== Luigi Execution Summary =====',
                    '',
                    'Scheduled 8 tasks of which:',
                    '* 7 present dependencies were encountered:',
                    '    - 4 Bar(num=0...3)',
                    '    ...',
                    '* 1 ran successfully:',
                    '    - 1 Foo()',
                    '',
                    'This progress looks :) because there were no failed tasks or missing external dependencies',
                    '',
                    '===== Luigi Execution Summary =====',
                    '']
        result = summary.split('\n')
        self.assertEqual(len(result), len(expected))
        for i, line in enumerate(result):
            self.assertEqual(line, expected[i])

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
        self.assertIn('\n    * 1 had failed dependencies:\n'
                      '        - 1 Foo()\n'
                      '    * 1 had missing external dependencies:\n'
                      '        - 1 Foo()\n\n'
                      'This progress looks :( because there were failed tasks\n', s)
        self.assertNotIn('\n\n\n', s)

    def test_already_running(self):
        lock1 = threading.Lock()
        lock2 = threading.Lock()

        class ParentTask(RunOnceTask):

            def requires(self):
                yield LockTask()

        class LockTask(RunOnceTask):
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
        self.assertIn('\nScheduled 2 tasks of which:\n'
                      '* 2 were left pending, among these:\n'
                      '    * 1 were being run by another worker:\n'
                      '        - 1 LockTask()\n'
                      '    * 1 had dependencies that were being run by other worker:\n'
                      '        - 1 ParentTask()\n', s)
        self.assertIn('\n\nThe other workers were:\n'
                      '    - other_worker ran 1 tasks\n\n'
                      'Did not run any tasks\n'
                      'This progress looks :) because there were no failed '
                      'tasks or missing external dependencies\n', s)
        self.assertNotIn('\n\n\n', s)

    def test_already_running_2(self):
        class AlreadyRunningTask(luigi.Task):
            def run(self):
                pass

        other_worker = luigi.worker.Worker(scheduler=self.scheduler, worker_id="other_worker")
        other_worker.add(AlreadyRunningTask())  # This also registers this worker
        old_func = luigi.scheduler.Scheduler.get_work

        def new_func(*args, **kwargs):
            new_kwargs = kwargs.copy()
            new_kwargs['worker'] = 'other_worker'
            old_func(*args, **new_kwargs)
            return old_func(*args, **kwargs)

        with mock.patch('luigi.scheduler.Scheduler.get_work', new_func):
            self.run_task(AlreadyRunningTask())

        d = self.summary_dict()
        self.assertFalse(d['already_done'])
        self.assertFalse(d['completed'])
        self.assertFalse(d['not_run'])
        self.assertEqual({AlreadyRunningTask()}, d['run_by_other_worker'])

    def test_not_run(self):
        class AlreadyRunningTask(luigi.Task):
            def run(self):
                pass

        other_worker = luigi.worker.Worker(scheduler=self.scheduler, worker_id="other_worker")
        other_worker.add(AlreadyRunningTask())  # This also registers this worker
        old_func = luigi.scheduler.Scheduler.get_work

        def new_func(*args, **kwargs):
            kwargs['current_tasks'] = None
            old_func(*args, **kwargs)
            return old_func(*args, **kwargs)

        with mock.patch('luigi.scheduler.Scheduler.get_work', new_func):
            self.run_task(AlreadyRunningTask())

        d = self.summary_dict()
        self.assertFalse(d['already_done'])
        self.assertFalse(d['completed'])
        self.assertFalse(d['run_by_other_worker'])
        self.assertEqual({AlreadyRunningTask()}, d['not_run'])

        s = self.summary()
        self.assertIn('\nScheduled 1 tasks of which:\n'
                      '* 1 were left pending, among these:\n'
                      '    * 1 was not granted run permission by the scheduler:\n'
                      '        - 1 AlreadyRunningTask()\n', s)
        self.assertNotIn('\n\n\n', s)

    def test_somebody_else_finish_task(self):
        class SomeTask(RunOnceTask):
            pass

        other_worker = luigi.worker.Worker(scheduler=self.scheduler, worker_id="other_worker")

        self.worker.add(SomeTask())
        other_worker.add(SomeTask())
        other_worker.run()
        self.worker.run()

        d = self.summary_dict()
        self.assertFalse(d['already_done'])
        self.assertFalse(d['completed'])
        self.assertFalse(d['run_by_other_worker'])
        self.assertEqual({SomeTask()}, d['not_run'])

    def test_somebody_else_disables_task(self):
        class SomeTask(luigi.Task):
            def complete(self):
                return False

            def run(self):
                raise ValueError()

        other_worker = luigi.worker.Worker(scheduler=self.scheduler, worker_id="other_worker")

        self.worker.add(SomeTask())
        other_worker.add(SomeTask())
        other_worker.run()  # Assuming it is disabled for a while after this
        self.worker.run()

        d = self.summary_dict()
        self.assertFalse(d['already_done'])
        self.assertFalse(d['completed'])
        self.assertFalse(d['run_by_other_worker'])
        self.assertEqual({SomeTask()}, d['not_run'])

    def test_larger_tree(self):

        class Dog(RunOnceTask):
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

        class Bar(RunOnceTask):
            num = luigi.IntParameter()

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

        class Bar(RunOnceTask):
            date = luigi.DateParameter()

        class Foo(luigi.Task):
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

        class Bar(RunOnceTask):
            time = luigi.DateMinuteParameter()

        class Foo(luigi.Task):
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

        class Bar(RunOnceTask):
            num = luigi.IntParameter()

        class Foo(luigi.Task):
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

        class Bar(RunOnceTask):
            num1 = luigi.IntParameter()
            num2 = luigi.IntParameter()
            num3 = luigi.IntParameter()

        class Foo(luigi.Task):
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

        class Bar(RunOnceTask):
            num = luigi.IntParameter()
            num2 = luigi.IntParameter()

        class Foo(luigi.Task):
            def requires(self):
                for i in range(2):
                    yield Bar(i, 2 * i)

        self.run_task(Foo())
        d = self.summary_dict()
        self.assertEqual({Foo(), Bar(num=0, num2=0), Bar(num=1, num2=2)}, d['completed'])

        summary = self.summary()
        result = summary.split('\n')
        expected = ['',
                    '===== Luigi Execution Summary =====',
                    '',
                    'Scheduled 3 tasks of which:',
                    '* 3 ran successfully:',
                    '    - 2 Bar(num=0, num2=0) and Bar(num=1, num2=2)',
                    '    - 1 Foo()',
                    '',
                    'This progress looks :) because there were no failed tasks or missing external dependencies',
                    '',
                    '===== Luigi Execution Summary =====',
                    '']

        self.assertEqual(len(result), len(expected))
        for i, line in enumerate(result):
            self.assertEqual(line, expected[i])

    def test_really_long_param_name(self):

        class Bar(RunOnceTask):
            This_is_a_really_long_parameter_that_we_should_not_print_out_because_people_will_get_annoyed = luigi.IntParameter()

        class Foo(luigi.Task):
            def requires(self):
                yield Bar(0)

        self.run_task(Foo())
        s = self.summary()
        self.assertIn('Bar(...)', s)
        self.assertNotIn("Did not run any tasks", s)
        self.assertNotIn('\n\n\n', s)

    def test_multiple_params_multiple_same_task_family(self):

        class Bar(RunOnceTask):
            num = luigi.IntParameter()
            num2 = luigi.IntParameter()

        class Foo(luigi.Task):
            def requires(self):
                for i in range(4):
                    yield Bar(i, 2 * i)

        self.run_task(Foo())
        summary = self.summary()

        result = summary.split('\n')
        expected = ['',
                    '===== Luigi Execution Summary =====',
                    '',
                    'Scheduled 5 tasks of which:',
                    '* 5 ran successfully:',
                    '    - 4 Bar(num=0, num2=0) ...',
                    '    - 1 Foo()',
                    '',
                    'This progress looks :) because there were no failed tasks or missing external dependencies',
                    '',
                    '===== Luigi Execution Summary =====',
                    '']

        self.assertEqual(len(result), len(expected))
        for i, line in enumerate(result):
            self.assertEqual(line, expected[i])

    def test_happy_smiley_face_normal(self):

        class Bar(RunOnceTask):
            num = luigi.IntParameter()
            num2 = luigi.IntParameter()

        class Foo(luigi.Task):
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

        class ParentTask(RunOnceTask):

            def requires(self):
                yield LockTask()

        class LockTask(RunOnceTask):

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

            def requires(self):
                for i in range(5, 200):
                    yield Bar(i)

        class Foo(luigi.Task):
            num = luigi.IntParameter()
            num2 = luigi.IntParameter()

            def requires(self):
                yield MyExternal()
                yield Boom(0)

        class Bar(luigi.Task):
            num = luigi.IntParameter()

            def complete(self):
                return True

        class DateTask(luigi.Task):
            date = luigi.DateParameter()
            num = luigi.IntParameter()

            def requires(self):
                yield MyExternal()
                yield Boom(0)

        class EntryPoint(luigi.Task):

            def requires(self):
                for i in range(10):
                    yield Foo(100, 2 * i)
                for i in range(10):
                    yield DateTask(datetime.date(1998, 3, 23) + datetime.timedelta(days=i), 5)

        self.run_task(EntryPoint())
        summary = self.summary()

        expected = ['',
                    '===== Luigi Execution Summary =====',
                    '',
                    'Scheduled 218 tasks of which:',
                    '* 195 present dependencies were encountered:',
                    '    - 195 Bar(num=5...199)',
                    '* 1 ran successfully:',
                    '    - 1 Boom(...)',
                    '* 22 were left pending, among these:',
                    '    * 1 were missing external dependencies:',
                    '        - 1 MyExternal()',
                    '    * 21 had missing external dependencies:',
                    '        - 10 DateTask(date=1998-03-23...1998-04-01, num=5)',
                    '        - 1 EntryPoint()',
                    '        - 10 Foo(num=100, num2=0) ...',
                    '',
                    'This progress looks :| because there were missing external dependencies',
                    '',
                    '===== Luigi Execution Summary =====',
                    '']
        result = summary.split('\n')

        self.assertEqual(len(result), len(expected))
        for i, line in enumerate(result):
            self.assertEqual(line, expected[i])

    def test_with_datehours(self):
        """ Just test that it doesn't crash with datehour params """

        start = datetime.datetime(1998, 3, 23, 5)

        class Bar(RunOnceTask):
            datehour = luigi.DateHourParameter()

        class Foo(luigi.Task):
            def requires(self):
                for i in range(10):
                    new_date = start + datetime.timedelta(hours=i)
                    yield Bar(datehour=new_date)

        self.run_task(Foo())
        d = self.summary_dict()
        exp_set = {Bar(start + datetime.timedelta(hours=i)) for i in range(10)}
        exp_set.add(Foo())
        self.assertEqual(exp_set, d['completed'])
        s = self.summary()
        self.assertIn('datehour=1998-03-23T0', s)
        self.assertIn('Scheduled 11 tasks', s)
        self.assertIn('Luigi Execution Summary', s)
        self.assertNotIn('00:00:00', s)
        self.assertNotIn('\n\n\n', s)

    def test_with_months(self):
        """ Just test that it doesn't crash with month params """

        start = datetime.datetime(1998, 3, 23)

        class Bar(RunOnceTask):
            month = luigi.MonthParameter()

        class Foo(luigi.Task):
            def requires(self):
                for i in range(3):
                    new_date = start + datetime.timedelta(days=30*i)
                    yield Bar(month=new_date)

        self.run_task(Foo())
        d = self.summary_dict()
        exp_set = {Bar(start + datetime.timedelta(days=30*i)) for i in range(3)}
        exp_set.add(Foo())
        self.assertEqual(exp_set, d['completed'])
        s = self.summary()
        self.assertIn('month=1998-0', s)
        self.assertIn('Scheduled 4 tasks', s)
        self.assertIn('Luigi Execution Summary', s)
        self.assertNotIn('00:00:00', s)
        self.assertNotIn('\n\n\n', s)

    def test_multiple_dash_dash_workers(self):
        """
        Don't print own worker with ``--workers 2`` setting.
        """
        self.worker = luigi.worker.Worker(scheduler=self.scheduler, worker_processes=2)

        class Foo(RunOnceTask):
            pass

        self.run_task(Foo())
        d = self.summary_dict()
        self.assertEqual(set(), d['run_by_other_worker'])
        s = self.summary()
        self.assertNotIn('The other workers were', s)
        self.assertIn('This progress looks :) because there were no failed ', s)
        self.assertNotIn('\n\n\n', s)

    def test_with_uncomparable_parameters(self):
        """
        Don't rely on parameters being sortable
        """
        class Color(Enum):
            red = 1
            yellow = 2

        class Bar(RunOnceTask):
            eparam = luigi.EnumParameter(enum=Color)

        class Baz(RunOnceTask):
            eparam = luigi.EnumParameter(enum=Color)
            another_param = luigi.IntParameter()

        class Foo(luigi.Task):
            def requires(self):
                yield Bar(Color.red)
                yield Bar(Color.yellow)
                yield Baz(Color.red, 5)
                yield Baz(Color.yellow, 5)

        self.run_task(Foo())
        s = self.summary()
        self.assertIn('yellow', s)

    def test_with_dict_dependency(self):
        """ Just test that it doesn't crash with dict params in dependencies """

        args = dict(start=datetime.date(1998, 3, 23), num=3)

        class Bar(RunOnceTask):
            args = luigi.DictParameter()

        class Foo(luigi.Task):
            def requires(self):
                for i in range(10):
                    new_dict = args.copy()
                    new_dict['start'] = str(new_dict['start'] + datetime.timedelta(days=i))
                    yield Bar(args=new_dict)

        self.run_task(Foo())
        d = self.summary_dict()
        exp_set = set()
        for i in range(10):
            new_dict = args.copy()
            new_dict['start'] = str(new_dict['start'] + datetime.timedelta(days=i))
            exp_set.add(Bar(new_dict))
        exp_set.add(Foo())
        self.assertEqual(exp_set, d['completed'])
        s = self.summary()
        self.assertIn('"num": 3', s)
        self.assertIn('"start": "1998-0', s)
        self.assertIn('Scheduled 11 tasks', s)
        self.assertIn('Luigi Execution Summary', s)
        self.assertNotIn('00:00:00', s)
        self.assertNotIn('\n\n\n', s)

    def test_with_dict_argument(self):
        """ Just test that it doesn't crash with dict params """

        args = dict(start=str(datetime.date(1998, 3, 23)), num=3)

        class Bar(RunOnceTask):
            args = luigi.DictParameter()

        self.run_task(Bar(args=args))
        d = self.summary_dict()
        exp_set = set()
        exp_set.add(Bar(args=args))
        self.assertEqual(exp_set, d['completed'])
        s = self.summary()
        self.assertIn('"num": 3', s)
        self.assertIn('"start": "1998-0', s)
        self.assertIn('Scheduled 1 task', s)
        self.assertIn('Luigi Execution Summary', s)
        self.assertNotIn('00:00:00', s)
        self.assertNotIn('\n\n\n', s)

    """
    Test that a task once crashing and then succeeding should be counted as no failure.
    """
    def test_status_with_task_retry(self):
        class Foo(luigi.Task):
            run_count = 0

            def run(self):
                self.run_count += 1
                if self.run_count == 1:
                    raise ValueError()

            def complete(self):
                return self.run_count > 0

        self.run_task(Foo())
        self.run_task(Foo())
        d = self.summary_dict()
        self.assertEqual({Foo()}, d['completed'])
        self.assertEqual({Foo()}, d['ever_failed'])
        self.assertFalse(d['failed'])
        self.assertFalse(d['upstream_failure'])
        self.assertFalse(d['upstream_missing_dependency'])
        self.assertFalse(d['run_by_other_worker'])
        self.assertFalse(d['still_pending_ext'])
        s = self.summary()
        self.assertIn('Scheduled 1 task', s)
        self.assertIn('Luigi Execution Summary', s)
        self.assertNotIn('ever failed', s)
        self.assertIn('\n\nThis progress looks :) because there were failed tasks but they all suceeded in a retry', s)
