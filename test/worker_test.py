# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
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
from __future__ import print_function

import functools
import logging
import os
import shutil
import signal
import tempfile
import threading
import time
from helpers import unittest

import luigi.notifications
import luigi.worker
import mock
from helpers import with_config
from luigi import ExternalTask, RemoteScheduler, Task
from luigi.mock import MockTarget, MockFileSystem
from luigi.scheduler import CentralPlannerScheduler
from luigi.worker import Worker
from luigi import six

luigi.notifications.DEBUG = True


class DummyTask(Task):

    def __init__(self, *args, **kwargs):
        super(DummyTask, self).__init__(*args, **kwargs)
        self.has_run = False

    def complete(self):
        return self.has_run

    def run(self):
        logging.debug("%s - setting has_run", self.task_id)
        self.has_run = True


class DynamicDummyTask(Task):
    p = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.p)

    def run(self):
        with self.output().open('w') as f:
            f.write('Done!')
        time.sleep(0.5)  # so we can benchmark & see if parallelization works


class DynamicDummyTaskWithNamespace(DynamicDummyTask):
    task_namespace = 'banana'


class DynamicRequires(Task):
    p = luigi.Parameter()
    use_banana_task = luigi.BoolParameter(default=False)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.p, 'parent'))

    def run(self):
        if self.use_banana_task:
            task_cls = DynamicDummyTaskWithNamespace
        else:
            task_cls = DynamicDummyTask
        dummy_targets = yield [task_cls(os.path.join(self.p, str(i)))
                               for i in range(5)]
        dummy_targets += yield [task_cls(os.path.join(self.p, str(i)))
                                for i in range(5, 7)]
        with self.output().open('w') as f:
            for i, d in enumerate(dummy_targets):
                for line in d.open('r'):
                    print('%d: %s' % (i, line.strip()), file=f)


class DynamicRequiresOtherModule(Task):
    p = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(os.path.join(self.p, 'baz'))

    def run(self):
        import other_module
        other_target_foo = yield other_module.OtherModuleTask(os.path.join(self.p, 'foo'))
        other_target_bar = yield other_module.OtherModuleTask(os.path.join(self.p, 'bar'))

        with self.output().open('w') as f:
            f.write('Done!')


class WorkerTest(unittest.TestCase):

    def setUp(self):
        # InstanceCache.disable()
        self.sch = CentralPlannerScheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)
        self.w = Worker(scheduler=self.sch, worker_id='X')
        self.w2 = Worker(scheduler=self.sch, worker_id='Y')
        self.time = time.time

    def tearDown(self):
        if time.time != self.time:
            time.time = self.time
        self.w.stop()
        self.w2.stop()

    def setTime(self, t):
        time.time = lambda: t

    def test_dep(self):
        class A(Task):

            def run(self):
                self.has_run = True

            def complete(self):
                return self.has_run
        a = A()

        class B(Task):

            def requires(self):
                return a

            def run(self):
                self.has_run = True

            def complete(self):
                return self.has_run

        b = B()
        a.has_run = False
        b.has_run = False

        self.assertTrue(self.w.add(b))
        self.assertTrue(self.w.run())
        self.assertTrue(a.has_run)
        self.assertTrue(b.has_run)

    def test_external_dep(self):
        class A(ExternalTask):

            def complete(self):
                return False
        a = A()

        class B(Task):

            def requires(self):
                return a

            def run(self):
                self.has_run = True

            def complete(self):
                return self.has_run

        b = B()

        a.has_run = False
        b.has_run = False

        self.assertTrue(self.w.add(b))
        self.assertTrue(self.w.run())

        self.assertFalse(a.has_run)
        self.assertFalse(b.has_run)

    def test_fail(self):
        class A(Task):

            def run(self):
                self.has_run = True
                raise Exception()

            def complete(self):
                return self.has_run

        a = A()

        class B(Task):

            def requires(self):
                return a

            def run(self):
                self.has_run = True

            def complete(self):
                return self.has_run

        b = B()

        a.has_run = False
        b.has_run = False

        self.assertTrue(self.w.add(b))
        self.assertFalse(self.w.run())

        self.assertTrue(a.has_run)
        self.assertFalse(b.has_run)

    def test_unknown_dep(self):
        # see central_planner_test.CentralPlannerTest.test_remove_dep
        class A(ExternalTask):

            def complete(self):
                return False

        class C(Task):

            def complete(self):
                return True

        def get_b(dep):
            class B(Task):

                def requires(self):
                    return dep

                def run(self):
                    self.has_run = True

                def complete(self):
                    return False

            b = B()
            b.has_run = False
            return b

        b_a = get_b(A())
        b_c = get_b(C())

        self.assertTrue(self.w.add(b_a))
        # So now another worker goes in and schedules C -> B
        # This should remove the dep A -> B but will screw up the first worker
        self.assertTrue(self.w2.add(b_c))

        self.assertFalse(self.w.run())  # should not run anything - the worker should detect that A is broken
        self.assertFalse(b_a.has_run)
        # not sure what should happen??
        # self.w2.run() # should run B since C is fulfilled
        # self.assertTrue(b_c.has_run)

    def test_unfulfilled_dep(self):
        class A(Task):

            def complete(self):
                return self.done

            def run(self):
                self.done = True

        def get_b(a):
            class B(A):

                def requires(self):
                    return a
            b = B()
            b.done = False
            a.done = True
            return b

        a = A()
        b = get_b(a)

        self.assertTrue(self.w.add(b))
        a.done = False
        self.w.run()
        self.assertTrue(a.complete())
        self.assertTrue(b.complete())

    def test_avoid_infinite_reschedule(self):
        class A(Task):

            def complete(self):
                return False

        class B(Task):

            def complete(self):
                return False

            def requires(self):
                return A()

        self.assertTrue(self.w.add(B()))
        self.assertFalse(self.w.run())

    def test_allow_reschedule_with_many_missing_deps(self):
        class A(Task):

            """ Task that must run twice to succeed """
            i = luigi.IntParameter()

            runs = 0

            def complete(self):
                return self.runs >= 2

            def run(self):
                self.runs += 1

        class B(Task):
            done = False

            def requires(self):
                return map(A, range(20))

            def complete(self):
                return self.done

            def run(self):
                self.done = True

        b = B()
        w = Worker(scheduler=self.sch, worker_id='X', max_reschedules=1)
        self.assertTrue(w.add(b))
        self.assertFalse(w.run())

        # For b to be done, we must have rescheduled its dependencies to run them twice
        self.assertTrue(b.complete())
        self.assertTrue(all(a.complete() for a in b.deps()))

    def test_interleaved_workers(self):
        class A(DummyTask):
            pass

        a = A()

        class B(DummyTask):

            def requires(self):
                return a

        class ExternalB(ExternalTask):
            task_family = "B"

            def complete(self):
                return False

        b = B()
        eb = ExternalB()
        self.assertEqual(eb.task_id, "B()")

        sch = CentralPlannerScheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)
        w = Worker(scheduler=sch, worker_id='X')
        w2 = Worker(scheduler=sch, worker_id='Y')

        self.assertTrue(w.add(b))
        self.assertTrue(w2.add(eb))
        logging.debug("RUNNING BROKEN WORKER")
        self.assertTrue(w2.run())
        self.assertFalse(a.complete())
        self.assertFalse(b.complete())
        logging.debug("RUNNING FUNCTIONAL WORKER")
        self.assertTrue(w.run())
        self.assertTrue(a.complete())
        self.assertTrue(b.complete())
        w.stop()
        w2.stop()

    def test_interleaved_workers2(self):
        # two tasks without dependencies, one external, one not
        class B(DummyTask):
            pass

        class ExternalB(ExternalTask):
            task_family = "B"

            def complete(self):
                return False

        b = B()
        eb = ExternalB()

        self.assertEqual(eb.task_id, "B()")

        sch = CentralPlannerScheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)
        w = Worker(scheduler=sch, worker_id='X')
        w2 = Worker(scheduler=sch, worker_id='Y')

        self.assertTrue(w2.add(eb))
        self.assertTrue(w.add(b))

        self.assertTrue(w2.run())
        self.assertFalse(b.complete())
        self.assertTrue(w.run())
        self.assertTrue(b.complete())
        w.stop()
        w2.stop()

    def test_interleaved_workers3(self):
        class A(DummyTask):

            def run(self):
                logging.debug('running A')
                time.sleep(0.1)
                super(A, self).run()

        a = A()

        class B(DummyTask):

            def requires(self):
                return a

            def run(self):
                logging.debug('running B')
                super(B, self).run()

        b = B()

        sch = CentralPlannerScheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)

        w = Worker(scheduler=sch, worker_id='X', keep_alive=True, count_uniques=True)
        w2 = Worker(scheduler=sch, worker_id='Y', keep_alive=True, count_uniques=True, wait_interval=0.1)

        self.assertTrue(w.add(a))
        self.assertTrue(w2.add(b))

        threading.Thread(target=w.run).start()
        self.assertTrue(w2.run())

        self.assertTrue(a.complete())
        self.assertTrue(b.complete())

        w.stop()
        w2.stop()

    def test_die_for_non_unique_pending(self):
        class A(DummyTask):

            def run(self):
                logging.debug('running A')
                time.sleep(0.1)
                super(A, self).run()

        a = A()

        class B(DummyTask):

            def requires(self):
                return a

            def run(self):
                logging.debug('running B')
                super(B, self).run()

        b = B()

        sch = CentralPlannerScheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)

        w = Worker(scheduler=sch, worker_id='X', keep_alive=True, count_uniques=True)
        w2 = Worker(scheduler=sch, worker_id='Y', keep_alive=True, count_uniques=True, wait_interval=0.1)

        self.assertTrue(w.add(b))
        self.assertTrue(w2.add(b))

        self.assertEqual(w._get_work()[0], 'A()')
        self.assertTrue(w2.run())

        self.assertFalse(a.complete())
        self.assertFalse(b.complete())

        w2.stop()

    def test_complete_exception(self):
        "Tests that a task is still scheduled if its sister task crashes in the complete() method"
        class A(DummyTask):

            def complete(self):
                raise Exception("doh")

        a = A()

        class C(DummyTask):
            pass

        c = C()

        class B(DummyTask):

            def requires(self):
                return a, c

        b = B()
        sch = CentralPlannerScheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)
        w = Worker(scheduler=sch, worker_id="foo")
        self.assertFalse(w.add(b))
        self.assertTrue(w.run())
        self.assertFalse(b.has_run)
        self.assertTrue(c.has_run)
        self.assertFalse(a.has_run)
        w.stop()

    def test_requires_exception(self):
        class A(DummyTask):

            def requires(self):
                raise Exception("doh")

        a = A()

        class C(DummyTask):
            pass

        c = C()

        class B(DummyTask):

            def requires(self):
                return a, c

        b = B()
        sch = CentralPlannerScheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)
        w = Worker(scheduler=sch, worker_id="foo")
        self.assertFalse(w.add(b))
        self.assertTrue(w.run())
        self.assertFalse(b.has_run)
        self.assertTrue(c.has_run)
        self.assertFalse(a.has_run)
        w.stop()


class DynamicDependenciesTest(unittest.TestCase):
    n_workers = 1
    timeout = float('inf')

    def setUp(self):
        self.p = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.p)

    def test_dynamic_dependencies(self, use_banana_task=False):
        t0 = time.time()
        t = DynamicRequires(p=self.p, use_banana_task=use_banana_task)
        luigi.build([t], local_scheduler=True, workers=self.n_workers)
        self.assertTrue(t.complete())

        # loop through output and verify
        f = t.output().open('r')
        for i in range(7):
            self.assertEqual(f.readline().strip(), '%d: Done!' % i)

        self.assertTrue(time.time() - t0 < self.timeout)

    def test_dynamic_dependencies_with_namespace(self):
        self.test_dynamic_dependencies(use_banana_task=True)

    def test_dynamic_dependencies_other_module(self):
        t = DynamicRequiresOtherModule(p=self.p)
        luigi.build([t], local_scheduler=True, workers=self.n_workers)
        self.assertTrue(t.complete())


class DynamicDependenciesWithMultipleWorkersTest(DynamicDependenciesTest):
    n_workers = 100
    timeout = 3.0  # We run 7 tasks that take 0.5s each so it should take less than 3.5s


class WorkerPingThreadTests(unittest.TestCase):

    def test_ping_retry(self):
        """ Worker ping fails once. Ping continues to try to connect to scheduler

        Kind of ugly since it uses actual timing with sleep to test the thread
        """
        sch = CentralPlannerScheduler(
            retry_delay=100,
            remove_delay=1000,
            worker_disconnect_delay=10,
        )

        self._total_pings = 0  # class var so it can be accessed from fail_ping

        def fail_ping(worker):
            # this will be called from within keep-alive thread...
            self._total_pings += 1
            raise Exception("Some random exception")

        sch.ping = fail_ping

        w = Worker(
            scheduler=sch,
            worker_id="foo",
            ping_interval=0.01  # very short between pings to make test fast
        )

        # let the keep-alive thread run for a bit...
        time.sleep(0.1)  # yes, this is ugly but it's exactly what we need to test
        w.stop()
        self.assertTrue(
            self._total_pings > 1,
            msg="Didn't retry pings (%d pings performed)" % (self._total_pings,)
        )

    def test_ping_thread_shutdown(self):
        w = Worker(ping_interval=0.01)
        self.assertTrue(w._keep_alive_thread.is_alive())
        w.stop()  # should stop within 0.01 s
        self.assertFalse(w._keep_alive_thread.is_alive())


def email_patch(test_func):
    EMAIL_CONFIG = {"core": {"error-email": "not-a-real-email-address-for-test-only"}, "email": {"force-send": "true"}}
    emails = []

    def mock_send_email(sender, recipients, msg):
        emails.append(msg)

    @with_config(EMAIL_CONFIG)
    @functools.wraps(test_func)
    @mock.patch('smtplib.SMTP')
    def run_test(self, smtp):
        smtp().sendmail.side_effect = mock_send_email
        test_func(self, emails)

    return run_test


class WorkerEmailTest(unittest.TestCase):

    def setUp(self):
        super(WorkerEmailTest, self).setUp()
        sch = CentralPlannerScheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)
        self.worker = Worker(scheduler=sch, worker_id="foo")

    def tearDown(self):
        self.worker.stop()

    @email_patch
    def test_connection_error(self, emails):
        sch = RemoteScheduler(host="this_host_doesnt_exist", port=1337, connect_timeout=1)
        worker = Worker(scheduler=sch)

        self.waits = 0

        def dummy_wait():
            self.waits += 1

        sch._wait = dummy_wait

        class A(DummyTask):
            pass

        a = A()
        self.assertEqual(emails, [])
        worker.add(a)
        self.assertEqual(self.waits, 2)  # should attempt to add it 3 times
        self.assertNotEquals(emails, [])
        self.assertTrue(emails[0].find("Luigi: Framework error while scheduling %s" % (a,)) != -1)
        worker.stop()

    @email_patch
    def test_complete_error(self, emails):
        class A(DummyTask):

            def complete(self):
                raise Exception("b0rk")

        a = A()
        self.assertEqual(emails, [])
        self.worker.add(a)
        self.assertTrue(emails[0].find("Luigi: %s failed scheduling" % (a,)) != -1)
        self.worker.run()
        self.assertTrue(emails[0].find("Luigi: %s failed scheduling" % (a,)) != -1)
        self.assertFalse(a.has_run)

    @email_patch
    def test_complete_return_value(self, emails):
        class A(DummyTask):

            def complete(self):
                pass  # no return value should be an error

        a = A()
        self.assertEqual(emails, [])
        self.worker.add(a)
        self.assertTrue(emails[0].find("Luigi: %s failed scheduling" % (a,)) != -1)
        self.worker.run()
        self.assertTrue(emails[0].find("Luigi: %s failed scheduling" % (a,)) != -1)
        self.assertFalse(a.has_run)

    @email_patch
    def test_run_error(self, emails):
        class A(luigi.Task):

            def complete(self):
                return False

            def run(self):
                raise Exception("b0rk")

        a = A()
        self.worker.add(a)
        self.assertEqual(emails, [])
        self.worker.run()
        self.assertTrue(emails[0].find("Luigi: %s FAILED" % (a,)) != -1)

    @email_patch
    def test_no_error(self, emails):
        class A(DummyTask):
            pass
        a = A()
        self.assertEqual(emails, [])
        self.worker.add(a)
        self.assertEqual(emails, [])
        self.worker.run()
        self.assertEqual(emails, [])
        self.assertTrue(a.complete())


class RaiseSystemExit(luigi.Task):

    def run(self):
        raise SystemExit("System exit!!")


class SuicidalWorker(luigi.Task):
    signal = luigi.IntParameter()

    def run(self):
        os.kill(os.getpid(), self.signal)


class HungWorker(luigi.Task):
    worker_timeout = luigi.IntParameter(default=None)

    def run(self):
        while True:
            pass

    def complete(self):
        return False


class MultipleWorkersTest(unittest.TestCase):

    # This pass under python3 when run as `nosetests test/worker_test.py`
    # but not as `nosetests test`. Probably some side effect on previous tests
    @unittest.skipIf(six.PY3, 'This test fail on python3 when run with tox.')
    def test_multiple_workers(self):
        # Test using multiple workers
        # Also test generating classes dynamically since this may reflect issues with
        # various platform and how multiprocessing is implemented. If it's using os.fork
        # under the hood it should be fine, but dynamic classses can't be pickled, so
        # other implementations of multiprocessing (using spawn etc) may fail
        class MyDynamicTask(luigi.Task):
            x = luigi.Parameter()

            def run(self):
                time.sleep(0.1)

        t0 = time.time()
        luigi.build([MyDynamicTask(i) for i in range(100)], workers=100, local_scheduler=True)
        self.assertTrue(time.time() < t0 + 5.0)  # should ideally take exactly 0.1s, but definitely less than 10.0

    def test_zero_workers(self):
        d = DummyTask()
        luigi.build([d], workers=0, local_scheduler=True)
        self.assertFalse(d.complete())

    def test_system_exit(self):
        # This would hang indefinitely before this fix:
        # https://github.com/spotify/luigi/pull/439
        luigi.build([RaiseSystemExit()], workers=2, local_scheduler=True)

    def test_term_worker(self):
        luigi.build([SuicidalWorker(signal.SIGTERM)], workers=2, local_scheduler=True)

    def test_kill_worker(self):
        luigi.build([SuicidalWorker(signal.SIGKILL)], workers=2, local_scheduler=True)

    def test_purge_multiple_workers(self):
        w = Worker(worker_processes=2, wait_interval=0.01)
        t1 = SuicidalWorker(signal.SIGTERM)
        t2 = SuicidalWorker(signal.SIGKILL)
        w.add(t1)
        w.add(t2)

        w._run_task(t1.task_id)
        w._run_task(t2.task_id)
        time.sleep(1.0)

        w._handle_next_task()
        w._handle_next_task()
        w._handle_next_task()

    def test_time_out_hung_worker(self):
        luigi.build([HungWorker(0.1)], workers=2, local_scheduler=True)

    @mock.patch('luigi.worker.time')
    def test_purge_hung_worker_default_timeout_time(self, mock_time):
        w = Worker(worker_processes=2, wait_interval=0.01, timeout=5)
        mock_time.time.return_value = 0
        w.add(HungWorker())
        w._run_task('HungWorker(worker_timeout=None)')

        mock_time.time.return_value = 5
        w._handle_next_task()
        self.assertEqual(1, len(w._running_tasks))

        mock_time.time.return_value = 6
        w._handle_next_task()
        self.assertEqual(0, len(w._running_tasks))

    @mock.patch('luigi.worker.time')
    def test_purge_hung_worker_override_timeout_time(self, mock_time):
        w = Worker(worker_processes=2, wait_interval=0.01, timeout=5)
        mock_time.time.return_value = 0
        w.add(HungWorker(10))
        w._run_task('HungWorker(worker_timeout=10)')

        mock_time.time.return_value = 10
        w._handle_next_task()
        self.assertEqual(1, len(w._running_tasks))

        mock_time.time.return_value = 11
        w._handle_next_task()
        self.assertEqual(0, len(w._running_tasks))


class Dummy2Task(Task):
    p = luigi.Parameter()

    def output(self):
        return MockTarget(self.p)

    def run(self):
        f = self.output().open('w')
        f.write('test')
        f.close()


class AssistantTest(unittest.TestCase):
    def setUp(self):
        self.sch = CentralPlannerScheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)
        self.w = Worker(scheduler=self.sch, worker_id='X')
        self.assistant = Worker(scheduler=self.sch, worker_id='Y', assistant=True)

    def test_get_work(self):
        d = Dummy2Task('123')
        self.w.add(d)

        self.assertFalse(d.complete())
        self.assistant.run()
        self.assertTrue(d.complete())

    def test_bad_job_type(self):
        class Dummy3Task(Dummy2Task):
            task_family = 'UnknownTaskFamily'

        d = Dummy3Task('123')
        self.w.add(d)

        self.assertFalse(d.complete())
        self.assertFalse(self.assistant.run())
        self.assertFalse(d.complete())
        self.assertEqual(list(self.sch.task_list('FAILED', '').keys()), [str(d)])

    def test_unimported_job_type(self):
        class NotImportedTask(luigi.Task):
            task_family = 'UnimportedTask'
            task_module = None

        task = NotImportedTask()

        # verify that it can't run the task without the module info necessary to import it
        self.w.add(task)
        self.assertFalse(self.assistant.run())
        self.assertEqual(list(self.sch.task_list('FAILED', '').keys()), ['UnimportedTask()'])

        # check that it can import with the right module
        task.task_module = 'dummy_test_module.not_imported'
        self.w.add(task)
        self.assertTrue(self.assistant.run())
        self.assertEqual(list(self.sch.task_list('DONE', '').keys()), ['UnimportedTask()'])


class ForkBombTask(luigi.Task):
    depth = luigi.IntParameter()
    breadth = luigi.IntParameter()
    p = luigi.Parameter(default=(0, ))  # ehm for some weird reason [0] becomes a tuple...?

    def output(self):
        return MockTarget('.'.join(map(str, self.p)))

    def run(self):
        with self.output().open('w') as f:
            f.write('Done!')

    def requires(self):
        if len(self.p) < self.depth:
            for i in range(self.breadth):
                yield ForkBombTask(self.depth, self.breadth, self.p + (i, ))


class TaskLimitTest(unittest.TestCase):
    def tearDown(self):
        MockFileSystem().remove('')

    @with_config({'core': {'worker-task-limit': '6'}})
    def test_task_limit_exceeded(self):
        w = Worker()
        t = ForkBombTask(3, 2)
        w.add(t)
        w.run()
        self.assertFalse(t.complete())
        leaf_tasks = [ForkBombTask(3, 2, branch) for branch in [(0, 0, 0), (0, 0, 1), (0, 1, 0), (0, 1, 1)]]
        self.assertEquals(3, sum(t.complete() for t in leaf_tasks), "should have gracefully completed as much as possible even though the single last leaf didn't get scheduled")

    @with_config({'core': {'worker-task-limit': '7'}})
    def test_task_limit_not_exceeded(self):
        w = Worker()
        t = ForkBombTask(3, 2)
        w.add(t)
        w.run()
        self.assertTrue(t.complete())

    def test_no_task_limit(self):
        w = Worker()
        t = ForkBombTask(4, 2)
        w.add(t)
        w.run()
        self.assertTrue(t.complete())


if __name__ == '__main__':
    luigi.run()
