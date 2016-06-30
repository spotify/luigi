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
import psutil
from helpers import unittest, with_config, skipOnTravis, LuigiTestCase

import luigi.notifications
import luigi.worker
import mock
from luigi import ExternalTask, RemoteScheduler, Task, Event
from luigi.mock import MockTarget, MockFileSystem
from luigi.scheduler import CentralPlannerScheduler
from luigi.worker import Worker
from luigi.rpc import RPCError
from luigi import six
from luigi.cmdline import luigi_run

luigi.notifications.DEBUG = True


class DummyTask(Task):

    def __init__(self, *args, **kwargs):
        super(DummyTask, self).__init__(*args, **kwargs)
        self.has_run = False

    def complete(self):
        return self.has_run

    def run(self):
        logging.debug("%s - setting has_run", self)
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
        other_target_foo = yield other_module.OtherModuleTask(os.path.join(self.p, 'foo'))  # NOQA
        other_target_bar = yield other_module.OtherModuleTask(os.path.join(self.p, 'bar'))  # NOQA

        with self.output().open('w') as f:
            f.write('Done!')


class WorkerTest(unittest.TestCase):

    def run(self, result=None):
        self.sch = CentralPlannerScheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)
        self.time = time.time
        with Worker(scheduler=self.sch, worker_id='X') as w, Worker(scheduler=self.sch, worker_id='Y') as w2:
            self.w = w
            self.w2 = w2
            super(WorkerTest, self).run(result)

        if time.time != self.time:
            time.time = self.time

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

    def test_stop_getting_new_work(self):
        d = DummyTask()
        self.w.add(d)

        self.assertFalse(d.complete())
        try:
            self.w.handle_interrupt(signal.SIGUSR1, None)
        except AttributeError:
            raise unittest.SkipTest('signal.SIGUSR1 not found on this system')
        self.w.run()
        self.assertFalse(d.complete())

    def test_disabled_shutdown_hook(self):
        w = Worker(scheduler=self.sch, keep_alive=True, no_install_shutdown_handler=True)
        with w:
            try:
                # try to kill the worker!
                os.kill(os.getpid(), signal.SIGUSR1)
            except AttributeError:
                raise unittest.SkipTest('signal.SIGUSR1 not found on this system')
            # try to kill the worker... AGAIN!
            t = SuicidalWorker(signal.SIGUSR1)
            w.add(t)
            w.run()
            # task should have stepped away from the ledge, and completed successfully despite all the SIGUSR1 signals
            self.assertEqual(list(self.sch.task_list('DONE', '').keys()), [t.task_id])

    @with_config({"worker": {"no_install_shutdown_handler": "True"}})
    def test_can_run_luigi_in_thread(self):
        class A(DummyTask):
            pass
        task = A()
        # Note that ``signal.signal(signal.SIGUSR1, fn)`` can only be called in the main thread.
        # So if we do not disable the shutdown handler, this would fail.
        t = threading.Thread(target=lambda: luigi.build([task], local_scheduler=True))
        t.start()
        t.join()
        self.assertTrue(task.complete())

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

    def test_externalized_dep(self):
        class A(Task):
            has_run = False

            def run(self):
                self.has_run = True

            def complete(self):
                return self.has_run
        a = A()

        class B(A):
            def requires(self):
                return luigi.task.externalize(a)
        b = B()

        self.assertTrue(self.w.add(b))
        self.assertTrue(self.w.run())

        self.assertFalse(a.has_run)
        self.assertFalse(b.has_run)

    def test_legacy_externalized_dep(self):
        class A(Task):
            has_run = False

            def run(self):
                self.has_run = True

            def complete(self):
                return self.has_run
        a = A()
        a.run = NotImplemented

        class B(A):
            def requires(self):
                return a
        b = B()

        self.assertTrue(self.w.add(b))
        self.assertTrue(self.w.run())

        self.assertFalse(a.has_run)
        self.assertFalse(b.has_run)

    def test_type_error_in_tracking_run_deprecated(self):
        class A(Task):
            num_runs = 0

            def complete(self):
                return False

            def run(self, tracking_url_callback=None):
                self.num_runs += 1
                raise TypeError('bad type')

        a = A()
        self.assertTrue(self.w.add(a))
        self.assertFalse(self.w.run())

        # Should only run and fail once, not retry because of the type error
        self.assertEqual(1, a.num_runs)

    def test_tracking_url(self):
        tracking_url = 'http://test_url.com/'

        class A(Task):
            has_run = False

            def complete(self):
                return self.has_run

            def run(self):
                self.set_tracking_url(tracking_url)
                self.has_run = True

        a = A()
        self.assertTrue(self.w.add(a))
        self.assertTrue(self.w.run())
        tasks = self.sch.task_list('DONE', '')
        self.assertEqual(1, len(tasks))
        self.assertEqual(tracking_url, tasks[a.task_id]['tracking_url'])

    def test_fail(self):
        class CustomException(BaseException):
            def __init__(self, msg):
                self.msg = msg

        class A(Task):

            def run(self):
                self.has_run = True
                raise CustomException('bad things')

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

    def test_gets_missed_work(self):
        class A(Task):
            done = False

            def complete(self):
                return self.done

            def run(self):
                self.done = True

        a = A()
        self.assertTrue(self.w.add(a))

        # simulate a missed get_work response
        self.assertEqual(a.task_id, self.sch.get_work(worker='X')['task_id'])

        self.assertTrue(self.w.run())
        self.assertTrue(a.complete())

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

    def test_fails_registering_signal(self):
        with mock.patch('luigi.worker.signal', spec=['signal']):
            # mock will raise an attribute error getting signal.SIGUSR1
            Worker()

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
        self.assertEqual(str(eb), "B()")

        sch = CentralPlannerScheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)
        with Worker(scheduler=sch, worker_id='X') as w, Worker(scheduler=sch, worker_id='Y') as w2:
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

        self.assertEqual(str(eb), "B()")

        sch = CentralPlannerScheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)
        with Worker(scheduler=sch, worker_id='X') as w, Worker(scheduler=sch, worker_id='Y') as w2:
            self.assertTrue(w2.add(eb))
            self.assertTrue(w.add(b))

            self.assertTrue(w2.run())
            self.assertFalse(b.complete())
            self.assertTrue(w.run())
            self.assertTrue(b.complete())

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

        with Worker(scheduler=sch, worker_id='X', keep_alive=True, count_uniques=True) as w:
            with Worker(scheduler=sch, worker_id='Y', keep_alive=True, count_uniques=True, wait_interval=0.1) as w2:
                self.assertTrue(w.add(a))
                self.assertTrue(w2.add(b))

                threading.Thread(target=w.run).start()
                self.assertTrue(w2.run())

                self.assertTrue(a.complete())
                self.assertTrue(b.complete())

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

        with Worker(scheduler=sch, worker_id='X', keep_alive=True, count_uniques=True) as w:
            with Worker(scheduler=sch, worker_id='Y', keep_alive=True, count_uniques=True, wait_interval=0.1) as w2:
                self.assertTrue(w.add(b))
                self.assertTrue(w2.add(b))

                self.assertEqual(w._get_work()[0], a.task_id)
                self.assertTrue(w2.run())

                self.assertFalse(a.complete())
                self.assertFalse(b.complete())

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
        with Worker(scheduler=sch, worker_id="foo") as w:
            self.assertFalse(w.add(b))
            self.assertTrue(w.run())
            self.assertFalse(b.has_run)
            self.assertTrue(c.has_run)
            self.assertFalse(a.has_run)

    def test_requires_exception(self):
        class A(DummyTask):

            def requires(self):
                raise Exception("doh")

        a = A()

        class D(DummyTask):
            pass

        d = D()

        class C(DummyTask):
            def requires(self):
                return d

        c = C()

        class B(DummyTask):

            def requires(self):
                return c, a

        b = B()
        sch = CentralPlannerScheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)
        with Worker(scheduler=sch, worker_id="foo") as w:
            self.assertFalse(w.add(b))
            self.assertTrue(w.run())
            self.assertFalse(b.has_run)
            self.assertTrue(c.has_run)
            self.assertTrue(d.has_run)
            self.assertFalse(a.has_run)


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

        with Worker(
                scheduler=sch,
                worker_id="foo",
                ping_interval=0.01  # very short between pings to make test fast
                ):
            # let the keep-alive thread run for a bit...
            time.sleep(0.1)  # yes, this is ugly but it's exactly what we need to test
        self.assertTrue(
            self._total_pings > 1,
            msg="Didn't retry pings (%d pings performed)" % (self._total_pings,)
        )

    def test_ping_thread_shutdown(self):
        with Worker(ping_interval=0.01) as w:
            self.assertTrue(w._keep_alive_thread.is_alive())
        self.assertFalse(w._keep_alive_thread.is_alive())


def email_patch(test_func, email_config=None):
    EMAIL_CONFIG = {"core": {"error-email": "not-a-real-email-address-for-test-only"}, "email": {"force-send": "true"}}
    if email_config is not None:
        EMAIL_CONFIG.update(email_config)
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


def custom_email_patch(config):
    return functools.partial(email_patch, email_config=config)


class WorkerEmailTest(LuigiTestCase):

    def run(self, result=None):
        super(WorkerEmailTest, self).setUp()
        sch = CentralPlannerScheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)
        with Worker(scheduler=sch, worker_id="foo") as self.worker:
            super(WorkerEmailTest, self).run(result)

    @email_patch
    def test_connection_error(self, emails):
        sch = RemoteScheduler('http://tld.invalid:1337', connect_timeout=1)

        self.waits = 0

        def dummy_wait():
            self.waits += 1

        sch._wait = dummy_wait

        class A(DummyTask):
            pass

        a = A()
        self.assertEqual(emails, [])
        with Worker(scheduler=sch) as worker:
            try:
                worker.add(a)
            except RPCError:
                self.assertEqual(self.waits, 2)  # should attempt to add it 3 times
                self.assertNotEqual(emails, [])
                self.assertTrue(emails[0].find("Luigi: Framework error while scheduling %s" % (a,)) != -1)
            else:
                self.fail()

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
    def test_requires_error(self, emails):
        class A(DummyTask):

            def requires(self):
                raise Exception("b0rk")

        a = A()
        self.assertEqual(emails, [])
        self.worker.add(a)
        self.assertTrue(emails[0].find("Luigi: %s failed scheduling" % (a,)) != -1)
        self.worker.run()
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
            def run(self):
                raise Exception("b0rk")

        a = A()
        self.worker.add(a)
        self.assertEqual(emails, [])
        self.worker.run()
        self.assertTrue(emails[0].find("Luigi: %s FAILED" % (a,)) != -1)

    @email_patch
    def test_task_process_dies(self, emails):
        a = SuicidalWorker(signal.SIGKILL)
        luigi.build([a], workers=2, local_scheduler=True)
        self.assertTrue(emails[0].find("Luigi: %s FAILED" % (a,)) != -1)
        self.assertTrue(emails[0].find("died unexpectedly with exit code -9") != -1)

    @email_patch
    def test_task_times_out(self, emails):
        class A(luigi.Task):
            worker_timeout = 0.0001

            def run(self):
                time.sleep(5)

        a = A()
        luigi.build([a], workers=2, local_scheduler=True)
        self.assertTrue(emails[0].find("Luigi: %s FAILED" % (a,)) != -1)
        self.assertTrue(emails[0].find("timed out after 0.0001 seconds and was terminated.") != -1)

    @with_config(dict(worker=dict(retry_external_tasks='true')))
    @email_patch
    def test_external_task_retries(self, emails):
        """
        Test that we do not send error emails on the failures of external tasks
        """
        class A(luigi.ExternalTask):
            pass

        a = A()
        luigi.build([a], workers=2, local_scheduler=True)
        self.assertEqual(emails, [])

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

    @custom_email_patch({"core": {"error-email": "not-a-real-email-address-for-test-only", 'email-type': 'none'}})
    def test_disable_emails(self, emails):
        class A(luigi.Task):

            def complete(self):
                raise Exception("b0rk")

        self.worker.add(A())
        self.assertEqual(emails, [])


class RaiseSystemExit(luigi.Task):

    def run(self):
        raise SystemExit("System exit!!")


class SuicidalWorker(luigi.Task):
    signal = luigi.IntParameter()

    def run(self):
        os.kill(os.getpid(), self.signal)


class HangTheWorkerTask(luigi.Task):
    worker_timeout = luigi.IntParameter(default=None)

    def run(self):
        while True:
            pass

    def complete(self):
        return False


class MultipleWorkersTest(unittest.TestCase):

    @unittest.skip('Always skip. There are many intermittent failures')
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

    def test_stop_worker_kills_subprocesses(self):
        with Worker(worker_processes=2) as w:
            hung_task = HangTheWorkerTask()
            w.add(hung_task)

            w._run_task(hung_task.task_id)
            pids = [p.pid for p in w._running_tasks.values()]
            self.assertEqual(1, len(pids))
            pid = pids[0]

            def is_running():
                return pid in {p.pid for p in psutil.Process().children()}

            self.assertTrue(is_running())
        self.assertFalse(is_running())

    def test_time_out_hung_worker(self):
        luigi.build([HangTheWorkerTask(0.1)], workers=2, local_scheduler=True)

    @skipOnTravis('https://travis-ci.org/spotify/luigi/jobs/72953986')
    @mock.patch('luigi.worker.time')
    def test_purge_hung_worker_default_timeout_time(self, mock_time):
        w = Worker(worker_processes=2, wait_interval=0.01, timeout=5)
        mock_time.time.return_value = 0
        task = HangTheWorkerTask()
        w.add(task)
        w._run_task(task.task_id)

        mock_time.time.return_value = 5
        w._handle_next_task()
        self.assertEqual(1, len(w._running_tasks))

        mock_time.time.return_value = 6
        w._handle_next_task()
        self.assertEqual(0, len(w._running_tasks))

    @skipOnTravis('https://travis-ci.org/spotify/luigi/jobs/76645264')
    @mock.patch('luigi.worker.time')
    def test_purge_hung_worker_override_timeout_time(self, mock_time):
        w = Worker(worker_processes=2, wait_interval=0.01, timeout=5)
        mock_time.time.return_value = 0
        task = HangTheWorkerTask(worker_timeout=10)
        w.add(task)
        w._run_task(task.task_id)

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
    def run(self, result=None):
        self.sch = CentralPlannerScheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)
        self.assistant = Worker(scheduler=self.sch, worker_id='Y', assistant=True)
        with Worker(scheduler=self.sch, worker_id='X') as w:
            self.w = w
            super(AssistantTest, self).run(result)

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
        self.assertEqual(list(self.sch.task_list('FAILED', '').keys()), [d.task_id])

    def test_unimported_job_type(self):
        class NotImportedTask(luigi.Task):
            task_family = 'UnimportedTask'
            task_module = None

        task = NotImportedTask()

        # verify that it can't run the task without the module info necessary to import it
        self.w.add(task)
        self.assertFalse(self.assistant.run())
        self.assertEqual(list(self.sch.task_list('FAILED', '').keys()), [task.task_id])

        # check that it can import with the right module
        task.task_module = 'dummy_test_module.not_imported'
        self.w.add(task)
        self.assertTrue(self.assistant.run())
        self.assertEqual(list(self.sch.task_list('DONE', '').keys()), [task.task_id])


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
        self.assertEqual(3, sum(t.complete() for t in leaf_tasks),
                         "should have gracefully completed as much as possible even though the single last leaf didn't get scheduled")

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


class WorkerConfigurationTest(unittest.TestCase):

    def test_asserts_for_worker(self):
        """
        Test that Worker() asserts that it's sanely configured
        """
        Worker(wait_interval=1)  # This shouldn't raise
        self.assertRaises(AssertionError, Worker, wait_interval=0)


class WorkerWaitJitterTest(unittest.TestCase):
    @with_config({'worker': {'wait_jitter': '10.0'}})
    @mock.patch("random.uniform")
    @mock.patch("time.sleep")
    def test_wait_jitter(self, mock_sleep, mock_random):
        """ verify configured jitter amount """
        mock_random.return_value = 1.0

        w = Worker()
        x = w._sleeper()
        six.next(x)
        mock_random.assert_called_with(0, 10.0)
        mock_sleep.assert_called_with(2.0)

        mock_random.return_value = 2.0
        six.next(x)
        mock_random.assert_called_with(0, 10.0)
        mock_sleep.assert_called_with(3.0)

    @mock.patch("random.uniform")
    @mock.patch("time.sleep")
    def test_wait_jitter_default(self, mock_sleep, mock_random):
        """ verify default jitter is as expected """
        mock_random.return_value = 1.0
        w = Worker()
        x = w._sleeper()
        six.next(x)
        mock_random.assert_called_with(0, 5.0)
        mock_sleep.assert_called_with(2.0)

        mock_random.return_value = 3.3
        six.next(x)
        mock_random.assert_called_with(0, 5.0)
        mock_sleep.assert_called_with(4.3)


class KeyboardInterruptBehaviorTest(LuigiTestCase):

    def test_propagation_when_executing(self):
        """
        Ensure that keyboard interrupts causes luigi to quit when you are
        executing tasks.

        TODO: Add a test that tests the multiprocessing (--worker >1) case
        """
        class KeyboardInterruptTask(luigi.Task):
            def run(self):
                raise KeyboardInterrupt()

        cmd = 'KeyboardInterruptTask --local-scheduler --no-lock'.split(' ')
        self.assertRaises(KeyboardInterrupt, luigi_run, cmd)

    def test_propagation_when_scheduling(self):
        """
        Test that KeyboardInterrupt causes luigi to quit while scheduling.
        """
        class KeyboardInterruptTask(luigi.Task):
            def complete(self):
                raise KeyboardInterrupt()

        class ExternalKeyboardInterruptTask(luigi.ExternalTask):
            def complete(self):
                raise KeyboardInterrupt()

        self.assertRaises(KeyboardInterrupt, luigi_run,
                          ['KeyboardInterruptTask', '--local-scheduler', '--no-lock'])
        self.assertRaises(KeyboardInterrupt, luigi_run,
                          ['ExternalKeyboardInterruptTask', '--local-scheduler', '--no-lock'])


class WorkerPurgeEventHandlerTest(unittest.TestCase):

    @mock.patch('luigi.worker.TaskProcess')
    def test_process_killed_handler(self, task_proc):
        result = []

        @HangTheWorkerTask.event_handler(Event.PROCESS_FAILURE)
        def store_task(t, error_msg):
            self.assertTrue(error_msg)
            result.append(t)

        w = Worker()
        task = HangTheWorkerTask()
        task_process = mock.MagicMock(is_alive=lambda: False, exitcode=-14, task=task)
        task_proc.return_value = task_process

        w.add(task)
        w._run_task(task.task_id)
        w._handle_next_task()

        self.assertEqual(result, [task])

    @mock.patch('luigi.worker.time')
    def test_timeout_handler(self, mock_time):
        result = []

        @HangTheWorkerTask.event_handler(Event.TIMEOUT)
        def store_task(t, error_msg):
            self.assertTrue(error_msg)
            result.append(t)

        w = Worker(worker_processes=2, wait_interval=0.01, timeout=5)
        mock_time.time.return_value = 0
        task = HangTheWorkerTask(worker_timeout=1)
        w.add(task)
        w._run_task(task.task_id)

        mock_time.time.return_value = 3
        w._handle_next_task()

        self.assertEqual(result, [task])
