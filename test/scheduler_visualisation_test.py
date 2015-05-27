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
from __future__ import division

import os
import tempfile
import time
from helpers import unittest

import luigi
import luigi.notifications
import luigi.scheduler
import luigi.worker

luigi.notifications.DEBUG = True

tempdir = tempfile.mkdtemp()


class DummyTask(luigi.Task):
    task_id = luigi.Parameter()

    def run(self):
        f = self.output().open('w')
        f.close()

    def output(self):
        return luigi.LocalTarget(os.path.join(tempdir, str(self.task_id)))


class FactorTask(luigi.Task):
    product = luigi.Parameter()

    def requires(self):
        for factor in range(2, self.product):
            if self.product % factor == 0:
                yield FactorTask(factor)
                yield FactorTask(self.product // factor)
                return

    def run(self):
        f = self.output().open('w')
        f.close()

    def output(self):
        return luigi.LocalTarget(os.path.join(tempdir, 'luigi_test_factor_%d' % self.product))


class BadReqTask(luigi.Task):
    succeed = luigi.BoolParameter()

    def requires(self):
        assert self.succeed
        yield BadReqTask(False)

    def run(self):
        pass

    def complete(self):
        return False


class FailingTask(luigi.Task):
    task_id = luigi.Parameter()

    def run(self):
        raise Exception("Error Message")


class SchedulerVisualisationTest(unittest.TestCase):
    # The following 2 are required to retain compatibility with python 2.6

    def assertGreaterEqual(self, a, b):
        self.assertTrue(a >= b)

    def assertLessEqual(self, a, b):
        self.assertTrue(a <= b)

    def setUp(self):
        self.scheduler = luigi.scheduler.CentralPlannerScheduler()

    def tearDown(self):
        pass

    def _assert_complete(self, tasks):
        for t in tasks:
            self.assert_(t.complete())

    def _build(self, tasks):
        w = luigi.worker.Worker(scheduler=self.scheduler, worker_processes=1)
        for t in tasks:
            w.add(t)
        w.run()
        w.stop()

    def _remote(self):
        return self.scheduler

    def _test_run(self, workers):
        tasks = [DummyTask(i) for i in range(20)]
        self._build(tasks, workers=workers)
        self._assert_complete(tasks)

    def test_graph(self):
        start = time.time()
        tasks = [DummyTask(task_id=1), DummyTask(task_id=2)]
        self._build(tasks)
        self._assert_complete(tasks)
        end = time.time()

        remote = self._remote()
        graph = remote.graph()
        self.assertEqual(len(graph), 2)
        self.assert_(u'DummyTask(task_id=1)' in graph)
        d1 = graph[u'DummyTask(task_id=1)']
        self.assertEqual(d1[u'status'], u'DONE')
        self.assertEqual(d1[u'deps'], [])
        self.assertGreaterEqual(d1[u'start_time'], start)
        self.assertLessEqual(d1[u'start_time'], end)
        d2 = graph[u'DummyTask(task_id=2)']
        self.assertEqual(d2[u'status'], u'DONE')
        self.assertEqual(d2[u'deps'], [])
        self.assertGreaterEqual(d2[u'start_time'], start)
        self.assertLessEqual(d2[u'start_time'], end)

    def _assert_all_done(self, tasks):
        self._assert_all(tasks, u'DONE')

    def _assert_all(self, tasks, status):
        for task in tasks.values():
            self.assertEqual(task[u'status'], status)

    def test_dep_graph_single(self):
        self._build([FactorTask(1)])
        remote = self._remote()
        dep_graph = remote.dep_graph('FactorTask(product=1)')
        self.assertEqual(len(dep_graph), 1)
        self._assert_all_done(dep_graph)

        d1 = dep_graph.get(u'FactorTask(product=1)')
        self.assertEqual(type(d1), type({}))
        self.assertEqual(d1[u'deps'], [])

    def test_dep_graph_not_found(self):
        self._build([FactorTask(1)])
        remote = self._remote()
        dep_graph = remote.dep_graph('FactorTask(product=5)')
        self.assertEqual(len(dep_graph), 0)

    def test_dep_graph_tree(self):
        self._build([FactorTask(30)])
        remote = self._remote()
        dep_graph = remote.dep_graph('FactorTask(product=30)')
        self.assertEqual(len(dep_graph), 5)
        self._assert_all_done(dep_graph)

        d30 = dep_graph[u'FactorTask(product=30)']
        self.assertEqual(sorted(d30[u'deps']), [u'FactorTask(product=15)', 'FactorTask(product=2)'])

        d2 = dep_graph[u'FactorTask(product=2)']
        self.assertEqual(sorted(d2[u'deps']), [])

        d15 = dep_graph[u'FactorTask(product=15)']
        self.assertEqual(sorted(d15[u'deps']), [u'FactorTask(product=3)', 'FactorTask(product=5)'])

        d3 = dep_graph[u'FactorTask(product=3)']
        self.assertEqual(sorted(d3[u'deps']), [])

        d5 = dep_graph[u'FactorTask(product=5)']
        self.assertEqual(sorted(d5[u'deps']), [])

    def test_dep_graph_missing_deps(self):
        self._build([BadReqTask(True)])
        dep_graph = self._remote().dep_graph('BadReqTask(succeed=True)')
        self.assertEqual(len(dep_graph), 2)

        suc = dep_graph[u'BadReqTask(succeed=True)']
        self.assertEqual(suc[u'deps'], [u'BadReqTask(succeed=False)'])

        fail = dep_graph[u'BadReqTask(succeed=False)']
        self.assertEqual(fail[u'name'], 'BadReqTask')
        self.assertEqual(fail[u'params'], {'succeed': 'False'})
        self.assertEqual(fail[u'status'], 'UNKNOWN')

    def test_dep_graph_diamond(self):
        self._build([FactorTask(12)])
        remote = self._remote()
        dep_graph = remote.dep_graph('FactorTask(product=12)')
        self.assertEqual(len(dep_graph), 4)
        self._assert_all_done(dep_graph)

        d12 = dep_graph[u'FactorTask(product=12)']
        self.assertEqual(sorted(d12[u'deps']), [u'FactorTask(product=2)', 'FactorTask(product=6)'])

        d6 = dep_graph[u'FactorTask(product=6)']
        self.assertEqual(sorted(d6[u'deps']), [u'FactorTask(product=2)', 'FactorTask(product=3)'])

        d3 = dep_graph[u'FactorTask(product=3)']
        self.assertEqual(sorted(d3[u'deps']), [])

        d2 = dep_graph[u'FactorTask(product=2)']
        self.assertEqual(sorted(d2[u'deps']), [])

    def test_task_list_single(self):
        self._build([FactorTask(7)])
        remote = self._remote()
        tasks_done = remote.task_list('DONE', '')
        self.assertEqual(len(tasks_done), 1)
        self._assert_all_done(tasks_done)

        t7 = tasks_done.get(u'FactorTask(product=7)')
        self.assertEqual(type(t7), type({}))

        self.assertEqual(remote.task_list('', ''), tasks_done)
        self.assertEqual(remote.task_list('FAILED', ''), {})
        self.assertEqual(remote.task_list('PENDING', ''), {})

    def test_task_list_failed(self):
        self._build([FailingTask(8)])
        remote = self._remote()
        failed = remote.task_list('FAILED', '')
        self.assertEqual(len(failed), 1)

        f8 = failed.get(u'FailingTask(task_id=8)')
        self.assertEqual(f8[u'status'], u'FAILED')

        self.assertEqual(remote.task_list('DONE', ''), {})
        self.assertEqual(remote.task_list('PENDING', ''), {})

    def test_task_list_upstream_status(self):
        class A(luigi.ExternalTask):
            pass

        class B(luigi.ExternalTask):

            def complete(self):
                return True

        class C(luigi.Task):

            def requires(self):
                return [A(), B()]

        class F(luigi.Task):

            def run(self):
                raise Exception()

        class D(luigi.Task):

            def requires(self):
                return [F()]

        class E(luigi.Task):

            def requires(self):
                return [C(), D()]

        self._build([E()])
        remote = self._remote()

        done = remote.task_list('DONE', '')
        self.assertEqual(len(done), 1)
        db = done.get('B()')
        self.assertEqual(db['status'], 'DONE')

        missing_input = remote.task_list('PENDING', 'UPSTREAM_MISSING_INPUT')
        self.assertEqual(len(missing_input), 2)

        pa = missing_input.get(u'A()')
        self.assertEqual(pa['status'], 'PENDING')
        self.assertEqual(remote._upstream_status('A()', {}), 'UPSTREAM_MISSING_INPUT')

        pc = missing_input.get(u'C()')
        self.assertEqual(pc['status'], 'PENDING')
        self.assertEqual(remote._upstream_status('C()', {}), 'UPSTREAM_MISSING_INPUT')

        upstream_failed = remote.task_list('PENDING', 'UPSTREAM_FAILED')
        self.assertEqual(len(upstream_failed), 2)
        pe = upstream_failed.get(u'E()')
        self.assertEqual(pe['status'], 'PENDING')
        self.assertEqual(remote._upstream_status('E()', {}), 'UPSTREAM_FAILED')

        pe = upstream_failed.get(u'D()')
        self.assertEqual(pe['status'], 'PENDING')
        self.assertEqual(remote._upstream_status('D()', {}), 'UPSTREAM_FAILED')

        pending = dict(missing_input)
        pending.update(upstream_failed)
        self.assertEqual(remote.task_list('PENDING', ''), pending)
        self.assertEqual(remote.task_list('PENDING', 'UPSTREAM_RUNNING'), {})

        failed = remote.task_list('FAILED', '')
        self.assertEqual(len(failed), 1)
        fd = failed.get('F()')
        self.assertEqual(fd['status'], 'FAILED')

        all = dict(pending)
        all.update(done)
        all.update(failed)
        self.assertEqual(remote.task_list('', ''), all)
        self.assertEqual(remote.task_list('RUNNING', ''), {})

    def test_task_search(self):
        self._build([FactorTask(8)])
        self._build([FailingTask(8)])
        remote = self._remote()
        all_tasks = remote.task_search('Task')
        self.assertEqual(len(all_tasks), 2)
        self._assert_all(all_tasks['DONE'], 'DONE')
        self._assert_all(all_tasks['FAILED'], 'FAILED')

    def test_fetch_error(self):
        self._build([FailingTask(8)])
        remote = self._remote()
        error = remote.fetch_error("FailingTask(task_id=8)")
        self.assertEqual(error["taskId"], "FailingTask(task_id=8)")
        self.assertTrue("Error Message" in error["error"])
        self.assertTrue("Runtime error" in error["error"])
        self.assertTrue("Traceback" in error["error"])

    def test_inverse_deps(self):
        class X(luigi.Task):
            pass

        class Y(luigi.Task):

            def requires(self):
                return [X()]

        class Z(luigi.Task):
            id = luigi.Parameter()

            def requires(self):
                return [Y()]

        class ZZ(luigi.Task):

            def requires(self):
                return [Z(1), Z(2)]

        self._build([ZZ()])
        dep_graph = self._remote().inverse_dep_graph('X()')

        def assert_has_deps(task_id, deps):
            self.assertTrue(task_id in dep_graph, '%s not in dep_graph %s' % (task_id, dep_graph))
            task = dep_graph[task_id]
            self.assertEqual(sorted(task['deps']), sorted(deps), '%s does not have deps %s' % (task_id, deps))

        assert_has_deps('X()', ['Y()'])
        assert_has_deps('Y()', ['Z(id=1)', 'Z(id=2)'])
        assert_has_deps('Z(id=1)', ['ZZ()'])
        assert_has_deps('Z(id=2)', ['ZZ()'])
        assert_has_deps('ZZ()', [])

    def test_simple_worker_list(self):
        class X(luigi.Task):

            def run(self):
                self._complete = True

            def complete(self):
                return getattr(self, '_complete', False)

        self._build([X()])

        workers = self._remote().worker_list()

        self.assertEqual(1, len(workers))
        worker = workers[0]
        self.assertEqual('X()', worker['first_task'])
        self.assertEqual(0, worker['num_pending'])
        self.assertEqual(0, worker['num_uniques'])
        self.assertEqual(0, worker['num_running'])
        self.assertEqual(1, worker['workers'])

    def test_worker_list_pending_uniques(self):
        class X(luigi.Task):

            def complete(self):
                return False

        class Y(X):

            def requires(self):
                return X()

        class Z(Y):
            pass

        w1 = luigi.worker.Worker(scheduler=self.scheduler, worker_processes=1)
        w2 = luigi.worker.Worker(scheduler=self.scheduler, worker_processes=1)

        w1.add(Y())
        w2.add(Z())

        workers = self._remote().worker_list()
        self.assertEqual(2, len(workers))
        for worker in workers:
            self.assertEqual(2, worker['num_pending'])
            self.assertEqual(1, worker['num_uniques'])
            self.assertEqual(0, worker['num_running'])

    def test_worker_list_running(self):
        class X(luigi.Task):
            n = luigi.IntParameter()

        w = luigi.worker.Worker(scheduler=self.scheduler, worker_processes=3)
        w.add(X(0))
        w.add(X(1))
        w.add(X(2))
        w.add(X(3))

        w._get_work()
        w._get_work()
        w._get_work()

        workers = self._remote().worker_list()
        self.assertEqual(1, len(workers))
        worker = workers[0]

        self.assertEqual(3, worker['num_running'])
        self.assertEqual(1, worker['num_pending'])
        self.assertEqual(1, worker['num_uniques'])


if __name__ == '__main__':
    unittest.main()
