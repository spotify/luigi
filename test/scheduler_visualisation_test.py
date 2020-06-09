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

import os
import tempfile
import time
from helpers import unittest, RunOnceTask

import luigi
import luigi.notifications
import luigi.scheduler
import luigi.worker

luigi.notifications.DEBUG = True

tempdir = tempfile.mkdtemp()


class DummyTask(luigi.Task):
    task_id = luigi.IntParameter()

    def run(self):
        f = self.output().open('w')
        f.close()

    def output(self):
        return luigi.LocalTarget(os.path.join(tempdir, str(self)))


class FactorTask(luigi.Task):
    product = luigi.IntParameter()

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
    task_namespace = __name__
    task_id = luigi.IntParameter()

    def complete(self):
        return False

    def run(self):
        raise Exception("Error Message")


class OddFibTask(luigi.Task):
    n = luigi.IntParameter()
    done = luigi.BoolParameter(default=True, significant=False)

    def requires(self):
        if self.n > 1:
            yield OddFibTask(self.n - 1, self.done)
            yield OddFibTask(self.n - 2, self.done)

    def complete(self):
        return self.n % 2 == 0 and self.done

    def run(self):
        assert False


class SchedulerVisualisationTest(unittest.TestCase):
    def setUp(self):
        self.scheduler = luigi.scheduler.Scheduler()

    def tearDown(self):
        pass

    def _assert_complete(self, tasks):
        for t in tasks:
            self.assertTrue(t.complete())

    def _build(self, tasks):
        with luigi.worker.Worker(scheduler=self.scheduler, worker_processes=1) as w:
            for t in tasks:
                w.add(t)
            w.run()

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
        self.assertTrue(DummyTask(task_id=1).task_id in graph)
        d1 = graph[DummyTask(task_id=1).task_id]
        self.assertEqual(d1[u'status'], u'DONE')
        self.assertEqual(d1[u'deps'], [])
        self.assertGreaterEqual(d1[u'start_time'], start)
        self.assertLessEqual(d1[u'start_time'], end)
        d2 = graph[DummyTask(task_id=2).task_id]
        self.assertEqual(d2[u'status'], u'DONE')
        self.assertEqual(d2[u'deps'], [])
        self.assertGreaterEqual(d2[u'start_time'], start)
        self.assertLessEqual(d2[u'start_time'], end)

    def test_large_graph_truncate(self):
        class LinearTask(luigi.Task):
            idx = luigi.IntParameter()

            def requires(self):
                if self.idx > 0:
                    yield LinearTask(self.idx - 1)

            def complete(self):
                return False

        root_task = LinearTask(100)

        self.scheduler = luigi.scheduler.Scheduler(max_graph_nodes=10)
        self._build([root_task])

        graph = self.scheduler.dep_graph(root_task.task_id)
        self.assertEqual(10, len(graph))
        expected_nodes = [LinearTask(i).task_id for i in range(100, 90, -1)]
        self.assertCountEqual(expected_nodes, graph)

    def test_large_inverse_graph_truncate(self):
        class LinearTask(luigi.Task):
            idx = luigi.IntParameter()

            def requires(self):
                if self.idx > 0:
                    yield LinearTask(self.idx - 1)

            def complete(self):
                return False

        root_task = LinearTask(100)

        self.scheduler = luigi.scheduler.Scheduler(max_graph_nodes=10)
        self._build([root_task])

        graph = self.scheduler.inverse_dep_graph(LinearTask(0).task_id)
        self.assertEqual(10, len(graph))
        expected_nodes = [LinearTask(i).task_id for i in range(10)]
        self.assertCountEqual(expected_nodes, graph)

    def test_truncate_graph_with_full_levels(self):
        class BinaryTreeTask(RunOnceTask):
            idx = luigi.IntParameter()

            def requires(self):
                if self.idx < 100:
                    return map(BinaryTreeTask, (self.idx * 2, self.idx * 2 + 1))

        root_task = BinaryTreeTask(1)

        self.scheduler = luigi.scheduler.Scheduler(max_graph_nodes=10)
        self._build([root_task])

        graph = self.scheduler.dep_graph(root_task.task_id)
        self.assertEqual(10, len(graph))
        expected_nodes = [BinaryTreeTask(i).task_id for i in range(1, 11)]
        self.assertCountEqual(expected_nodes, graph)

    def test_truncate_graph_with_multiple_depths(self):
        class LinearTask(luigi.Task):
            idx = luigi.IntParameter()

            def requires(self):
                if self.idx > 0:
                    yield LinearTask(self.idx - 1)
                yield LinearTask(0)

            def complete(self):
                return False

        root_task = LinearTask(100)

        self.scheduler = luigi.scheduler.Scheduler(max_graph_nodes=10)
        self._build([root_task])

        graph = self.scheduler.dep_graph(root_task.task_id)
        self.assertEqual(10, len(graph))
        expected_nodes = [LinearTask(i).task_id for i in range(100, 91, -1)] + \
                         [LinearTask(0).task_id]
        self.maxDiff = None
        self.assertCountEqual(expected_nodes, graph)

    def _assert_all_done(self, tasks):
        self._assert_all(tasks, u'DONE')

    def _assert_all(self, tasks, status):
        for task in tasks.values():
            self.assertEqual(task[u'status'], status)

    def test_dep_graph_single(self):
        self._build([FactorTask(1)])
        remote = self._remote()
        dep_graph = remote.dep_graph(FactorTask(product=1).task_id)
        self.assertEqual(len(dep_graph), 1)
        self._assert_all_done(dep_graph)

        d1 = dep_graph.get(FactorTask(product=1).task_id)
        self.assertEqual(type(d1), type({}))
        self.assertEqual(d1[u'deps'], [])

    def test_dep_graph_not_found(self):
        self._build([FactorTask(1)])
        remote = self._remote()
        dep_graph = remote.dep_graph(FactorTask(product=5).task_id)
        self.assertEqual(len(dep_graph), 0)

    def test_inverse_dep_graph_not_found(self):
        self._build([FactorTask(1)])
        remote = self._remote()
        dep_graph = remote.inverse_dep_graph('FactorTask(product=5)')
        self.assertEqual(len(dep_graph), 0)

    def test_dep_graph_tree(self):
        self._build([FactorTask(30)])
        remote = self._remote()
        dep_graph = remote.dep_graph(FactorTask(product=30).task_id)
        self.assertEqual(len(dep_graph), 5)
        self._assert_all_done(dep_graph)

        d30 = dep_graph[FactorTask(product=30).task_id]
        self.assertEqual(sorted(d30[u'deps']), sorted([FactorTask(product=15).task_id, FactorTask(product=2).task_id]))

        d2 = dep_graph[FactorTask(product=2).task_id]
        self.assertEqual(sorted(d2[u'deps']), [])

        d15 = dep_graph[FactorTask(product=15).task_id]
        self.assertEqual(sorted(d15[u'deps']), sorted([FactorTask(product=3).task_id, FactorTask(product=5).task_id]))

        d3 = dep_graph[FactorTask(product=3).task_id]
        self.assertEqual(sorted(d3[u'deps']), [])

        d5 = dep_graph[FactorTask(product=5).task_id]
        self.assertEqual(sorted(d5[u'deps']), [])

    def test_dep_graph_missing_deps(self):
        self._build([BadReqTask(True)])
        dep_graph = self._remote().dep_graph(BadReqTask(succeed=True).task_id)
        self.assertEqual(len(dep_graph), 2)

        suc = dep_graph[BadReqTask(succeed=True).task_id]
        self.assertEqual(suc[u'deps'], [BadReqTask(succeed=False).task_id])

        fail = dep_graph[BadReqTask(succeed=False).task_id]
        self.assertEqual(fail[u'name'], 'BadReqTask')
        self.assertEqual(fail[u'params'], {'succeed': 'False'})
        self.assertEqual(fail[u'status'], 'UNKNOWN')

    def test_dep_graph_diamond(self):
        self._build([FactorTask(12)])
        remote = self._remote()
        dep_graph = remote.dep_graph(FactorTask(product=12).task_id)
        self.assertEqual(len(dep_graph), 4)
        self._assert_all_done(dep_graph)

        d12 = dep_graph[FactorTask(product=12).task_id]
        self.assertEqual(sorted(d12[u'deps']), sorted([FactorTask(product=2).task_id, FactorTask(product=6).task_id]))

        d6 = dep_graph[FactorTask(product=6).task_id]
        self.assertEqual(sorted(d6[u'deps']), sorted([FactorTask(product=2).task_id, FactorTask(product=3).task_id]))

        d3 = dep_graph[FactorTask(product=3).task_id]
        self.assertEqual(sorted(d3[u'deps']), [])

        d2 = dep_graph[FactorTask(product=2).task_id]
        self.assertEqual(sorted(d2[u'deps']), [])

    def test_dep_graph_skip_done(self):
        task = OddFibTask(9)
        self._build([task])
        remote = self._remote()

        task_id = task.task_id
        self.assertEqual(9, len(remote.dep_graph(task_id, include_done=True)))

        skip_done_graph = remote.dep_graph(task_id, include_done=False)
        self.assertEqual(5, len(skip_done_graph))
        for task in skip_done_graph.values():
            self.assertNotEqual('DONE', task['status'])
            self.assertLess(len(task['deps']), 2)

    def test_inverse_dep_graph_skip_done(self):
        self._build([OddFibTask(9, done=False)])
        self._build([OddFibTask(9, done=True)])
        remote = self._remote()

        task_id = OddFibTask(1).task_id
        self.assertEqual(9, len(remote.inverse_dep_graph(task_id, include_done=True)))

        skip_done_graph = remote.inverse_dep_graph(task_id, include_done=False)
        self.assertEqual(5, len(skip_done_graph))
        for task in skip_done_graph.values():
            self.assertNotEqual('DONE', task['status'])
            self.assertLess(len(task['deps']), 2)

    def test_task_list_single(self):
        self._build([FactorTask(7)])
        remote = self._remote()
        tasks_done = remote.task_list('DONE', '')
        self.assertEqual(len(tasks_done), 1)
        self._assert_all_done(tasks_done)

        t7 = tasks_done.get(FactorTask(product=7).task_id)
        self.assertEqual(type(t7), type({}))

        self.assertEqual(remote.task_list('', ''), tasks_done)
        self.assertEqual(remote.task_list('FAILED', ''), {})
        self.assertEqual(remote.task_list('PENDING', ''), {})

    def test_dep_graph_root_has_display_name(self):
        root_task = FactorTask(12)
        self._build([root_task])

        dep_graph = self._remote().dep_graph(root_task.task_id)
        self.assertEqual('FactorTask(product=12)', dep_graph[root_task.task_id]['display_name'])

    def test_dep_graph_non_root_nodes_lack_display_name(self):
        root_task = FactorTask(12)
        self._build([root_task])

        dep_graph = self._remote().dep_graph(root_task.task_id)
        for task_id, node in dep_graph.items():
            if task_id != root_task.task_id:
                self.assertNotIn('display_name', node)

    def test_task_list_failed(self):
        self._build([FailingTask(8)])
        remote = self._remote()
        failed = remote.task_list('FAILED', '')
        self.assertEqual(len(failed), 1)

        f8 = failed.get(FailingTask(task_id=8).task_id)
        self.assertEqual(f8[u'status'], u'FAILED')

        self.assertEqual(remote.task_list('DONE', ''), {})
        self.assertEqual(remote.task_list('PENDING', ''), {})

    def test_task_list_upstream_status(self):
        class A(luigi.ExternalTask):
            def complete(self):
                return False

        class B(luigi.ExternalTask):
            def complete(self):
                return True

        class C(RunOnceTask):
            def requires(self):
                return [A(), B()]

        class F(luigi.Task):
            def complete(self):
                return False

            def run(self):
                raise Exception()

        class D(RunOnceTask):
            def requires(self):
                return [F()]

        class E(RunOnceTask):
            def requires(self):
                return [C(), D()]

        self._build([E()])
        remote = self._remote()

        done = remote.task_list('DONE', '')
        self.assertEqual(len(done), 1)
        db = done.get(B().task_id)
        self.assertEqual(db['status'], 'DONE')

        missing_input = remote.task_list('PENDING', 'UPSTREAM_MISSING_INPUT')
        self.assertEqual(len(missing_input), 2)

        pa = missing_input.get(A().task_id)
        self.assertEqual(pa['status'], 'PENDING')
        self.assertEqual(remote._upstream_status(A().task_id, {}), 'UPSTREAM_MISSING_INPUT')

        pc = missing_input.get(C().task_id)
        self.assertEqual(pc['status'], 'PENDING')
        self.assertEqual(remote._upstream_status(C().task_id, {}), 'UPSTREAM_MISSING_INPUT')

        upstream_failed = remote.task_list('PENDING', 'UPSTREAM_FAILED')
        self.assertEqual(len(upstream_failed), 2)
        pe = upstream_failed.get(E().task_id)
        self.assertEqual(pe['status'], 'PENDING')
        self.assertEqual(remote._upstream_status(E().task_id, {}), 'UPSTREAM_FAILED')

        pe = upstream_failed.get(D().task_id)
        self.assertEqual(pe['status'], 'PENDING')
        self.assertEqual(remote._upstream_status(D().task_id, {}), 'UPSTREAM_FAILED')

        pending = dict(missing_input)
        pending.update(upstream_failed)
        self.assertEqual(remote.task_list('PENDING', ''), pending)
        self.assertEqual(remote.task_list('PENDING', 'UPSTREAM_RUNNING'), {})

        failed = remote.task_list('FAILED', '')
        self.assertEqual(len(failed), 1)
        fd = failed.get(F().task_id)
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
        error = remote.fetch_error(FailingTask(task_id=8).task_id)
        self.assertEqual(error["taskId"], FailingTask(task_id=8).task_id)
        self.assertTrue("Error Message" in error["error"])
        self.assertTrue("Runtime error" in error["error"])
        self.assertTrue("Traceback" in error["error"])

    def test_inverse_deps(self):
        class X(RunOnceTask):
            pass

        class Y(RunOnceTask):
            def requires(self):
                return [X()]

        class Z(RunOnceTask):
            id = luigi.IntParameter()

            def requires(self):
                return [Y()]

        class ZZ(RunOnceTask):
            def requires(self):
                return [Z(1), Z(2)]

        self._build([ZZ()])
        dep_graph = self._remote().inverse_dep_graph(X().task_id)

        def assert_has_deps(task_id, deps):
            self.assertTrue(task_id in dep_graph, '%s not in dep_graph %s' % (task_id, dep_graph))
            task = dep_graph[task_id]
            self.assertEqual(sorted(task['deps']), sorted(deps), '%s does not have deps %s' % (task_id, deps))

        assert_has_deps(X().task_id, [Y().task_id])
        assert_has_deps(Y().task_id, [Z(id=1).task_id, Z(id=2).task_id])
        assert_has_deps(Z(id=1).task_id, [ZZ().task_id])
        assert_has_deps(Z(id=2).task_id, [ZZ().task_id])
        assert_has_deps(ZZ().task_id, [])

    def test_simple_worker_list(self):
        class X(luigi.Task):
            def run(self):
                self._complete = True

            def complete(self):
                return getattr(self, '_complete', False)

        task_x = X()
        self._build([task_x])

        workers = self._remote().worker_list()

        self.assertEqual(1, len(workers))
        worker = workers[0]
        self.assertEqual(task_x.task_id, worker['first_task'])
        self.assertEqual(0, worker['num_pending'])
        self.assertEqual(0, worker['num_uniques'])
        self.assertEqual(0, worker['num_running'])
        self.assertEqual('active', worker['state'])
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
        class X(RunOnceTask):
            n = luigi.IntParameter()

        w = luigi.worker.Worker(worker_id='w', scheduler=self.scheduler, worker_processes=3)
        w.add(X(0))
        w.add(X(1))
        w.add(X(2))
        w.add(X(3))

        self.scheduler.get_work(worker='w')
        self.scheduler.get_work(worker='w')
        self.scheduler.get_work(worker='w')

        workers = self._remote().worker_list()
        self.assertEqual(1, len(workers))
        worker = workers[0]

        self.assertEqual(3, worker['num_running'])
        self.assertEqual(1, worker['num_pending'])
        self.assertEqual(1, worker['num_uniques'])

    def test_worker_list_disabled_worker(self):
        class X(RunOnceTask):
            pass

        with luigi.worker.Worker(worker_id='w', scheduler=self.scheduler) as w:
            w.add(X())  #
            workers = self._remote().worker_list()
            self.assertEqual(1, len(workers))
            self.assertEqual('active', workers[0]['state'])
            self.scheduler.disable_worker('w')
            workers = self._remote().worker_list()
            self.assertEqual(1, len(workers))
            self.assertEqual(1, len(workers))
            self.assertEqual('disabled', workers[0]['state'])
