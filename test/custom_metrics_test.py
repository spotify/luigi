# -*- coding: utf-8 -*-
#
# Copyright 2012-2017 Spotify AB
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

from helpers import LuigiTestCase, temporary_unloaded_module

import luigi
from luigi.metrics import MetricsCollectors
from luigi.scheduler import Scheduler
from luigi.worker import Worker


class CustomMetricsTestMyTask(luigi.Task):
    root_path = luigi.PathParameter()

    n = luigi.IntParameter()

    def output(self):
        basename = "%s_%s.txt" % (self.__class__.__name__, self.n)
        return luigi.LocalTarget(os.path.join(self.root_path, basename))

    def run(self):
        time.sleep(self.n)
        with self.output().open('w') as f:
            f.write("content\n")


class CustomMetricsTestWrapper(CustomMetricsTestMyTask):
    def requires(self):
        return [self.clone(CustomMetricsTestMyTask, n=n) for n in range(self.n)]


METRICS_COLLECTOR_MODULE = b'''
from luigi.metrics import NoMetricsCollector

class CustomMetricsCollector(NoMetricsCollector):
    def __init__(self, *args, **kwargs):
        super(CustomMetricsCollector, self).__init__(*args, **kwargs)
        self.elapsed = {}

    def handle_task_statistics(self, task, statistics):
        if "elapsed" in statistics:
            self.elapsed[(task.family, task.params.get("n"))] = statistics["elapsed"]
'''


TASK_CONTEXT_MODULE = b'''
import time

class CustomTaskContext:
    def __init__(self, task_process):
        self._task_process = task_process
        self._start = None

    def __enter__(self):
        self._start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        assert self._start is not None
        elapsed = time.perf_counter() - self._start
        self._task_process.status_reporter.report_task_statistics({"elapsed": elapsed})
'''


class CustomMetricsTest(LuigiTestCase):
    """
    Test showcasing collection of cutom metrics
    """

    def _run_task_on_worker(self, worker):
        with tempfile.TemporaryDirectory() as tmpdir:
            task = CustomMetricsTestWrapper(n=3, root_path=tmpdir)
            self.assertTrue(worker.add(task))
            worker.run()
            self.assertTrue(task.complete())

    def _create_worker_and_run_task(self, scheduler):
        with temporary_unloaded_module(TASK_CONTEXT_MODULE) as task_context_module:
            with Worker(scheduler=scheduler, worker_id='X', task_process_context=task_context_module + '.CustomTaskContext') as worker:
                self._run_task_on_worker(worker)

    def test_custom_metrics(self):
        with temporary_unloaded_module(METRICS_COLLECTOR_MODULE) as metrics_collector_module:
            scheduler = Scheduler(metrics_collector=MetricsCollectors.custom, metrics_custom_import=metrics_collector_module + '.CustomMetricsCollector')
            self._create_worker_and_run_task(scheduler)
            for (family, n), elapsed in scheduler._state._metrics_collector.elapsed.items():
                self.assertTrue(family in {'CustomMetricsTestMyTask', 'CustomMetricsTestWrapper'})
                self.assertTrue(elapsed >= float(n))
