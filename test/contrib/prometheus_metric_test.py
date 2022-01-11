from helpers import unittest
import pytest
from prometheus_client import CONTENT_TYPE_LATEST

from luigi.contrib.prometheus_metric import PrometheusMetricsCollector
from luigi.metrics import MetricsCollectors
from luigi.scheduler import Scheduler

try:
    from unittest import mock
except ImportError:
    import mock


WORKER = 'myworker'
TASK_ID = 'TaskID'
TASK_FAMILY = 'TaskFamily'


@pytest.mark.contrib
class PrometheusMetricTest(unittest.TestCase):
    def setUp(self):
        self.collector = PrometheusMetricsCollector()
        self.s = Scheduler(metrics_collector=MetricsCollectors.prometheus)
        self.gauge_name = 'luigi_task_execution_time_seconds'
        self.labels = {'family': TASK_FAMILY}

    def startTask(self):
        self.s.add_task(worker=WORKER, task_id=TASK_ID, family=TASK_FAMILY)
        task = self.s._state.get_task(TASK_ID)
        task.time_running = 0
        task.updated = 5
        return task

    def test_handle_task_started(self):
        task = self.startTask()
        self.collector.handle_task_started(task)

        counter_name = 'luigi_task_started_total'
        gauge_name = self.gauge_name
        labels = self.labels

        assert self.collector.registry.get_sample_value(counter_name, labels=self.labels) == 1
        assert self.collector.registry.get_sample_value(gauge_name, labels=labels) == 0

    def test_handle_task_failed(self):
        task = self.startTask()
        self.collector.handle_task_failed(task)

        counter_name = 'luigi_task_failed_total'
        gauge_name = self.gauge_name
        labels = self.labels

        assert self.collector.registry.get_sample_value(counter_name, labels=labels) == 1
        assert self.collector.registry.get_sample_value(gauge_name, labels=labels) == task.updated - task.time_running

    def test_handle_task_disabled(self):
        task = self.startTask()
        self.collector.handle_task_disabled(task, self.s._config)

        counter_name = 'luigi_task_disabled_total'
        gauge_name = self.gauge_name
        labels = self.labels

        assert self.collector.registry.get_sample_value(counter_name, labels=labels) == 1
        assert self.collector.registry.get_sample_value(gauge_name, labels=labels) == task.updated - task.time_running

    def test_handle_task_done(self):
        task = self.startTask()
        self.collector.handle_task_done(task)

        counter_name = 'luigi_task_done_total'
        gauge_name = self.gauge_name
        labels = self.labels

        assert self.collector.registry.get_sample_value(counter_name, labels=labels) == 1
        assert self.collector.registry.get_sample_value(gauge_name, labels=labels) == task.updated - task.time_running

    def test_configure_http_handler(self):
        mock_http_handler = mock.MagicMock()
        self.collector.configure_http_handler(mock_http_handler)
        mock_http_handler.set_header.assert_called_once_with('Content-Type', CONTENT_TYPE_LATEST)
