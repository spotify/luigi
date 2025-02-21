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
A_PARAM_VALUE = "1"
B_PARAM_VALUE = "2"
C_PARAM_VALUE = "3"


@pytest.mark.contrib
class PrometheusMetricBaseTest(unittest.TestCase):
    COLLECTOR_KWARGS = {}
    EXPECTED_LABELS = {"family": TASK_FAMILY}

    def setUp(self):
        self.collector = PrometheusMetricsCollector(**self.COLLECTOR_KWARGS)
        self.s = Scheduler(metrics_collector=MetricsCollectors.prometheus)
        self.gauge_name = "luigi_task_execution_time_seconds"

    def startTask(self):
        self.s.add_task(
            worker=WORKER,
            task_id=TASK_ID,
            family=TASK_FAMILY,
            params={"a": A_PARAM_VALUE, "b": B_PARAM_VALUE, "c": C_PARAM_VALUE},
        )
        task = self.s._state.get_task(TASK_ID)
        task.time_running = 0
        task.updated = 5
        return task

    def test_handle_task_started(self):
        task = self.startTask()
        self.collector.handle_task_started(task)

        counter_name = 'luigi_task_started_total'
        gauge_name = self.gauge_name
        labels = self.EXPECTED_LABELS

        assert (
            self.collector.registry.get_sample_value(counter_name, labels=labels) == 1
        )
        assert self.collector.registry.get_sample_value(gauge_name, labels=labels) == 0

    def test_handle_task_failed(self):
        task = self.startTask()
        self.collector.handle_task_failed(task)

        counter_name = 'luigi_task_failed_total'
        gauge_name = self.gauge_name
        labels = self.EXPECTED_LABELS

        assert self.collector.registry.get_sample_value(counter_name, labels=labels) == 1
        assert self.collector.registry.get_sample_value(gauge_name, labels=labels) == task.updated - task.time_running

    def test_handle_task_disabled(self):
        task = self.startTask()
        self.collector.handle_task_disabled(task, self.s._config)

        counter_name = 'luigi_task_disabled_total'
        gauge_name = self.gauge_name
        labels = self.EXPECTED_LABELS

        assert self.collector.registry.get_sample_value(counter_name, labels=labels) == 1
        assert self.collector.registry.get_sample_value(gauge_name, labels=labels) == task.updated - task.time_running

    def test_handle_task_done(self):
        task = self.startTask()
        self.collector.handle_task_done(task)

        counter_name = 'luigi_task_done_total'
        gauge_name = self.gauge_name
        labels = self.EXPECTED_LABELS

        assert self.collector.registry.get_sample_value(counter_name, labels=labels) == 1
        assert self.collector.registry.get_sample_value(gauge_name, labels=labels) == task.updated - task.time_running

    def test_configure_http_handler(self):
        mock_http_handler = mock.MagicMock()
        self.collector.configure_http_handler(mock_http_handler)
        mock_http_handler.set_header.assert_called_once_with('Content-Type', CONTENT_TYPE_LATEST)


@pytest.mark.contrib
class PrometheusMetricTaskParamsOnlyTest(PrometheusMetricBaseTest):
    COLLECTOR_KWARGS = {
        "use_task_family_in_labels": False,
        "task_parameters_to_use_in_labels": ["a", "c"],
    }
    EXPECTED_LABELS = {"a": A_PARAM_VALUE, "c": C_PARAM_VALUE}


@pytest.mark.contrib
class PrometheusMetricTaskFamilyAndTaskParamsTest(PrometheusMetricBaseTest):
    COLLECTOR_KWARGS = {
        "use_task_family_in_labels": True,
        "task_parameters_to_use_in_labels": ["b"],
    }
    EXPECTED_LABELS = {"family": TASK_FAMILY, "b": B_PARAM_VALUE}
