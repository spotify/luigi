# -*- coding: utf-8 -*-

from helpers import unittest
import mock
import time

from luigi.contrib.datadog_metric import DatadogMetricsCollector
from luigi.metrics import MetricsCollectors
from luigi.scheduler import Scheduler

WORKER = 'myworker'


class DatadogMetricTest(unittest.TestCase):
    def setUp(self):
        self.mockDatadog()
        self.time = time.time
        self.collector = DatadogMetricsCollector()
        self.s = Scheduler(metrics_collector=MetricsCollectors.datadog)

    def tearDown(self):
        self.unMockDatadog()

        if time.time != self.time:
            time.time = self.time

    def startTask(self, scheduler=None):
        if scheduler:
            s = scheduler
        else:
            s = self.s

        s.add_task(worker=WORKER, task_id='DDTaskID', family='DDTaskName')
        task = s._state.get_task('DDTaskID')

        task.time_running = 0
        return task

    def mockDatadog(self):
        self.create_patcher = mock.patch('datadog.api.Event.create')
        self.mock_create = self.create_patcher.start()

        self.increment_patcher = mock.patch('datadog.statsd.increment')
        self.mock_increment = self.increment_patcher.start()

        self.gauge_patcher = mock.patch('datadog.statsd.gauge')
        self.mock_gauge = self.gauge_patcher.start()

    def unMockDatadog(self):
        self.create_patcher.stop()
        self.increment_patcher.stop()
        self.gauge_patcher.stop()

    def setTime(self, t):
        time.time = lambda: t

    def test_send_event_on_task_started(self):
        task = self.startTask()
        self.collector.handle_task_started(task)

        self.mock_create.assert_called_once_with(alert_type='info',
                                                 priority='low',
                                                 tags=['task_name:DDTaskName',
                                                       'task_state:STARTED',
                                                       'environment:development',
                                                       'application:luigi'],
                                                 text='A task has been started in the pipeline named: DDTaskName',
                                                 title='Luigi: A task has been started!')

    def test_send_increment_on_task_started(self):
        task = self.startTask()
        self.collector.handle_task_started(task)

        self.mock_increment.assert_called_once_with('luigi.task.started', 1, tags=['task_name:DDTaskName',
                                                                                   'environment:development',
                                                                                   'application:luigi'])

    def test_send_event_on_task_failed(self):
        task = self.startTask()
        self.collector.handle_task_failed(task)

        self.mock_create.assert_called_once_with(alert_type='error',
                                                 priority='normal',
                                                 tags=['task_name:DDTaskName',
                                                       'task_state:FAILED',
                                                       'environment:development',
                                                       'application:luigi'],
                                                 text='A task has failed in the pipeline named: DDTaskName',
                                                 title='Luigi: A task has failed!')

    def test_send_increment_on_task_failed(self):
        task = self.startTask()
        self.collector.handle_task_failed(task)

        self.mock_increment.assert_called_once_with('luigi.task.failed', 1, tags=['task_name:DDTaskName',
                                                                                  'environment:development',
                                                                                  'application:luigi'])

    def test_send_event_on_task_disabled(self):
        s = Scheduler(metrics_collector=MetricsCollectors.datadog, disable_persist=10, retry_count=2, disable_window=2)
        task = self.startTask(scheduler=s)
        self.collector.handle_task_disabled(task, s._config)

        self.mock_create.assert_called_once_with(alert_type='error',
                                                 priority='normal',
                                                 tags=['task_name:DDTaskName',
                                                       'task_state:DISABLED',
                                                       'environment:development',
                                                       'application:luigi'],
                                                 text='A task has been disabled in the pipeline named: DDTaskName. ' +
                                                      'The task has failed 2 times in the last 2 seconds' +
                                                      ', so it is being disabled for 10 seconds.',
                                                 title='Luigi: A task has been disabled!')

    def test_send_increment_on_task_disabled(self):
        task = self.startTask()
        self.collector.handle_task_disabled(task, self.s._config)

        self.mock_increment.assert_called_once_with('luigi.task.disabled', 1, tags=['task_name:DDTaskName',
                                                                                    'environment:development',
                                                                                    'application:luigi'])

    def test_send_event_on_task_done(self):
        task = self.startTask()
        self.collector.handle_task_done(task)

        self.mock_create.assert_called_once_with(alert_type='info',
                                                 priority='low',
                                                 tags=['task_name:DDTaskName',
                                                       'task_state:DONE',
                                                       'environment:development',
                                                       'application:luigi'],
                                                 text='A task has completed in the pipeline named: DDTaskName',
                                                 title='Luigi: A task has been completed!')

    def test_send_increment_on_task_done(self):
        task = self.startTask()
        self.collector.handle_task_done(task)

        self.mock_increment.assert_called_once_with('luigi.task.done', 1, tags=['task_name:DDTaskName',
                                                                                'environment:development',
                                                                                'application:luigi'])

    def test_send_gauge_on_task_done(self):
        self.setTime(0)
        task = self.startTask()
        self.collector.handle_task_done(task)

        self.mock_gauge.assert_called_once_with('luigi.task.execution_time', 0, tags=['task_name:DDTaskName',
                                                                                      'environment:development',
                                                                                      'application:luigi'])
