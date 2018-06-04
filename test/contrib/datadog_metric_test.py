# -*- coding: utf-8 -*-

from helpers import unittest, with_config
import mock
import time

import luigi.notifications

from luigi.contrib.datadog_metric import DatadogMetricsCollector
from luigi.scheduler import Scheduler

luigi.notifications.DEBUG = True
WORKER = 'myworker'


class DatadogNotificationTest(unittest.TestCase):
    @with_config({'scheduler': {'metrics_collector': 'datadog'}})
    def setUp(self):
        self.mockDatadog()
        self.collector = DatadogMetricsCollector()

        self.setTime(0)
        self.s = Scheduler()
        self.s.add_task(worker=WORKER, task_id='A', family='MyTask')
        self.task = self.s._state.get_task('A')
        self.task.time_running = 0

    def tearDown(self):
        self.create_patcher.stop()
        self.increment_patcher.stop()
        self.gauge_patcher.stop()

    def mockDatadog(self):
        self.create_patcher = mock.patch('datadog.api.Event.create')
        self.mock_create = self.create_patcher.start()

        self.increment_patcher = mock.patch('datadog.statsd.increment')
        self.mock_increment = self.increment_patcher.start()

        self.gauge_patcher = mock.patch('datadog.statsd.gauge')
        self.mock_gauge = self.gauge_patcher.start()

    def setTime(self, t):
        time.time = lambda: t

    @mock.patch('datadog.api.Event.create')
    def test_send_event_on_task_started(self, create_dd_event):
        self.collector.handle_task_started(self.task)

        create_dd_event.assert_called_once_with(alert_type='info',
                                                priority='low',
                                                tags=['task_name:MyTask',
                                                      'task_state:STARTED',
                                                      'environment:development',
                                                      'application:luigi'],
                                                text='A task has been started in the pipeline named: MyTask',
                                                title='Luigi: A task has been started!')

    @mock.patch('datadog.statsd.increment')
    def test_send_increment_on_task_started(self, increment_dd_counter):
        self.collector.handle_task_started(self.task)

        increment_dd_counter.assert_called_once_with('luigi.task.started', 1, tags=['task_name:MyTask',
                                                                                    'environment:development',
                                                                                    'application:luigi'])

    @mock.patch('datadog.api.Event.create')
    def test_send_event_on_task_failed(self, create_dd_event):
        self.collector.handle_task_failed(self.task)

        create_dd_event.assert_called_once_with(alert_type='error',
                                                priority='normal',
                                                tags=['task_name:MyTask',
                                                      'task_state:FAILED',
                                                      'environment:development',
                                                      'application:luigi'],
                                                text='A task has failed in the pipeline named: MyTask',
                                                title='Luigi: A task has failed!')

    @mock.patch('datadog.statsd.increment')
    def test_send_increment_on_task_failed(self, increment_dd_counter):
        self.collector.handle_task_failed(self.task)

        increment_dd_counter.assert_called_once_with('luigi.task.failed', 1, tags=['task_name:MyTask',
                                                                                   'environment:development',
                                                                                   'application:luigi'])

    @mock.patch('datadog.api.Event.create')
    @with_config({'scheduler': {'metrics_collector': 'datadog', 'disable_persist': '10', 'retry_count': '5', 'disable-window-seconds': '2'}})
    def test_send_event_on_task_disabled(self, create_dd_event):
        sch = luigi.scheduler.Scheduler()
        self.collector.handle_task_disabled(self.task, sch._config)

        create_dd_event.assert_called_once_with(alert_type='error',
                                                priority='normal',
                                                tags=['task_name:MyTask',
                                                      'task_state:DISABLED',
                                                      'environment:development',
                                                      'application:luigi'],
                                                text='A task has been disabled in the pipeline named: MyTask. ' +
                                                     'The task has failed 5 times in the last 2 seconds' +
                                                     ', so it is being disabled for 10 seconds.',
                                                title='Luigi: A task has been disabled!')

    @mock.patch('datadog.statsd.increment')
    def test_send_increment_on_task_disabled(self, increment_dd_counter):
        self.collector.handle_task_disabled(self.task, self.s._config)

        increment_dd_counter.assert_called_once_with('luigi.task.disabled', 1, tags=['task_name:MyTask',
                                                                                     'environment:development',
                                                                                     'application:luigi'])

    @mock.patch('datadog.api.Event.create')
    def test_send_event_on_task_done(self, create_dd_event):
        self.collector.handle_task_done(self.task)

        create_dd_event.assert_called_once_with(alert_type='info',
                                                priority='low',
                                                tags=['task_name:MyTask',
                                                      'task_state:DONE',
                                                      'environment:development',
                                                      'application:luigi'],
                                                text='A task has completed in the pipeline named: MyTask',
                                                title='Luigi: A task has been completed!')

    @mock.patch('datadog.statsd.increment')
    def test_send_increment_on_task_done(self, increment_dd_counter):
        self.collector.handle_task_done(self.task)

        increment_dd_counter.assert_called_once_with('luigi.task.done', 1, tags=['task_name:MyTask',
                                                                                 'environment:development',
                                                                                 'application:luigi'])

    @mock.patch('datadog.statsd.gauge')
    def test_send_gauge_on_task_done(self, gauge_dd):
        self.collector.handle_task_done(self.task)

        gauge_dd.assert_called_once_with('luigi.task.execution_time', 0, tags=['task_name:MyTask',
                                                                               'environment:development',
                                                                               'application:luigi'])
