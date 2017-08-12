import logging

from luigi.metrics import MetricsCollector

from prometheus_client import Counter, Gauge
from prometheus_client.exposition import generate_latest as generate_latest_metrics


PROMETHEUS_CONTENT_TYPE_LATEST = str('text/plain; version=0.0.4; charset=utf-8')
'''Content type of the latest text format'''

class PrometheusMetricsCollector(MetricsCollector):

    def __init__(self, *args, **kwargs):
        super(PrometheusMetricsCollector, self).__init__(*args, **kwargs)

        self.task_counter = Counter('luigi_task_count', 'Number of luigi tasks', ['status', 'task_family'])
        self.worker_counter = Counter('luigi_worker_count', 'Number of luigi Workers', ['status', 'host', 'username'])

    def handle_task_status_change(self, task, status):
        self._increment_task_counter(status.lower(), task.family)

    def handle_worker_status_change(self, worker, status):
        self._increment_worker_counter(status.lower(), worker.info.get('host', None), worker.info.get('username', None))

    def _increment_task_counter(self, status, task_family):
        self.task_counter.labels(status=status, task_family=task_family).inc()

    def _increment_worker_counter(self, status, host, username):
        self.worker_counter.labels(status=status, host=host, username=username).inc()
