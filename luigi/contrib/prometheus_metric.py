from prometheus_client import CollectorRegistry, Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST
from luigi.metrics import MetricsCollector


class PrometheusMetricsCollector(MetricsCollector):

    def __init__(self):
        super(PrometheusMetricsCollector, self).__init__()
        self.registry = CollectorRegistry()
        self.task_started_counter = Counter(
            'luigi_task_started_total',
            'number of started luigi tasks',
            ['family'],
            registry=self.registry
        )
        self.task_failed_counter = Counter(
            'luigi_task_failed_total',
            'number of failed luigi tasks',
            ['family'],
            registry=self.registry
        )
        self.task_disabled_counter = Counter(
            'luigi_task_disabled_total',
            'number of disabled luigi tasks',
            ['family'],
            registry=self.registry
        )
        self.task_done_counter = Counter(
            'luigi_task_done_total',
            'number of done luigi tasks',
            ['family'],
            registry=self.registry
        )
        self.task_execution_time = Gauge(
            'luigi_task_execution_time_seconds',
            'luigi task execution time in seconds',
            ['family'],
            registry=self.registry
        )

    def generate_latest(self):
        return generate_latest(self.registry)

    def handle_task_started(self, task):
        self.task_started_counter.labels(family=task.family).inc()
        self.task_execution_time.labels(family=task.family)

    def handle_task_failed(self, task):
        self.task_failed_counter.labels(family=task.family).inc()
        self.task_execution_time.labels(family=task.family).set(task.updated - task.time_running)

    def handle_task_disabled(self, task, config):
        self.task_disabled_counter.labels(family=task.family).inc()
        self.task_execution_time.labels(family=task.family).set(task.updated - task.time_running)

    def handle_task_done(self, task):
        self.task_done_counter.labels(family=task.family).inc()
        # time_running can be `None` if task was already complete
        if task.time_running is not None:
            self.task_execution_time.labels(family=task.family).set(task.updated - task.time_running)

    def configure_http_handler(self, http_handler):
        http_handler.set_header('Content-Type', CONTENT_TYPE_LATEST)
