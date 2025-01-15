from prometheus_client import CollectorRegistry, Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST
from luigi import parameter
from luigi.metrics import MetricsCollector
from luigi.task import Config


class prometheus(Config):
    use_task_family_in_labels = parameter.BoolParameter(
        default=True, parsing=parameter.BoolParameter.EXPLICIT_PARSING
    )
    task_parameters_to_use_in_labels = parameter.ListParameter(default=[])


class PrometheusMetricsCollector(MetricsCollector):

    def _generate_task_labels(self, task):
        return {
            label: task.family if label == "family" else task.params.get(label)
            for label in self.labels
        }

    def __init__(self, *args, **kwargs):
        super(PrometheusMetricsCollector, self).__init__()
        self.registry = CollectorRegistry()
        config = prometheus(**kwargs)
        self.labels = list(config.task_parameters_to_use_in_labels)
        if config.use_task_family_in_labels:
            self.labels += ["family"]
        if not self.labels:
            raise ValueError("Prometheus labels cannot be empty (see prometheus configuration)")
        self.task_started_counter = Counter(
            'luigi_task_started_total',
            'number of started luigi tasks',
            self.labels,
            registry=self.registry
        )
        self.task_failed_counter = Counter(
            'luigi_task_failed_total',
            'number of failed luigi tasks',
            self.labels,
            registry=self.registry
        )
        self.task_disabled_counter = Counter(
            'luigi_task_disabled_total',
            'number of disabled luigi tasks',
            self.labels,
            registry=self.registry
        )
        self.task_done_counter = Counter(
            'luigi_task_done_total',
            'number of done luigi tasks',
            self.labels,
            registry=self.registry
        )
        self.task_execution_time = Gauge(
            'luigi_task_execution_time_seconds',
            'luigi task execution time in seconds',
            self.labels,
            registry=self.registry
        )

    def generate_latest(self):
        return generate_latest(self.registry)

    def handle_task_started(self, task):
        self.task_started_counter.labels(**self._generate_task_labels(task)).inc()
        self.task_execution_time.labels(**self._generate_task_labels(task))

    def handle_task_failed(self, task):
        self.task_failed_counter.labels(**self._generate_task_labels(task)).inc()
        self.task_execution_time.labels(**self._generate_task_labels(task)).set(task.updated - task.time_running)

    def handle_task_disabled(self, task, config):
        self.task_disabled_counter.labels(**self._generate_task_labels(task)).inc()
        self.task_execution_time.labels(**self._generate_task_labels(task)).set(task.updated - task.time_running)

    def handle_task_done(self, task):
        self.task_done_counter.labels(**self._generate_task_labels(task)).inc()
        # time_running can be `None` if task was already complete
        if task.time_running is not None:
            self.task_execution_time.labels(**self._generate_task_labels(task)).set(task.updated - task.time_running)

    def configure_http_handler(self, http_handler):
        http_handler.set_header('Content-Type', CONTENT_TYPE_LATEST)
