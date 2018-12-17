import logging

from luigi import parameter
from luigi.metrics import MetricsCollector
from luigi.task import Config

logger = logging.getLogger('luigi-interface')

try:
    from datadog import initialize, api, statsd
except ImportError:
    logger.warning("Loading datadog module without datadog installed. Will crash at runtime if datadog functionality is used.")


class datadog(Config):
    api_key = parameter.Parameter(default='dummy_api_key', description='API key provided by Datadog')
    app_key = parameter.Parameter(default='dummy_app_key', description='APP key provided by Datadog')
    default_tags = parameter.Parameter(default='application:luigi', description='Default tags for every events and metrics sent to Datadog')
    environment = parameter.Parameter(default='development', description="Environment of which the pipeline is ran from (eg: 'production', 'staging', ...")
    metric_namespace = parameter.Parameter(default='luigi', description="Default namespace for events and metrics (eg: 'luigi' for 'luigi.task.started')")
    statsd_host = parameter.Parameter(default='localhost', description='StatsD host implementing the Datadog service')
    statsd_port = parameter.IntParameter(default=8125, description='StatsD port implementing the Datadog service')


class DatadogMetricsCollector(MetricsCollector):
    def __init__(self, *args, **kwargs):
        self._config = datadog(**kwargs)

        initialize(api_key=self._config.api_key,
                   app_key=self._config.app_key,
                   statsd_host=self._config.statsd_host,
                   statsd_port=self._config.statsd_port)

    def handle_task_started(self, task):
        title = "Luigi: A task has been started!"
        text = "A task has been started in the pipeline named: {name}".format(name=task.family)
        tags = ["task_name:{name}".format(name=task.family)] + self._format_task_params_to_tags(task)

        self._send_increment('task.started', tags=tags)

        event_tags = tags + ["task_state:STARTED"]
        self._send_event(title=title, text=text, tags=event_tags, alert_type='info', priority='low')

    def handle_task_failed(self, task):
        title = "Luigi: A task has failed!"
        text = "A task has failed in the pipeline named: {name}".format(name=task.family)
        tags = ["task_name:{name}".format(name=task.family)] + self._format_task_params_to_tags(task)

        self._send_increment('task.failed', tags=tags)

        event_tags = tags + ["task_state:FAILED"]
        self._send_event(title=title, text=text, tags=event_tags, alert_type='error', priority='normal')

    def handle_task_disabled(self, task, config):
        title = "Luigi: A task has been disabled!"
        lines = ['A task has been disabled in the pipeline named: {name}.']
        lines.append('The task has failed {failures} times in the last {window}')
        lines.append('seconds, so it is being disabled for {persist} seconds.')

        preformated_text = ' '.join(lines)

        text = preformated_text.format(name=task.family,
                                       persist=config.disable_persist,
                                       failures=config.retry_count,
                                       window=config.disable_window)

        tags = ["task_name:{name}".format(name=task.family)] + self._format_task_params_to_tags(task)

        self._send_increment('task.disabled', tags=tags)

        event_tags = tags + ["task_state:DISABLED"]
        self._send_event(title=title, text=text, tags=event_tags, alert_type='error', priority='normal')

    def handle_task_done(self, task):
        # The task is already done -- Let's not re-create an event
        if task.time_running is None:
            return

        title = "Luigi: A task has been completed!"
        text = "A task has completed in the pipeline named: {name}".format(name=task.family)
        tags = ["task_name:{name}".format(name=task.family)] + self._format_task_params_to_tags(task)

        time_elapse = task.updated - task.time_running

        self._send_increment('task.done', tags=tags)
        self._send_gauge('task.execution_time', time_elapse, tags=tags)

        event_tags = tags + ["task_state:DONE"]
        self._send_event(title=title, text=text, tags=event_tags, alert_type='info', priority='low')

    def _send_event(self, **params):
        params['tags'] += self.default_tags

        api.Event.create(**params)

    def _send_gauge(self, metric_name, value, tags=[]):
        all_tags = tags + self.default_tags

        namespaced_metric = "{namespace}.{metric_name}".format(namespace=self._config.metric_namespace,
                                                               metric_name=metric_name)
        statsd.gauge(namespaced_metric, value, tags=all_tags)

    def _send_increment(self, metric_name, value=1, tags=[]):
        all_tags = tags + self.default_tags

        namespaced_metric = "{namespace}.{metric_name}".format(namespace=self._config.metric_namespace,
                                                               metric_name=metric_name)
        statsd.increment(namespaced_metric, value, tags=all_tags)

    def _format_task_params_to_tags(self, task):
        params = []
        for key, value in task.params.items():
            params.append("{key}:{value}".format(key=key, value=value))

        return params

    @property
    def default_tags(self):
        default_tags = []

        env_tag = "environment:{environment}".format(environment=self._config.environment)
        default_tags.append(env_tag)

        if self._config.default_tags:
            default_tags = default_tags + str.split(self._config.default_tags, ',')

        return default_tags
