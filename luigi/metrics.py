from enum import Enum


class MetricsCollectors(Enum):
    default = 1
    none = 1
    datadog = 2

    @classmethod
    def get(cls, which):
        if which == MetricsCollectors.none:
            return MetricsCollector()
        elif which == MetricsCollectors.datadog:
            from luigi.contrib.datadog_metric import DatadogMetricsCollector
            return DatadogMetricsCollector()
        else:
            raise ValueError("MetricsCollectors value ' {0} ' isn't supported", which)


class MetricsCollector(object):
    """Dummy MetricsCollecter base class that can be replace by tool specific
    implementation.
    """
    def __init__(self):
        pass

    def handle_task_started(self, task):
        pass

    def handle_task_failed(self, task):
        pass

    def handle_task_disabled(self, task, config):
        pass

    def handle_task_done(self, task):
        pass
