import abc

from enum import Enum
from luigi import six


class MetricsCollectors(Enum):
    default = 1
    none = 1
    datadog = 2

    @classmethod
    def get(cls, which):
        if which == MetricsCollectors.none:
            return NoMetricsCollector()
        elif which == MetricsCollectors.datadog:
            from luigi.contrib.datadog_metric import DatadogMetricsCollector
            return DatadogMetricsCollector()
        else:
            raise ValueError("MetricsCollectors value ' {0} ' isn't supported", which)


@six.add_metaclass(abc.ABCMeta)
class MetricsCollector(object):
    """Abstractable MetricsCollector base class that can be replace by tool
    specific implementation.
    """

    @abc.abstractmethod
    def __init__(self):
        pass

    @abc.abstractmethod
    def handle_task_started(self, task):
        pass

    @abc.abstractmethod
    def handle_task_failed(self, task):
        pass

    @abc.abstractmethod
    def handle_task_disabled(self, task, config):
        pass

    @abc.abstractmethod
    def handle_task_done(self, task):
        pass


class NoMetricsCollector(MetricsCollector):
    """Empty MetricsCollector when no collector is being used
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
