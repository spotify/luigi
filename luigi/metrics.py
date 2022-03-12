import abc
import importlib

from enum import Enum


class MetricsCollectors(Enum):
    custom = -1
    default = 1
    none = 1
    datadog = 2
    prometheus = 3

    @classmethod
    def get(cls, which, custom_import=None):
        if which == MetricsCollectors.none:
            return NoMetricsCollector()
        elif which == MetricsCollectors.datadog:
            from luigi.contrib.datadog_metric import DatadogMetricsCollector
            return DatadogMetricsCollector()
        elif which == MetricsCollectors.prometheus:
            from luigi.contrib.prometheus_metric import PrometheusMetricsCollector
            return PrometheusMetricsCollector()
        elif which == MetricsCollectors.custom:
            if custom_import is None:
                raise ValueError(f"MetricsCollectors value ' {which} ' is -1 and custom_import is None")

            split_import_string = custom_import.split(".")

            import_path = ".".join(split_import_string[:-1])
            import_class_string = split_import_string[-1]

            mod = importlib.import_module(import_path)
            metrics_class = getattr(mod, import_class_string)

            if issubclass(metrics_class, MetricsCollector):
                return metrics_class()
            else:
                raise ValueError(f"Custom Import: {custom_import} is not a subclass of MetricsCollector")
        else:
            raise ValueError("MetricsCollectors value ' {0} ' isn't supported", which)


class MetricsCollector(metaclass=abc.ABCMeta):
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

    def generate_latest(self):
        return

    def configure_http_handler(self, http_handler):
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
