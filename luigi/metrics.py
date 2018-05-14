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
