

class MetricsCollector(object):
    """
    Dummy MetricsCollecter base class that can be replace by tool specific implementation
    """
    def __init__(self, scheduler):
        self._scheduler = scheduler

    def handle_task_status_change(self, task, status):
        pass

    def handle_worker_status_change(self, worker, status):
        pass
