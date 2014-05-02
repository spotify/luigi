import pypar as pp
import atexit
import logging

from luigi import build
from luigi.worker import Worker, Event
from luigi.scheduler import *

atexit.register(pp.finalize)

log = logging.getLogger('luigi-interface')


class MasterScheduler(CentralPlannerScheduler):

    def __init__(self, *args, **kwargs):
        super(MasterScheduler, self).__init__(*args, **kwargs)


class MPIScheduler(Scheduler):

    def _sndrcv(self, msg, with_rank=0):
        pp.send(msg, destination=with_rank)
        return pp.receive(source=with_rank)

    def add_task(self, *args, **kwargs):
        return self._sndrcv({'cmd': 'add_task', 'args': args, 'kwargs': kwargs})

    def get_work(self, *args, **kwargs):
        return self._sndrcv({'cmd': 'get_work', 'args': args, 'kwargs': kwargs})

    def ping(self, *args, **kwargs):
        return self._sndrcv({'cmd': 'ping', 'args': args, 'kwargs': kwargs})


class MPIWorker(Worker):

    def __init__(self, scheduler=MPIScheduler(), worker_id=None, ping_interval=None,
                 keep_alive=None, wait_interval=None):
        if not worker_id:
            worker_id = 'mpi-%06d' % pp.rank()
        super(MPIWorker, self).__init__(scheduler, worker_id=worker_id,
                                        ping_interval=ping_interval, keep_alive=keep_alive,
                                        wait_interval=wait_interval)
        self.host = self.host + ':' + self._id


class MasterMPIWorker(MPIWorker):

    def __init__(self, scheduler=CentralPlannerScheduler(), worker_id=None):
        super(MasterMPIWorker, self).__init__(scheduler, worker_id)
        self._task_status = {}

    def _check_complete(self, task):
        return self._task_status.setdefault(task.task_id, task.complete())

    def run(self):
        # Here we block to wait for all workers to be ready for initialisation.
        # At this stage the MasterMPIWorker object (i.e., self) has checked if
        # tasks are already complete. This allows the `SlaveMPIWorker`s to query
        # the `MasterMPIWorker` to see if the task is complete instead of thrashing
        # the (distributed) file system.
        
        log.debug('Syncronising with slaves')
        pp.barrier()

        # Handle the messages from the workers

        slaves_alive = pp.size() - 1 # minus the master

        while slaves_alive > 0:
            msg, status = pp.receive(source=pp.any_source, return_status=True)
            cmd, args, kwargs = msg['cmd'], msg['args'], msg['kwargs']

            try: # to pass along the message to the master scheduler
                func = getattr(self._scheduler, cmd)
                result = func(*args, **kwargs)                
                # send back the result
                pp.send(result, status.source)
            except AttributeError:
                if cmd == 'check_complete':
                    is_complete = False
                    try:
                        task = self._scheduled_tasks[args[0]]
                        is_complete = self._check_complete(task)
                    except KeyError:
                        is_complete = True
                    pp.send(is_complete, status.source)

                elif cmd == 'stop':
                    slaves_alive -= 1


class SlaveMPIWorker(MPIWorker):

    def __init__(self, scheduler=MPIScheduler(), worker_id=None, ping_interval=None,
                 keep_alive=None, wait_interval=None):

        # Here we block to allow the MasterMPIWorker to check the completion 
        # status of all tasks (see `_check_complete`). This is to stop
        # SlaveMPIWorkers from thrashing the (distributed) file system.

        log.debug('Syncronising with master')
        pp.barrier()

        # Now go ahead and initialise.

        super(SlaveMPIWorker, self).__init__(scheduler, worker_id,
                                             ping_interval=ping_interval,
                                             keep_alive=keep_alive,
                                             wait_interval=wait_interval)

    def _check_complete(self, task):
        pp.send({'cmd': 'check_complete', 'args': [task.task_id], 'kwargs': None},
                destination=0)
        return pp.receive(source=0)

    def run(self):
        self._scheduler.ping(self._id)
        super(SlaveMPIWorker, self).run()

    def stop(self):
        pp.send({'cmd': 'stop', 'args': [self._id], 'kwargs': None},
                destination=0)
        super(SlaveMPIWorker, self).stop()


class WorkerSchedulerFactory(object):

    def __init__(self, task_history=None):
        if pp.rank() > 0:
            self.scheduler = MPIScheduler()
            self.worker = SlaveMPIWorker(self.scheduler)
        else: # on master
            self.scheduler = MasterScheduler(task_history=task_history)
            self.worker = MasterMPIWorker(self.scheduler)

    def create_local_scheduler(self):
        return self.scheduler

    def create_remote_scheduler(self, host, port):
        return NotImplemented

    def create_worker(self, scheduler, worker_processes=None):
        return self.worker

def run(tasks, task_history=None):
    sch = WorkerSchedulerFactory(task_history=task_history)
    build(tasks, worker_scheduler_factory=sch, local_scheduler=True)