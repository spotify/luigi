import logging
from mpi4py import MPI
from luigi import build
from luigi.worker import Worker, Event
from luigi.scheduler import CentralPlannerScheduler, Scheduler

log = logging.getLogger('luigi-interface')

COMM = MPI.COMM_WORLD

def sndrcv(msg, with_rank=0):
    log.debug('[%i] %s', COMM.Get_rank(), msg['cmd'])
    COMM.send(msg, with_rank)
    result = COMM.recv(source=with_rank)
    return result


class MasterScheduler(CentralPlannerScheduler):

    def __init__(self, *args, **kwargs):
        super(MasterScheduler, self).__init__(*args, **kwargs)


class MPIScheduler(Scheduler):

    def add_task(self, *args, **kwargs):
        return sndrcv({'cmd': 'add_task', 'args': args, 'kwargs': kwargs})

    def add_worker(self, *args, **kwargs):
        return sndrcv({'cmd': 'add_worker', 'args': args, 'kwargs': kwargs})

    def get_work(self, *args, **kwargs):
        return sndrcv({'cmd': 'get_work', 'args': args, 'kwargs': kwargs})

    def ping(self, *args, **kwargs):
        return sndrcv({'cmd': 'ping', 'args': args, 'kwargs': kwargs})


class MPIWorker(Worker):

    def __init__(self, scheduler=MPIScheduler(), worker_id=None,
                 ping_interval=None, keep_alive=None, wait_interval=None):
        if not worker_id:
            worker_id = 'mpi-%06d' % COMM.Get_rank()
        super(MPIWorker, self).__init__(scheduler, worker_id=worker_id,
                                        ping_interval=ping_interval,
                                        keep_alive=keep_alive,
                                        wait_interval=wait_interval)
        self.host = self.host + ':' + self._id


class MasterMPIWorker(MPIWorker):

    def __init__(self, scheduler=CentralPlannerScheduler(), worker_id=None):
        super(MasterMPIWorker, self).__init__(scheduler, worker_id)
        self._task_status = {}

    def _check_complete(self, task):
        return self._task_status.setdefault(task.task_id, task.complete())

    def run(self):
        # Here we block to wait for all workers to be ready for
        # initialisation.  At this stage the MasterMPIWorker object
        # (i.e., self) has checked if tasks are already complete. This
        # allows the `SlaveMPIWorker`s to query the `MasterMPIWorker` to
        # see if the task is complete instead of thrashing the
        # (distributed) file system.

        log.debug('Syncronising with slaves')
        COMM.Barrier()

        # Handle the messages from the workers

        slaves_alive = COMM.Get_size() - 1  # minus the master
        status = MPI.Status()

        while slaves_alive > 0:
            msg = COMM.recv(status=status)
            cmd, args, kwargs = msg['cmd'], msg['args'], msg['kwargs']

            try:  # to pass along the message to the master scheduler
                func = getattr(self._scheduler, cmd)
                result = func(*args, **kwargs)
                # send back the result
                COMM.send(result, status.source)
            except AttributeError:
                if cmd == 'check_complete':
                    is_complete = False
                    try:
                        task = self._scheduled_tasks[args[0]]
                        is_complete = self._check_complete(task)
                    except KeyError:
                        is_complete = True
                    COMM.send(is_complete, status.source)

                elif cmd == 'stop':
                    slaves_alive -= 1

        return True


class SlaveMPIWorker(MPIWorker):

    def __init__(self, scheduler=MPIScheduler(), worker_id=None,
                 ping_interval=None, keep_alive=None, wait_interval=None):

        # Here we block to allow the MasterMPIWorker to check the
        # completion status of all tasks (see `_check_complete`). This
        # is to stop SlaveMPIWorkers from thrashing the (distributed)
        # file system.

        log.debug('Syncronising with master')
        COMM.Barrier()

        # Now go ahead and initialise.

        super(SlaveMPIWorker, self).__init__(scheduler, worker_id,
                                             ping_interval=ping_interval,
                                             keep_alive=keep_alive,
                                             wait_interval=wait_interval)

    def _check_complete(self, task):
        COMM.send({'cmd': 'check_complete', 'args': [task.task_id],
                 'kwargs': None}, 0)
        return COMM.recv(source=0)

    def run(self):
        self._scheduler.ping(self._id)
        return super(SlaveMPIWorker, self).run()

    def stop(self):
        COMM.send({'cmd': 'stop', 'args': [self._id], 'kwargs': None}, 0)
        super(SlaveMPIWorker, self).stop()


class WorkerSchedulerFactory(object):

    def __init__(self, task_history=None):
        if COMM.Get_rank() > 0:
            self.scheduler = MPIScheduler()
            self.worker = SlaveMPIWorker(self.scheduler)
        else:  # on master
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
