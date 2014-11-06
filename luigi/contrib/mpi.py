"""
MPI Task Scheduling
"""

import logging
import socket
import collections
import multiprocessing
from mpi4py import MPI
from luigi import build, configuration
from luigi.worker import Worker, Event
from luigi.scheduler import CentralPlannerScheduler, Scheduler

import cPickle as pickle

log = logging.getLogger('luigi-interface')

COMM = MPI.COMM_WORLD

def send(msg, dest=0):
    #log.debug('%i->%i: %s', COMM.Get_rank(), dest, msg)
    COMM.send(msg, dest)

def recv(source=MPI.ANY_SOURCE):
    status = MPI.Status()
    msg = COMM.recv(source=source, status=status)
    #log.debug('%i<-%i: %s', COMM.Get_rank(), status.source, msg)
    return msg, status

def sndrcv(msg, with_rank=0):
    COMM.send(msg, with_rank)
    return COMM.recv(source=with_rank)

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
        return True
        #return sndrcv({'cmd': 'ping', 'args': args, 'kwargs': kwargs})


class MPIWorker(Worker):

    #def __init__(self, scheduler=MPIScheduler(), worker_id=None,
    #             worker_processes=1, ping_interval=None, keep_alive=None,
    #             wait_interval=None, max_reschedules=None, count_uniques=None):
    #    super(MPIWorker, self).__init__(scheduler=scheduler, 
    #                                    worker_id=worker_id,
    #                                    worker_processes=worker_processes,
    #                                    ping_interval=ping_interval,
    #                                    keep_alive=keep_alive,
    #                                    wait_interval=wait_interval,
    #                                    max_reschedules=max_reschedules,
    #                                    count_uniques=count_uniques)

    def __init__(self, scheduler=MPIScheduler(), worker_id=None,
                 worker_processes=1, ping_interval=None, keep_alive=None,
                 wait_interval=None, max_reschedules=None, count_uniques=None):
        self.worker_processes = int(worker_processes)
        self._worker_info = self._generate_worker_info()

        if not worker_id:
            worker_id = 'Worker(%s)' % ', '.join(['%s=%s' % (k, v) for k, v in self._worker_info])

        config = configuration.get_config()

        if ping_interval is None:
            ping_interval = config.getfloat('core', 'worker-ping-interval', 1.0)

        if keep_alive is None:
            keep_alive = config.getboolean('core', 'worker-keep-alive', False)
        self.keep_alive = keep_alive

        # worker-count-uniques means that we will keep a worker alive only if it has a unique
        # pending task, as well as having keep-alive true
        if count_uniques is None:
            count_uniques = config.getboolean('core', 'worker-count-uniques', False)
        self._count_uniques = count_uniques

        if wait_interval is None:
            wait_interval = config.getint('core', 'worker-wait-interval', 1)
        self._wait_interval = wait_interval

        if max_reschedules is None:
            max_reschedules = config.getint('core', 'max-reschedules', 1)
        self._max_reschedules = max_reschedules

        self._id = worker_id
        self._scheduler = scheduler

        self.host = socket.gethostname()
        self._scheduled_tasks = {}
        self._suspended_tasks = {}

        self._first_task = None

        self.add_succeeded = True
        self.run_succeeded = True
        self.unfulfilled_counts = collections.defaultdict(int)

        # Keep info about what tasks are running (could be in other processes)
        self._task_result_queue = multiprocessing.Queue()
        self._running_tasks = {}

    def _generate_worker_info(self):
        args = super(MPIWorker, self)._generate_worker_info()
        args += [('rank', COMM.Get_rank())]
        return args


class MasterMPIWorker(MPIWorker):

    def __init__(self, scheduler=MasterScheduler(), worker_id=None,
                 worker_processes=1, ping_interval=None, keep_alive=None,
                 wait_interval=None, max_reschedules=None, count_uniques=None):
        super(MasterMPIWorker, self).__init__(scheduler=scheduler, 
                                              worker_id=worker_id,
                                              worker_processes=worker_processes,
                                              ping_interval=ping_interval,
                                              keep_alive=keep_alive,
                                              wait_interval=wait_interval,
                                              max_reschedules=max_reschedules,
                                              count_uniques=count_uniques)
        self._task_status = {}

    def _check_complete(self, task):
        return self._task_status.setdefault(task.task_id, task.complete())

    def stop(self):
        pass

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
            msg, status = recv()
            cmd, args, kwargs = msg['cmd'], msg['args'], msg['kwargs']

            try:  # to pass along the message to the master scheduler
                func = getattr(self._scheduler, cmd)
                result = func(*args, **kwargs)
                # send back the result
                send(result, status.source)
            except AttributeError:
                if cmd == 'check_complete':
                    is_complete = False
                    try:
                        task = self._scheduled_tasks[args[0]]
                        is_complete = self._check_complete(task)
                    except KeyError:
                        is_complete = True
                    send(is_complete, status.source)

                elif cmd == 'stop':
                    slaves_alive -= 1

        return True


class SlaveMPIWorker(MPIWorker):

    def __init__(self, scheduler=MPIScheduler(), worker_id=None,
                 worker_processes=1, ping_interval=None, keep_alive=None,
                 wait_interval=None, max_reschedules=None, count_uniques=None):

        # Here we block to allow the MasterMPIWorker to check the
        # completion status of all tasks (see `_check_complete`). This
        # is to stop SlaveMPIWorkers from thrashing the (distributed)
        # file system.

        log.debug('Syncronising with master')
        COMM.Barrier()

        # Now go ahead and initialise.
        
        super(SlaveMPIWorker, self).__init__(scheduler=scheduler, 
                                             worker_id=worker_id,
                                             worker_processes=worker_processes,
                                             ping_interval=ping_interval,
                                             keep_alive=keep_alive,
                                             wait_interval=wait_interval,
                                             max_reschedules=max_reschedules,
                                             count_uniques=count_uniques)

    def _check_complete(self, task):
        send({'cmd': 'check_complete',
              'args': [task.task_id],
              'kwargs': None}, 0)
        result, status = recv(source=0)
        return result

    def _run_task(self, task_id):
        task = self._scheduled_tasks[task_id]
        return super(SlaveMPIWorker, self)._run_task(task_id)

    def stop(self):
        send({'cmd': 'stop', 'args': [self._id], 'kwargs': None}, 0)


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
