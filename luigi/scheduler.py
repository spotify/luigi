import logging
import time
logger = logging.getLogger("luigi-interface")


class Scheduler(object):
    ''' Abstract base class

    Note that the methods all take string arguments, not Task objects...
    '''

    add_task = NotImplemented
    add_dep = NotImplemented
    get_work = NotImplemented
    ping = NotImplemented  # TODO: remove?
    status = NotImplemented  # merge with add_task?


class DummyScheduler(Scheduler):
    ''' DEPRECATED

    For local scheduling, now we are using CentralPlanner but with no RPC in between.
    However, this class is left as an example of a super small Schedule implementation
    that actually works fairly well.
    '''
    def __init__(self):
        self.__schedule = []

    def add_task(self, task, status, worker):
        if status == 'PENDING':
            self.__schedule.append(task)

    def add_dep(self, task, status, worker):
        pass

    def get_work(self, worker):
        if len(self.__schedule):
            # TODO: check for dependencies:
            #for task_2 in task.deps():
            #    if not task_2.complete():
            #        print task,'has dependency', task_2, 'which is not complete',
            #        break
            return False, self.__schedule.pop()
        else:
            return True, None

    def status(self, task, status, expl, worker):
        pass


class Task(object):
    def __init__(self, status):
        self.workers = set()
        self.deps = set()
        self.status = status
        self.time = time.time()
        self.retry = None
        self.remove = None
        self.worker_running = None

_default_worker = 'default-worker'  # for testing


class CentralPlannerScheduler(Scheduler):
    ''' Async scheduler that can handle multiple workers etc

    Can be run locally or on a server (using RemoteScheduler + server.Server).
    '''
    def __init__(self, retry_delay=60.0, remove_delay=600.0, worker_disconnect_delay=60.0):  # seconds
        self.__tasks = {}
        self.__retry_delay = retry_delay
        self.__remove_delay = remove_delay
        self.__worker_disconnect_delay = worker_disconnect_delay
        self.__workers = {}  # map from id to timestamp (last updated)
        # TODO: have a Worker object instead, add more data to it

    def prune(self):
        # Remove workers that disconnected, together with their corresponding tasks
        # TODO: remove dependencies? (But they should always have the same worker right? So it's unnecessary)

        delete_workers = []
        for worker in self.__workers:
            if self.__workers[worker] < time.time() - self.__worker_disconnect_delay:
                print 'worker', worker, 'updated at', self.__workers[worker], 'timed out at', time.time(), '-', self.__worker_disconnect_delay
                delete_workers.append(worker)

        for worker in delete_workers:
            self.__workers.pop(worker)

        remaining_workers = set(list(self.__workers.keys()))

        # Remove tasks corresponding to disconnected workers
        for task, t in self.__tasks.iteritems():
            if not t.workers.intersection(remaining_workers):
                if t.remove == None:
                    print 'task', task, 'has workers', self.__tasks[task].workers, 'but only', remaining_workers, 'remain -> will remove task in', self.__remove_delay, 'seconds'
                    t.remove = time.time() + self.__remove_delay  # TODO: configure!!

            if t.status == 'RUNNING' and t.worker_running and t.worker_running not in remaining_workers:
                # If a running worker disconnects, tag all its jobs as FAILED and subject it to the same retry logic
                print 'task', task, 'is running by worker', t.worker_running, 'but only', remaining_workers, 'remain -> will reset task'
                t.worker_running = None
                t.status = 'FAILED'
                t.retry = time.time() + self.__retry_delay

        # Remove tasks that timed out
        remove_tasks = []
        for task, t in self.__tasks.iteritems():
            if t.remove and time.time() > t.remove:
                print 'Removing task', task
                remove_tasks.append(task)

        for task in remove_tasks:
            self.__tasks.pop(task)

        # Reset FAILED tasks to PENDING if max timeout is reached
        for task in self.__tasks.values():
            if task.status == 'FAILED' and task.retry < time.time():
                task.status = 'PENDING'

    def autoupdate(f):
        def g(self, *args, **kwargs):
            # update timestamp so that we keep track
            # of whenever the worker was last active
            worker = kwargs.get('worker', _default_worker)
            self.__workers[worker] = time.time()
            self.prune()
            return f(self, *args, **kwargs)
        return g

    @autoupdate
    def add_task(self, task, worker=_default_worker, status='PENDING'):
        p = self.__tasks.setdefault(task, Task(status=status))

        disallowed_state_changes = set([('RUNNING', 'PENDING')])

        if (p.status, status) not in disallowed_state_changes:
            p.status = status
            p.workers.add(worker)
            p.remove = None
            p.deps.clear()

    @autoupdate
    def add_dep(self, task, dep_task, worker=_default_worker):
        # print task, '->', dep_task
        # print self.__tasks
        # self.__tasks.setdefault(task, Task()).deps.add(dep_task)
        self.__tasks[task].deps.add(dep_task)

    @autoupdate
    def get_work(self, worker=_default_worker):
        # TODO: remove any expired nodes

        # Algo: iterate over all nodes, find first node with no dependencies
        # TODO: remove tasks that can't be done, figure out if the worker has absolutely
        # nothing it can wait for

        best_t = float('inf')
        best_task = None
        n_can_do = 0  # stupid thingie
        for task, p in self.__tasks.iteritems():
            if worker not in p.workers:
                continue

            if p.status != 'PENDING':
                continue

            n_can_do += 1

            ok = True
            for dep in p.deps:
                if dep not in self.__tasks:
                    ok = False
                elif self.__tasks[dep].status != 'DONE':
                    ok = False

            if ok:
                if p.time < best_t:
                    best_t = p.time
                    best_task = task

        if best_task:
            t = self.__tasks[best_task]
            t.status = 'RUNNING'
            t.worker_running = worker

        return (n_can_do == 0), best_task

    @autoupdate
    def status(self, task, status, worker=_default_worker, expl=None):
        self.__tasks[task].status = status
        if status == 'FAILED':
            self.__tasks[task].retry = time.time() + self.__retry_delay

    @autoupdate
    def ping(self, worker=_default_worker):
        # TODO: if run locally, there is no need to ping this scheduler obviously!
        pass  # autoupdate will take care of it

    def graph(self):
        serialized = {}
        for taskname, task in self.__tasks.iteritems():
            serialized[taskname] = {
                'deps': list(task.deps),
                'status': task.status
            }
        return serialized
