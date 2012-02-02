# Just a super ugly prototype at this stage - lots of work remaining

import time
import re
import pkg_resources
import scheduler

class Task(object):
    def __init__(self, status):
        self.workers = set()
        self.deps = set()
        self.status = status
        self.time = time.time()
        self.retry = None
        self.remove = None
        self.worker_running = None

_default_worker = 'default-worker' # for testing

class CentralPlannerScheduler(scheduler.Scheduler):
    ''' Async scheduler that can handle multiple workers etc

    Can be run locally or on a server (using RemoteScheduler + server.Server).
    '''
    def __init__(self, retry_delay=60.0, remove_delay=600.0, worker_disconnect_delay=60.0): # seconds
        self.__tasks = {}
        self.__retry_delay = retry_delay
        self.__remove_delay = remove_delay
        self.__worker_disconnect_delay = worker_disconnect_delay
        self.__workers = {} # map from id to timestamp (last updated)
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
                    t.remove = time.time() + self.__remove_delay # TODO: configure!!

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

        # allowed_state_changes = set([('RUNNING', 'DONE'), ('RUNNING', 'FAILED'),
        #                             ('RUNNING', 'PENDING'), # TODO: Disallow!
        #                             ('DONE', 'PENDING'),
        #                             ('FAILED', 'PENDING')])

        # print 'task', task, ':', p.status, '->', status

        # assert p.status == status or (p.status, status) in allowed_state_changes

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
        n_can_do = 0 # stupid thingie
        for task, p in self.__tasks.iteritems():
            if worker not in p.workers:
                continue

            if p.status != 'PENDING': continue

            n_can_do += 1

            ok = True
            for dep in p.deps:
                if dep not in self.__tasks: ok = False
                elif self.__tasks[dep].status != 'DONE': ok = False

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
        pass # autoupdate will take care of it

    @autoupdate
    def draw(self):
        import pygraphviz

        # TODO: if there are too many nodes, we need to prune the view
        # One idea: do a Dijkstra from all running nodes. Hide all nodes
        # with distance >= 50.

        graphviz = pygraphviz.AGraph(directed=True, size=12)
        n_nodes = 0
        for task, p in self.__tasks.iteritems():
            color = {'PENDING': 'white', 
                     'DONE': 'green',
                     'FAILED': 'red',
                     'RUNNING': 'blue',
                     'BROKEN': 'orange', # external task, can't run
                     }[p.status]
            shape = 'box'
            label = task.replace('(', '\\n(').replace(',', ',\\n') # force GraphViz to break lines
            # TODO: if the ( or , is a part of the argument we shouldn't really break it
            graphviz.add_node(task, label=label, style='filled', fillcolor=color, shape=shape, fontname='Helvetica', fontsize=11)
            n_nodes += 1

        for task, p in self.__tasks.iteritems():
            for dep in p.deps:
                graphviz.add_edge(dep, task)

        if n_nodes < 100:
            graphviz.layout('dot')
        else:
            # stupid workaround...
            graphviz.layout('fdp')
        fn = '/tmp/graph.svg'
        graphviz.draw(fn)
        
        html_header = pkg_resources.resource_string(__name__, 'static/header.html')

        svg = ''.join([line for line in open(fn)])
        
        pattern = r'(<svg.*?)(<g id="graph1".*?)(</svg>)'
        mo = re.search(pattern, svg, re.S)
        
        return ''.join([html_header,
         mo.group(1),
         '<g id="viewport">',
         mo.group(2),
        '</g>',
         mo.group(3),
         "</body></html>"])

