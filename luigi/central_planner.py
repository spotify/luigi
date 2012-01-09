# Just a super ugly prototype at this stage - lots of work remaining

import time
import scheduler

class Task(object):
    def __init__(self, status):
        self.clients = set()
        self.deps = set()
        self.status = status
        self.time = time.time()
        self.retry = None
        self.remove = None
        self.client_running = None

_default_client = 'default-client'

class CentralPlannerScheduler(scheduler.Scheduler):
    ''' Async scheduler that can handle multiple clients etc

    Can be run locally or on a server (using RemoteScheduler + server.Server).
    '''
    def __init__(self, retry_delay=60.0, remove_delay=600.0, client_disconnect_delay=60.0): # seconds
        self.__tasks = {}
        self.__retry_delay = retry_delay
        self.__remove_delay = remove_delay
        self.__client_disconnect_delay = client_disconnect_delay
        self.__clients = {} # map from id to timestamp (last updated)
        # TODO: have a Client object instead, add more data to it

    def prune(self):
        # Remove clients that disconnected, together with their corresponding tasks
        # TODO: remove dependencies? (But they should always have the same client right? So it's unnecessary)

        delete_clients = []
        for client in self.__clients:
            if self.__clients[client] < time.time() - self.__client_disconnect_delay:
                print 'client', client, 'updated at', self.__clients[client], 'timed out at', time.time(), '-', self.__client_disconnect_delay
                delete_clients.append(client)

        for client in delete_clients:
            self.__clients.pop(client)

        remaining_clients = set(list(self.__clients.keys()))

        # Remove tasks corresponding to disconnected clients
        for task, t in self.__tasks.iteritems():
            if not t.clients.intersection(remaining_clients):
                print 'task', task, 'has clients', self.__tasks[task].clients, 'but only', remaining_clients, 'remain -> will remove task in', self.__remove_delay, 'seconds'
                if t.remove == None:
                    t.remove = time.time() + self.__remove_delay # TODO: configure!!

            if t.status == 'RUNNING' and t.client_running and t.client_running not in remaining_clients:
                # If a running client disconnects, tag all its jobs as FAILED and subject it to the same retry logic
                print 'task', task, 'is running by client', t.client_running, 'but only', remaining_clients, 'remain -> will reset task'
                t.client_running = None
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
            # of whenever the client was last active
            client = kwargs.get('client', _default_client)
            self.__clients[client] = time.time()
            self.prune()
            return f(self, *args, **kwargs)
        return g

    @autoupdate
    def add_task(self, task, status='PENDING', client=_default_client):
        p = self.__tasks.setdefault(task, Task(status=status))

        # allowed_state_changes = set([('RUNNING', 'DONE'), ('RUNNING', 'FAILED'),
        #                             ('RUNNING', 'PENDING'), # TODO: Disallow!
        #                             ('DONE', 'PENDING'),
        #                             ('FAILED', 'PENDING')])

        print 'task', task, ':', p.status, '->', status

        # assert p.status == status or (p.status, status) in allowed_state_changes

        p.status = status

        p.clients.add(client)

    @autoupdate
    def add_dep(self, task, dep_task, client=_default_client):
        print task, '->', dep_task
        # print self.__tasks
        # self.__tasks.setdefault(task, Task()).deps.add(dep_task)
        self.__tasks[task].deps.add(dep_task)

    @autoupdate
    def get_work(self, client=_default_client):
        # TODO: remove any expired nodes

        # Algo: iterate over all nodes, find first node with no dependencies
        # TODO: remove tasks that can't be done, figure out if the client has absolutely
        # nothing it can wait for

        best_t = float('inf')
        best_task = None
        n_can_do = 0 # stupid thingie
        for task, p in self.__tasks.iteritems():
            if client not in p.clients:
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
            t.client_running = client

        return (n_can_do == 0), best_task

    @autoupdate
    def status(self, task, status, client=_default_client, expl=None):
        self.__tasks[task].status = status
        if status == 'FAILED':
            self.__tasks[task].retry = time.time() + self.__retry_delay

    @autoupdate
    def ping(self, client=_default_client):
        # TODO: if run locally, there is no need to ping this scheduler obviously!
        pass # autoupdate will take care of it

    @autoupdate
    def draw(self):
        import pygraphviz
        graphviz = pygraphviz.AGraph(directed=True, size=12)
        n_nodes = 0
        for task, p in self.__tasks.iteritems():
            color = {'PENDING': 'white', 
                     'DONE': 'green',
                     'FAILED': 'red',
                     'RUNNING': 'blue',
                     }[p.status]
            shape = 'box'
            label = task.replace('(', '\\n(').replace(',', ',\\n') # force GraphViz to break lines
            # TODO: if the ( or , is a part of the argument we shouldn't really break it
            graphviz.add_node(task, label=label, style='filled', fillcolor=color, shape=shape, fontname='Helvetica', fontsize=11)
            n_nodes += 1

        for task, p in self.__tasks.iteritems():
            for dep in p.deps:
                graphviz.add_edge(dep, task)

        #if n_nodes > 0: # Don't draw the graph if it's empty
        graphviz.layout('dot')
        fn = '/tmp/graph.svg'
        graphviz.draw(fn)

        data = ''.join([line for line in open(fn)])

        return data

