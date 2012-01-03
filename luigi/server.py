# Just a super ugly prototype at this stage - lots of work remaining
# TODO: move all server HTTP code into rpc.py
#       use some other web framework (Tornado?)
#       move all client HTTP code away from scheduler.py into rpc.py
#       move the Graph class into scheduler.py and rename it RemoteSchedulerServer (or something)
#       what do do with Graph.draw?

import cgi, json, BaseHTTPServer, time

class Task(object):
    def __init__(self, status):
        self.clients = set()
        self.deps = set()
        self.status = status
        self.time = time.time()
        self.retry = None
        self.remove = None

class Graph(object):
    def __init__(self):
        self.__tasks = {}
        self.__retry_delay = 60.0 # seconds - should be much higher later
        self.__remove_delay = 600.0
        self.__client_disconnect_delay = 10.0
        # TODO: we have different timeouts:
        # - Retry timeout (when to retry a failed task)
        # - Client keep alive timeout
        self.__clients = {} # map from id to timestamp (last updated)
        # TODO: have a Client object instead, add more data to it

    def prune(self):
        # Remove clients that disconnected, together with their corresponding
        # tasks
        # TODO: remove dependencies
        delete_clients = []
        for client in self.__clients:
            if self.__clients[client] < time.time() - self.__client_disconnect_delay:
                print 'client', client, 'updated at', self.__clients[client], 'timed out at', time.time(), '-', self.__client_disconnect_delay
                delete_clients.append(client)

        for client in delete_clients:
            self.__clients.pop(client)

        remaining_clients = set(list(self.__clients.keys()))

        # Set timeout deadline for tasks
        for task, t in self.__tasks.iteritems():
            # TODO: don't remove finished tasks in something like 1h, would be interesting to see...
            #       probably we should have the same limit for all tasks - don't remove them within 1h after first update
            if not t.clients.intersection(remaining_clients):
                print 'task', task, 'has clients', self.__tasks[task].clients, 'but only', remaining_clients, 'remain'
                if t.remove == None:
                    t.remove = time.time() + self.__remove_delay # TODO: configure!!
                    

        # TODO: if a running client disconnects, tag all its jobs as FAILED and subject it to the same retry logic

        # Remove tasks that timed out
        remove_tasks = []
        for task, t in self.__tasks.iteritems():
            if t.remove and time.time() > t.remove:
                remove_tasks.append(task)

        for task in remove_tasks:
            self.__tasks.pop(task)

        # Reset FAILED tasks to PENDING if max timeout is reached
        for task in self.__tasks.values():
            if task.status == 'FAILED' and task.retry < time.time():
                task.status = 'PENDING'

    def autoupdate(f):
        def g(self, data):
            if 'client' in data:
                # update timestamp so that we keep track
                # of whenever the client was last active
                client = str(data['client'])
                self.__clients[client] = time.time()
            self.prune()
            return f(self, data)
        return g

    @autoupdate
    def task(self, data):
        task = str(data['task'])
        p = self.__tasks.setdefault(task, Task(status=data.get('status', 'PENDING')))

        allowed_state_changes = set([('RUNNING', 'DONE'), ('RUNNING', 'FAILED'),
                                     ('RUNNING', 'PENDING'), # TODO: Disallow!
                                     ('DONE', 'PENDING'),
                                     ('FAILED', 'PENDING')])

        new_status = str(data.get('status', 'PENDING'))

        print p.status, '->', new_status

        assert p.status == new_status or (p.status, new_status) in allowed_state_changes

        p.status = new_status

        if 'client' in data:
            p.clients.add(str(data['client']))
        return {}

    @autoupdate
    def dep(self, data):
        task = str(data['task'])
        dep_task = str(data['dep-task'])
        print task, '->', dep_task
        print self.__tasks
        # self.__tasks.setdefault(task, Task()).deps.add(dep_task)
        self.__tasks[task].deps.add(dep_task)

    @autoupdate
    def work(self, data):
        client = str(data['client'])

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
            self.__tasks[best_task].status = 'RUNNING'

        return {'task': best_task, 'done': (n_can_do == 0)}

    @autoupdate
    def status(self, data):
        task = str(data['task'])
        status = str(data['status'])
        self.__tasks[task].status = status
        if status == 'FAILED':
            self.__tasks[task].retry = time.time() + self.__retry_delay

        return {}

    @autoupdate
    def ping(self, data):
        return {}
        pass

    @autoupdate
    def draw(self, data):
        import pygraphviz
        graphviz = pygraphviz.AGraph(directed = True)
        n_nodes = 0
        for task, p in self.__tasks.iteritems():
            color = {'PENDING': 'white', 
                     'DONE': 'green',
                     'FAILED': 'red',
                     'RUNNING': 'blue',
                     }[p.status]
            shape = 'box'
            graphviz.add_node(task, label = task, style = 'filled', fillcolor = color, shape = shape)
            n_nodes += 1

        for task, p in self.__tasks.iteritems():
            for dep in p.deps:
                graphviz.add_edge(dep, task)

        #if n_nodes > 0: # Don't draw the graph if it's empty
        graphviz.layout('dot')
        fn = '/tmp/graph.png'
        graphviz.draw(fn)

        data = ''.join([line for line in open(fn)])

        return data

class Server:
    def __init__(self, host = '', port = 8081):
        self.__urls = []
        self.__handlers = {}
        self.__graph = Graph()
        self.__host = host
        self.__port = 8081

    def process(self, cmd, args, handler):
        def json_input(args):
            return json.loads(args.get('data', '{}'))

        def json_output(result):
            page = json.dumps(result)

            handler.send_response(200)
            handler.send_header('content-type', 'text/html')
            handler.end_headers()
            handler.wfile.write(page)

        def png_output(result):
            handler.send_response(200)
            handler.send_header('content-type', 'image/png')
            handler.end_headers()
            handler.wfile.write(result)

        handlers = {'/api/task': (self.__graph.task, json_input, json_output),
                    '/api/dep': (self.__graph.dep, json_input, json_output),
                    '/api/work': (self.__graph.work, json_input, json_output),
                    '/api/status': (self.__graph.status, json_input, json_output),
                    '/api/ping': (self.__graph.ping, json_input, json_output),
                    '/draw': (self.__graph.draw, json_input, png_output)}

        for uri, k in handlers.iteritems():
            if cmd == uri:
                f, input_reader, output_writer = k
                return output_writer(f(input_reader(args)))

    def run(self):
        server = self
        class Handler(BaseHTTPServer.BaseHTTPRequestHandler):
            def do_GET(self):
                p = self.path

                d = p.split('?')
                if len(d) == 2:
                    cmd, tmp = d
                    args = {}
                    for k, v in cgi.parse_qs(tmp).iteritems():
                        args[k] = v[0]

                else:
                    cmd, args = p, {}

                server.process(cmd, args, self)

        httpd = BaseHTTPServer.HTTPServer((self.__host, self.__port), Handler)
        httpd.serve_forever()
        
if __name__ == "__main__":
    s = Server()
    s.run()

