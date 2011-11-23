# Just a super ugly prototype at this stage - lots of work remaining

import cgi, json, BaseHTTPServer, time

class Task(object):
    def __init__(self):
        self.clients = set()
        self.deps = set()
        self.status = 'PENDING'
        self.time = time.time()
        self.retry = None

class Graph(object):
    def __init__(self):
        self.__tasks = {}
        self.__timeout = 10.0 # seconds - should be much higher later

    def task(self, data):
        task = str(data['task'])
        client = str(data['client'])
        self.__tasks.setdefault(task, Task())
        self.__tasks[task].clients.add(client)
        return {}

    def dep(self, data):
        task = str(data['task'])
        dep_task = str(data['dep-task'])
        self.__tasks.setdefault(task, Task()).deps.add(dep_task)

    def work(self, data):
        client = str(data['client'])

        # TODO: remove any expired nodes

        # Algo: iterate over all nodes, find first node with no dependencies
        # TODO: remove tasks that can't be done, figure out if the client has absolutely
        # nothing it can wait for

        best_t = float('inf')
        best_task = None
        for task, p in self.__tasks.iteritems():
            if p.status == 'FAILED':
                if time.time() < p.retry:
                    continue
            elif p.status != 'PENDING':
                continue
            if client not in p.clients: continue

            ok = True
            for dep in p.deps:
                if dep not in self.__tasks: ok = False
                elif self.__tasks[dep].status != 'OK': ok = False

            if ok:
                if p.time < best_t:
                    best_t = p.time
                    best_task = task

        if best_task:
            self.__tasks[best_task].status = 'RUNNING'

        return {'task': best_task}

    def status(self, data):
        task = str(data['task'])
        status = str(data['status'])
        self.__tasks[task].status = status
        if status == 'FAILED':
            self.__tasks[task].retry = time.time() + self.__timeout

        return {}

    def draw(self, data):
        import pygraphviz
        graphviz = pygraphviz.AGraph(directed = True)
        n_nodes = 0
        for task, p in self.__tasks.iteritems():
            color = {'PENDING': 'white', 
                     'OK': 'green',
                     'FAILED': 'red',
                     'RUNNING': 'blue',
                     }[p.status]
            shape = 'diamond'
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
    def __init__(self):
        self.__urls = []
        self.__handlers = {}
        self.__graph = Graph()

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

        httpd = BaseHTTPServer.HTTPServer(('', 8081), Handler)
        httpd.serve_forever()
        
if __name__ == "__main__":
    s = Server()
    s.run()

