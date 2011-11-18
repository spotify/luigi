# THE WORLD'S UGLIEST WEB SERVER BUT ALL WEB FRAMEWORK SUCK!!!

import cgi, json, BaseHTTPServer, time

class Product:
    def __init__(self):
        self.clients = set()
        self.deps = set()
        self.status = 'PENDING'
        self.time = time.time()

class Graph:
    def __init__(self):
        self.__products = {}

    def product(self, client, product):
        self.__products.setdefault(product, Product()).clients.add(client)
        return {}

    def dep(self, client, product, dep_product):
        self.__products.setdefault(product, Product()).deps.add(dep_product)

    def work(self, client):
        # Algo: iterate over all nodes, find first node with no dependencies
        best_t = float('inf')
        best_product = None
        for product, p in self.__products.iteritems():
            if p.status != 'PENDING': continue
            if client not in p.clients: continue

            ok = True
            for dep in p.deps:
                if dep not in self.__products: ok = False
                elif self.__products[dep].status != 'OK': ok = False

            if ok:
                if p.time < best_t:
                    best_t = p.time
                    best_product = product

        if best_product:
            self.__products[best_product].status = 'RUNNING'

        return {'product': best_product}

    def status(self, client, product, status):
        self.__products[product].status = status

class Server:
    def __init__(self):
        self.__urls = []
        self.__handlers = {}
        self.__graph = Graph()

    def process(self, cmd, args):
        handlers = {'/api/product': self.__graph.product,
                    '/api/dep': self.__graph.dep,
                    '/api/work': self.__graph.work,
                    '/api/status': self.__graph.status}

        for uri, handler in handlers.iteritems():
            if cmd == uri:
                return json.dumps(handler(**args))

    def run(self):
        server = self
        class Handler(BaseHTTPServer.BaseHTTPRequestHandler):
            def do_GET(self):
                p = self.path

                d = p.split('?')
                args = {}
                if len(d) == 2:
                    cmd, tmp = d
                    for k, v in cgi.parse_qs(tmp).iteritems():
                        args[k] = v[0]
                else:
                    cmd = p

                page = server.process(cmd, args)

                self.send_response(200)
                self.send_header('content-type', 'text/html')
                self.end_headers()
                self.wfile.write(page)

        httpd = BaseHTTPServer.HTTPServer(('', 8080), Handler)
        httpd.serve_forever()
        
if __name__ == "__main__":
    s = Server()
    s.run()

