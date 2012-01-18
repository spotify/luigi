# Simple REST server that takes commands in JSON
# TODO: use some other web framework (Tornado?)

import cgi, json, BaseHTTPServer
import central_planner

class Server:
    def __init__(self, host = '', port = 8081):
        self.__urls = []
        self.__handlers = {}
        self.__host = host
        self.__port = 8081

        self.__scheduler = central_planner.CentralPlannerScheduler()

        def json_input(args):
            # TODO: support arbitrary types, not just strings
            data = json.loads(args.get('data', '{}'))
            kv = {}
            for k, v in data.iteritems():
                kv[str(k)] = str(v) # The problem is that otherwise we have problem with unicode casting

            return kv

        def json_output(result, handler):
            page = json.dumps(result)

            handler.send_response(200)
            handler.send_header('content-type', 'text/html')
            handler.end_headers()
            handler.wfile.write(page)

        def svg_output(result, handler):
            handler.send_response(200)
            handler.send_header('content-type', 'image/svg+xml')
            handler.end_headers()
            handler.wfile.write(result)

        self.__handlers = {'/api/task': (self.__scheduler.add_task, json_input, json_output),
                           '/api/dep': (self.__scheduler.add_dep, json_input, json_output),
                           '/api/work': (self.__scheduler.get_work, json_input, json_output),
                           '/api/status': (self.__scheduler.status, json_input, json_output),
                           '/api/ping': (self.__scheduler.ping, json_input, json_output),
                           '/draw': (self.__scheduler.draw, json_input, svg_output), # TODO: duck typing, assumes a Central Planner
                           '/': (self.__scheduler.draw, json_input, svg_output)} # just an alias

    def process(self, cmd, args, handler):
        f, input_reader, output_writer = self.__handlers[str(cmd)]
        input = input_reader(args)
        result = f(**input)
        output = output_writer(result, handler)
        return output

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

