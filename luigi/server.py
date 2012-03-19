# Simple REST server that takes commands in JSON

import cherrypy, json, os
import central_planner
import pkg_resources

def json_wrapped(f):
    def f_wrapped(data):
        data = json.loads(data)
        kv = {}
        for k, v in data.iteritems():
            kv[str(k)] = str(v) # Otherwise we have problem with unicode casting
            
        result = f(**kv)
        
        return json.dumps(result)
    return f_wrapped

class Server(object):
    def __init__(self):
        sch = central_planner.CentralPlannerScheduler()

        class API(object): pass
        self.api = API()
        self.api.task = cherrypy.expose(json_wrapped(sch.add_task))
        self.api.dep  = cherrypy.expose(json_wrapped(sch.add_dep))
        self.api.work = cherrypy.expose(json_wrapped(sch.get_work))
        self.api.ping = cherrypy.expose(json_wrapped(sch.ping))
        self.api.status = cherrypy.expose(json_wrapped(sch.status))
        self.draw = cherrypy.expose(lambda: sch.draw()) # TODO: interface
        self.index = self.draw

def run(background=False, pidfile=None, port=8081):
    if background:
        from cherrypy.process.plugins import Daemonizer
        Daemonizer(cherrypy.engine).subscribe()

    if pidfile:
        from cherrypy.process.plugins import PIDFile
        PIDFile(cherrypy.engine, pidfile).subscribe()

    s = Server()
    cherrypy.config.update({'server.socket_host': '0.0.0.0',
                            'server.socket_port': port,
                            'server.thread_pool': 1,
                            'server.thread_pool_max': 1,
                            })

    current_dir = os.path.dirname(os.path.abspath(__file__))
    conf = {'/static': {'tools.staticdir.on': True,
                        'tools.staticdir.dir': os.path.join(current_dir, 'static')}}
    cherrypy.quickstart(s, config = conf)

if __name__ == "__main__":
    run()
