# Simple REST server that takes commands in JSON

import cherrypy, json
import central_planner

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
        self.draw = cherrypy.expose(lambda: sch.draw())
        self.index = self.draw

if __name__ == "__main__":
    s = Server()
    cherrypy.config.update({'server.socket_host': '0.0.0.0',
                            'server.socket_port': 8081,})
    cherrypy.quickstart(s)

