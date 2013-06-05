# Copyright (c) 2012 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

# Simple REST server that takes commands in a JSON payload
import json
import os
import atexit
import mimetypes
import tornado.ioloop
import tornado.web
import tornado.httpclient
import tornado.httpserver
import configuration
import scheduler
import pkg_resources
import signal
from rpc import RemoteSchedulerResponder


def _create_scheduler():
    config = configuration.get_config()
    retry_delay = config.getfloat('scheduler', 'retry-delay', 900.0)
    remove_delay = config.getfloat('scheduler', 'remove-delay', 600.0)
    worker_disconnect_delay = config.getfloat('scheduler', 'worker-disconnect-delay', 60.0)
    return scheduler.CentralPlannerScheduler(retry_delay, remove_delay, worker_disconnect_delay)


class RPCHandler(tornado.web.RequestHandler):
    """ Handle remote scheduling calls using rpc.RemoteSchedulerResponder"""
    scheduler = _create_scheduler()
    api = RemoteSchedulerResponder(scheduler)

    def get(self, method):
        payload = self.get_argument('data', default="{}")
        arguments = json.loads(payload)

        if hasattr(self.api, method):
            result = getattr(self.api, method)(**arguments)
            self.write({"response": result})  # wrap all json response in a dictionary
        else:
            self.send_error(400)


class StaticFileHandler(tornado.web.RequestHandler):
    def get(self, path):
        # TODO: this is probably not the right way to do it...
        # TODO: security
        extension = os.path.splitext(path)[1]
        if extension in mimetypes.types_map:
            self.set_header("Content-Type", mimetypes.types_map[extension])
        data = pkg_resources.resource_string(__name__, os.path.join("static", path))
        self.write(data)


class RootPathHandler(tornado.web.RequestHandler):
    def get(self):
        self.redirect("/static/visualiser/index.html")


def app(debug):
    api_app = tornado.web.Application([
        (r'/api/(.*)', RPCHandler),
        (r'/static/(.*)', StaticFileHandler),
        (r'/', RootPathHandler)
    ], debug=debug)
    return api_app


def run(api_port=8082):
    """ Runs one instance of the API server
    """
    api_app = app(debug=False)

    api_sockets = tornado.netutil.bind_sockets(api_port)

    print "Launching API instance"

    # load scheduler state
    RPCHandler.scheduler.load()

    # prune work DAG every 10 seconds
    pruner = tornado.ioloop.PeriodicCallback(RPCHandler.scheduler.prune, 10000)
    pruner.start()

    def shutdown_handler(foo=None, bar=None):
        print "api instance shutting down..."
        RPCHandler.scheduler.dump()
        os._exit(0)

    server = tornado.httpserver.HTTPServer(api_app)
    server.add_sockets(api_sockets)

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGQUIT, shutdown_handler)
    atexit.register(shutdown_handler)

    tornado.ioloop.IOLoop.instance().start()


def run_api_threaded(api_port=8082):
    ''' For unit tests'''

    api_app = app(debug=False)

    api_sockets = tornado.netutil.bind_sockets(api_port)

    server = tornado.httpserver.HTTPServer(api_app)
    server.add_sockets(api_sockets)

    def run_tornado():
        tornado.ioloop.IOLoop.instance().start()

    import threading
    threading.Thread(target=run_tornado).start()


def stop():
    tornado.ioloop.IOLoop.instance().stop()


if __name__ == "__main__":
    run()
