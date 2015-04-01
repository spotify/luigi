# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
Simple REST server that takes commands in a JSON payload
Interface to the :py:class:`~luigi.scheduler.CentralPlannerScheduler` class.
See :doc:`/central_scheduler` for more info.
"""
#
# Description: Added codes for visualization of how long each task takes
# running-time until it reaches the next status (failed or done)
# At "{base_url}/tasklist", all completed(failed or done) tasks are shown.
# At "{base_url}/tasklist", a user can select one specific task to see
# how its running-time has changed over time.
# At "{base_url}/tasklist/{task_name}", it visualizes a multi-bar graph
# that represents the changes of the running-time for a selected task
# up to the next status (failed or done).
# This visualization let us know how the running-time of the specific task
# has changed over time.
#
# Copyright 2015 Naver Corp.
# Author Yeseul Park (yeseul.park@navercorp.com)
#

import atexit
import json
import logging
import mimetypes
import os
import posixpath
import signal
import sys
import datetime
import time

import pkg_resources
import tornado.httpclient
import tornado.httpserver
import tornado.ioloop
import tornado.netutil
import tornado.web

from luigi import configuration
from luigi.scheduler import CentralPlannerScheduler


logger = logging.getLogger("luigi.server")


class RPCHandler(tornado.web.RequestHandler):
    """
    Handle remote scheduling calls using rpc.RemoteSchedulerResponder.
    """

    def initialize(self, scheduler):
        self._scheduler = scheduler

    def get(self, method):
        payload = self.get_argument('data', default="{}")
        arguments = json.loads(payload)

        # TODO: we should probably denote all methods on the scheduler that are "API-level"
        # versus internal methods. Right now you can do a REST method call to any method
        # defined on the scheduler, which is pretty bad from a security point of view.

        if hasattr(self._scheduler, method):
            result = getattr(self._scheduler, method)(**arguments)
            self.write({"response": result})  # wrap all json response in a dictionary
        else:
            self.send_error(404)

    post = get


class BaseTaskHistoryHandler(tornado.web.RequestHandler):

    def initialize(self, scheduler):
        self._scheduler = scheduler

    def get_template_path(self):
        return pkg_resources.resource_filename(__name__, 'templates')


class AllRunHandler(BaseTaskHistoryHandler):

    def get(self):
        all_tasks = self._scheduler.task_history.find_all_runs()
        tasknames = []
        for task in all_tasks:
            tasknames.append(task.name)
        # show all tasks with their name list to be selected
        # why all tasks? the duration of the event history of a selected task
        # can be more than 24 hours.
        self.render("menu.html", tasknames=tasknames)


class SelectedRunHandler(BaseTaskHistoryHandler):

    def get(self, name):
        tasks = {}
        statusResults = {}
        taskResults = []
        # get all tasks that has been updated
        all_tasks = self._scheduler.task_history.find_all_runs()
        # get events history for all tasks
        all_tasks_event_history = self._scheduler.task_history.find_all_events()
        for task in all_tasks:
            task_seq = task.id
            task_name = task.name
            # build the dictionary, tasks with index: id, value: task_name
            tasks[task_seq] = str(task_name)
        for task in all_tasks_event_history:
            # if the name of user-selected task is in tasks, get its task_id
            if tasks.get(task.task_id) == str(name):
                status = str(task.event_name)
                if status not in statusResults:
                    statusResults[status] = []
                # append the id, task_id, ts, y with 0, next_process with null
                # for the status(running/failed/done) of the selected task
                statusResults[status].append(({
                    'id': str(task.id), 'task_id': str(task.task_id),
                    'x': from_utc(str(task.ts)), 'y': 0, 'next_process': ''}))
                # append the id, task_name, task_id, status, datetime, timestamp
                # for the selected task
                taskResults.append({
                    'id': str(task.id), 'taskName': str(name), 'task_id': str(task.task_id),
                    'status': str(task.event_name), 'datetime': str(task.ts),
                    'timestamp': from_utc(str(task.ts))})
        statusResults = json.dumps(statusResults)
        taskResults = json.dumps(taskResults)
        statusResults = tornado.escape.xhtml_unescape(str(statusResults))
        taskResults = tornado.escape.xhtml_unescape(str(taskResults))
        self.render('history.html', name=name, statusResults=statusResults, taskResults=taskResults)


def from_utc(utcTime, fmt="%Y-%m-%d %H:%M:%S.%f"):
    """convert UTC time string to time.struct_time: change datetime.datetime to time, return time.struct_time type"""
    time_struct = datetime.datetime.strptime(utcTime, fmt)
    date = int(time.mktime(time_struct.timetuple()))
    return date


class RecentRunHandler(BaseTaskHistoryHandler):

    def get(self):
        tasks = self._scheduler.task_history.find_latest_runs()
        self.render("recent.html", tasks=tasks)


class ByNameHandler(BaseTaskHistoryHandler):

    def get(self, name):
        tasks = self._scheduler.task_history.find_all_by_name(name)
        self.render("recent.html", tasks=tasks)


class ByIdHandler(BaseTaskHistoryHandler):

    def get(self, id):
        task = self._scheduler.task_history.find_task_by_id(id)
        self.render("show.html", task=task)


class ByParamsHandler(BaseTaskHistoryHandler):

    def get(self, name):
        payload = self.get_argument('data', default="{}")
        arguments = json.loads(payload)
        tasks = self._scheduler.task_history.find_all_by_parameters(name, session=None, **arguments)
        self.render("recent.html", tasks=tasks)


class StaticFileHandler(tornado.web.RequestHandler):

    def get(self, path):
        # Path checking taken from Flask's safe_join function:
        # https://github.com/mitsuhiko/flask/blob/1d55b8983/flask/helpers.py#L563-L587
        path = posixpath.normpath(path)
        if os.path.isabs(path) or path.startswith(".."):
            return self.send_error(404)

        extension = os.path.splitext(path)[1]
        if extension in mimetypes.types_map:
            self.set_header("Content-Type", mimetypes.types_map[extension])
        data = pkg_resources.resource_string(__name__, os.path.join("static", path))
        self.write(data)


class RootPathHandler(tornado.web.RequestHandler):

    def get(self):
        self.redirect("/static/visualiser/index.html")


def app(scheduler):
    settings = {"static_path": os.path.join(os.path.dirname(__file__), "static"), "unescape": tornado.escape.xhtml_unescape}
    handlers = [
        (r'/api/(.*)', RPCHandler, {"scheduler": scheduler}),
        (r'/static/(.*)', StaticFileHandler),
        (r'/', RootPathHandler),
        (r'/tasklist', AllRunHandler, {'scheduler': scheduler}),
        (r'/tasklist/(.*?)', SelectedRunHandler, {'scheduler': scheduler}),
        (r'/history', RecentRunHandler, {'scheduler': scheduler}),
        (r'/history/by_name/(.*?)', ByNameHandler, {'scheduler': scheduler}),
        (r'/history/by_id/(.*?)', ByIdHandler, {'scheduler': scheduler}),
        (r'/history/by_params/(.*?)', ByParamsHandler, {'scheduler': scheduler})
    ]
    api_app = tornado.web.Application(handlers, **settings)
    return api_app


def _init_api(scheduler, responder=None, api_port=None, address=None):
    if responder:
        raise Exception('The "responder" argument is no longer supported')
    api_app = app(scheduler)
    api_sockets = tornado.netutil.bind_sockets(api_port, address=address)
    server = tornado.httpserver.HTTPServer(api_app)
    server.add_sockets(api_sockets)

    # Return the bound socket names.  Useful for connecting client in test scenarios.
    return [s.getsockname() for s in api_sockets]


def run(api_port=8082, address=None, scheduler=None, responder=None):
    """
    Runs one instance of the API server.
    """
    if scheduler is None:
        scheduler = CentralPlannerScheduler()

    # load scheduler state
    scheduler.load()

    _init_api(scheduler, responder, api_port, address)

    # prune work DAG every 60 seconds
    pruner = tornado.ioloop.PeriodicCallback(scheduler.prune, 60000)
    pruner.start()

    def shutdown_handler(signum, frame):
        exit_handler()
        sys.exit(0)

    @atexit.register
    def exit_handler():
        logger.info("Scheduler instance shutting down")
        scheduler.dump()
        stop()

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    if os.name == 'nt':
        signal.signal(signal.SIGBREAK, shutdown_handler)
    else:
        signal.signal(signal.SIGQUIT, shutdown_handler)

    logger.info("Scheduler starting up")

    tornado.ioloop.IOLoop.instance().start()


def stop():
    tornado.ioloop.IOLoop.instance().stop()


if __name__ == "__main__":
    run()
