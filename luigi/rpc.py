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

import urllib
import urllib2
import logging
import json
import time
from scheduler import Scheduler, PENDING

logger = logging.getLogger('luigi-interface')  # TODO: 'interface'?


class RPCError(Exception):
    def __init__(self, message, sub_exception=None):
        super(RPCError, self).__init__(message)
        self.sub_exception = sub_exception


class RemoteScheduler(Scheduler):
    ''' Scheduler proxy object. Talks to a RemoteSchedulerResponder '''

    def __init__(self, host='localhost', port=8082):
        self._host = host
        self._port = port
        self._attempts = 3

    def _wait(self):
        time.sleep(30)

    def _request(self, url, data):
        # TODO(erikbern): do POST requests instead
        data = {'data': json.dumps(data)}
        url = 'http://%s:%d%s?%s' % \
            (self._host, self._port, url, urllib.urlencode(data))

        req = urllib2.Request(url)
        last_exception = None
        for attempt in xrange(self._attempts):
            if last_exception:
                logger.info("Retrying...")
                self._wait()  # wait for a bit and retry
            try:
                response = urllib2.urlopen(req)
                break
            except urllib2.URLError, last_exception:
                logger.exception("Failed connecting to remote scheduler %r" % (self._host,))
                continue
        else:
            raise RPCError("Errors (%d attempts) when connecting to remote scheduler %r" % (
                            self._attempts, self._host), last_exception)
        page = response.read()
        result = json.loads(page)
        return result["response"]

    def ping(self, worker):
        self._request('/api/ping', {'worker': worker})  # Keep-alive

    def add_task(self, worker, task_id, status=PENDING, runnable=False, deps=None, expl=None):
        self._request('/api/add_task',
            {'task_id': task_id,
             'worker': worker,
             'status': status,
             'runnable': runnable,
             'deps': deps,
             'expl': expl,
             })

    def get_work(self, worker):
        return self._request('/api/get_work', {'worker': worker})

    def graph(self):
        return self._request('/api/graph', {})

    def dep_graph(self, task_id):
        return self._request('/api/dep_graph', {'task_id': task_id})

    def task_list(self, status, upstream_status):
        return self._request('/api/task_list', {'status': status, 'upstream_status': upstream_status})


class RemoteSchedulerResponder(object):
    """ Use on the server side for responding to requests"""

    def __init__(self, scheduler):
        self._scheduler = scheduler

    def add_task(self, worker, task_id, status, runnable, deps, expl):
        return self._scheduler.add_task(worker, task_id, status, runnable, deps, expl)

    def get_work(self, worker):
        return self._scheduler.get_work(worker)

    def ping(self, worker):
        return self._scheduler.ping(worker)

    def graph(self):
        return self._scheduler.graph()

    index = graph

    def dep_graph(self, task_id):
        return self._scheduler.dep_graph(task_id)

    def task_list(self, status, upstream_status):
        return self._scheduler.task_list(status, upstream_status)
