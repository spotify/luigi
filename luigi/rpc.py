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
import warnings
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

    def _wait(self):
        time.sleep(30)

    def _request(self, url, data, log_exceptions=True, attempts=3):
        # TODO(erikbern): do POST requests instead
        data = {'data': json.dumps(data)}
        url = 'http://%s:%d%s?%s' % \
            (self._host, self._port, url, urllib.urlencode(data))

        req = urllib2.Request(url)
        last_exception = None
        for attempt in xrange(attempts):
            if last_exception:
                logger.info("Retrying...")
                self._wait()  # wait for a bit and retry
            try:
                response = urllib2.urlopen(req)
                break
            except urllib2.URLError, last_exception:
                if log_exceptions:
                    logger.exception("Failed connecting to remote scheduler %r", self._host)
                continue
        else:
            raise RPCError(
                "Errors (%d attempts) when connecting to remote scheduler %r" %
                (attempts, self._host),
                last_exception
            )
        page = response.read()
        result = json.loads(page)
        return result["response"]

    def ping(self, worker):
        # just one attemtps, keep-alive thread will keep trying anyway
        self._request('/api/ping', {'worker': worker}, attempts=1)

    def add_task(self, worker, task_id, status=PENDING, runnable=False, deps=None, expl=None):
        self._request('/api/add_task', {
            'task_id': task_id,
            'worker': worker,
            'status': status,
            'runnable': runnable,
            'deps': deps,
            'expl': expl,
        })

    def get_work(self, worker, host=None):
        ''' Ugly work around for an older scheduler version, where get_work doesn't have a host argument. Try once passing
            host to it, falling back to the old version. Should be removed once people have had time to update everything
        '''
        try:
            return self._request(
                '/api/get_work',
                {'worker': worker, 'host': host},
                log_exceptions=False,
                attempts=1
            )
        except:
            logger.info("get_work RPC call failed, is it possible that you need to update your scheduler?")
            raise

    def graph(self):
        return self._request('/api/graph', {})

    def dep_graph(self, task_id):
        return self._request('/api/dep_graph', {'task_id': task_id})

    def task_list(self, status, upstream_status):
        return self._request('/api/task_list', {'status': status, 'upstream_status': upstream_status})

    def fetch_error(self, task_id):
        return self._request('/api/fetch_error', {'task_id': task_id})


class RemoteSchedulerResponder(object):
    """ Use on the server side for responding to requests
    
    The kwargs are there for forwards compatibility in case workers add
    new (optional) arguments. That way there's no dependency on the server
    component when upgrading Luigi on the worker side.

    TODO(erikbern): what is this class actually used for? Other than an
    unnecessary layer of indirection around central scheduler
    """

    def __init__(self, scheduler):
        self._scheduler = scheduler

    def add_task(self, worker, task_id, status, runnable, deps, expl, **kwargs):
        return self._scheduler.add_task(worker, task_id, status, runnable, deps, expl)

    def get_work(self, worker, host=None, **kwargs):
        return self._scheduler.get_work(worker, host)

    def ping(self, worker, **kwargs):
        return self._scheduler.ping(worker)

    def graph(self, **kwargs):
        return self._scheduler.graph()

    index = graph

    def dep_graph(self, task_id, **kwargs):
        return self._scheduler.dep_graph(task_id)

    def task_list(self, status, upstream_status, **kwargs):
        return self._scheduler.task_list(status, upstream_status)

    def fetch_error(self, task_id, **kwargs):
        return self._scheduler.fetch_error(task_id)

    @property
    def task_history(self):
        return self._scheduler.task_history
