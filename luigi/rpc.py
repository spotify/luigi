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

import json
import logging
import time
import urllib
import urllib2

import configuration
from scheduler import PENDING, Scheduler

logger = logging.getLogger('luigi-interface')  # TODO: 'interface'?


class RPCError(Exception):

    def __init__(self, message, sub_exception=None):
        super(RPCError, self).__init__(message)
        self.sub_exception = sub_exception


class RemoteScheduler(Scheduler):
    """
    Scheduler proxy object. Talks to a RemoteSchedulerResponder.
    """

    def __init__(self, host='localhost', port=8082, connect_timeout=None):
        self._host = host
        self._port = port

        config = configuration.get_config()

        if connect_timeout is None:
            connect_timeout = config.getfloat('core', 'rpc-connect-timeout', 10.0)
        self._connect_timeout = connect_timeout

    def _wait(self):
        time.sleep(30)

    def _get(self, url, data):
        url = 'http://%s:%d%s?%s' % \
              (self._host, self._port, url, urllib.urlencode(data))
        return urllib2.Request(url)

    def _post(self, url, data):
        url = 'http://%s:%d%s' % (self._host, self._port, url)
        return urllib2.Request(url, urllib.urlencode(data))

    def _request(self, url, data, log_exceptions=True, attempts=3):
        data = {'data': json.dumps(data)}

        req = self._post(url, data)
        last_exception = None
        attempt = 0
        while attempt < attempts:
            attempt += 1
            if last_exception:
                logger.info("Retrying...")
                self._wait()  # wait for a bit and retry
            try:
                response = urllib2.urlopen(req, None, self._connect_timeout)
                break
            except urllib2.URLError as last_exception:
                if isinstance(last_exception, urllib2.HTTPError) and last_exception.code == 405:
                    # TODO(f355): 2014-08-29 Remove this fallback after several weeks
                    logger.warning("POST requests are unsupported. Please upgrade scheduler ASAP. Falling back to GET for now.")
                    req = self._get(url, data)
                    last_exception = None
                    attempt -= 1
                elif log_exceptions:
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

    def add_task(self, worker, task_id, status=PENDING, runnable=False,
                 deps=None, new_deps=None, expl=None, resources={}, priority=0,
                 family='', params={}):
        self._request('/api/add_task', {
            'task_id': task_id,
            'worker': worker,
            'status': status,
            'runnable': runnable,
            'deps': deps,
            'new_deps': new_deps,
            'expl': expl,
            'resources': resources,
            'priority': priority,
            'family': family,
            'params': params,
        })

    def get_work(self, worker, host=None):
        return self._request(
            '/api/get_work',
            {'worker': worker, 'host': host},
            log_exceptions=False,
            attempts=1)

    def graph(self):
        return self._request('/api/graph', {})

    def dep_graph(self, task_id):
        return self._request('/api/dep_graph', {'task_id': task_id})

    def inverse_dep_graph(self, task_id):
        return self._request('/api/inverse_dep_graph', {'task_id': task_id})

    def task_list(self, status, upstream_status):
        return self._request('/api/task_list', {'status': status, 'upstream_status': upstream_status})

    def worker_list(self):
        return self._request('/api/worker_list', {})

    def task_search(self, task_str):
        return self._request('/api/task_search', {'task_str': task_str})

    def fetch_error(self, task_id):
        return self._request('/api/fetch_error', {'task_id': task_id})

    def add_worker(self, worker, info):
        return self._request('/api/add_worker', {'worker': worker, 'info': info})
