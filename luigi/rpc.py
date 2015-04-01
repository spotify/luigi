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
Implementation of the REST interface between the workers and the server.
rpc.py implements the client side of it, server.py implements the server side.
See :doc:`/central_scheduler` for more info.
"""

import json
import logging
import time

from luigi.six.moves.urllib.parse import urlencode
from luigi.six.moves.urllib.request import Request, urlopen
from luigi.six.moves.urllib.error import URLError

from luigi import configuration
from luigi.scheduler import PENDING, Scheduler

logger = logging.getLogger('luigi-interface')  # TODO: 'interface'?


class RPCError(Exception):

    def __init__(self, message, sub_exception=None):
        super(RPCError, self).__init__(message)
        self.sub_exception = sub_exception


class RemoteScheduler(Scheduler):
    """
    Scheduler proxy object. Talks to a RemoteSchedulerResponder.
    """

    def __init__(self, host='localhost', port=8082, connect_timeout=None, url_prefix=''):
        self._host = host
        self._port = port
        self._url_prefix = url_prefix

        config = configuration.get_config()

        if connect_timeout is None:
            connect_timeout = config.getfloat('core', 'rpc-connect-timeout', 10.0)
        self._connect_timeout = connect_timeout

    def _wait(self):
        time.sleep(30)

    def _fetch(self, url, body, log_exceptions=True, attempts=3):

        full_url = 'http://{host}:{port:d}{prefix}{url}'.format(
            host=self._host,
            port=self._port,
            prefix=self._url_prefix,
            url=url)
        last_exception = None
        attempt = 0
        while attempt < attempts:
            attempt += 1
            if last_exception:
                logger.info("Retrying...")
                self._wait()  # wait for a bit and retry
            try:
                response = urlopen(full_url, body, self._connect_timeout)
                break
            except URLError as e:
                last_exception = e
                if log_exceptions:
                    logger.exception("Failed connecting to remote scheduler %r", self._host)
                continue
        else:
            raise RPCError(
                "Errors (%d attempts) when connecting to remote scheduler %r" %
                (attempts, self._host),
                last_exception
            )
        return response.read().decode('utf-8')

    def _request(self, url, data, log_exceptions=True, attempts=3):
        data = {'data': json.dumps(data)}
        body = urlencode(data).encode('utf-8')

        page = self._fetch(url, body, log_exceptions, attempts)
        result = json.loads(page)
        return result["response"]

    def ping(self, worker):
        # just one attemtps, keep-alive thread will keep trying anyway
        self._request('/api/ping', {'worker': worker}, attempts=1)

    def add_task(self, worker, task_id, status=PENDING, runnable=True,
                 deps=None, new_deps=None, expl=None, resources=None, priority=0,
                 family='', module=None, params=None, assistant=False):
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
            'module': module,
            'params': params,
            'assistant': assistant,
        })

    def get_work(self, worker, host=None, assistant=False):
        return self._request(
            '/api/get_work',
            {'worker': worker, 'host': host, 'assistant': assistant},
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

    def update_resources(self, **resources):
        return self._request('/api/update_resources', resources)

    def prune(self):
        return self._request('/api/prune', {})

    def re_enable_task(self, task_id):
        return self._request('/api/re_enable_task', {'task_id': task_id})
