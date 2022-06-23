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
import abc
import os
import json
import logging
import socket
import base64

from urllib.parse import urljoin, urlencode, urlparse
from urllib.request import urlopen, Request
from urllib.error import URLError

from tenacity import Retrying, wait_fixed, stop_after_attempt
from luigi import configuration
from luigi.scheduler import RPC_METHODS

HAS_UNIX_SOCKET = True
HAS_REQUESTS = True


try:
    import requests_unixsocket as requests
except ImportError:
    HAS_UNIX_SOCKET = False
    try:
        import requests
    except ImportError:
        HAS_REQUESTS = False


logger = logging.getLogger('luigi-interface')  # TODO: 'interface'?


def _urljoin(base, url):
    """
    Join relative URLs to base URLs like urllib.parse.urljoin but support
    arbitrary URIs (esp. 'http+unix://').
    """
    parsed = urlparse(base)
    scheme = parsed.scheme
    return urlparse(
        urljoin(parsed._replace(scheme='http').geturl(), url)
    )._replace(scheme=scheme).geturl()


class RPCError(Exception):

    def __init__(self, message, sub_exception=None):
        super(RPCError, self).__init__(message)
        self.sub_exception = sub_exception


class _FetcherInterface(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def fetch(self, full_url, body, timeout):
        pass

    @abc.abstractmethod
    def close(self):
        pass


class URLLibFetcher(_FetcherInterface):
    raises = (URLError, socket.timeout)

    def _create_request(self, full_url, body=None):
        # when full_url contains basic auth info, extract it and set the Authorization header
        url = urlparse(full_url)
        if url.username:
            # base64 encoding of username:password
            auth = base64.b64encode('{}:{}'.format(url.username, url.password or '').encode('utf-8'))
            auth = auth.decode('utf-8')
            # update full_url and create a request object with the auth header set
            full_url = url._replace(netloc=url.netloc.split('@', 1)[-1]).geturl()
            req = Request(full_url)
            req.add_header('Authorization', 'Basic {}'.format(auth))
        else:
            req = Request(full_url)

        # add the request body
        if body:
            req.data = urlencode(body).encode('utf-8')

        return req

    def fetch(self, full_url, body, timeout):
        req = self._create_request(full_url, body=body)
        return urlopen(req, timeout=timeout).read().decode('utf-8')

    def close(self):
        pass


class RequestsFetcher(_FetcherInterface):
    def __init__(self):
        from requests import exceptions as requests_exceptions
        self.raises = requests_exceptions.RequestException
        self.session = requests.Session()
        self.process_id = os.getpid()

    def check_pid(self):
        # if the process id change changed from when the session was created
        # a new session needs to be setup since requests isn't multiprocessing safe.
        if os.getpid() != self.process_id:
            self.session = requests.Session()
            self.process_id = os.getpid()

    def fetch(self, full_url, body, timeout):
        self.check_pid()
        resp = self.session.post(full_url, data=body, timeout=timeout)
        resp.raise_for_status()
        return resp.text

    def close(self):
        self.session.close()


class RemoteScheduler:
    """
    Scheduler proxy object. Talks to a RemoteSchedulerResponder.
    """

    def __init__(self, url='http://localhost:8082/', connect_timeout=None):
        assert not url.startswith('http+unix://') or HAS_UNIX_SOCKET, (
            'You need to install requests-unixsocket for Unix socket support.'
        )

        self._url = url.rstrip('/')
        config = configuration.get_config()

        if connect_timeout is None:
            connect_timeout = config.getfloat('core', 'rpc-connect-timeout', 10.0)
        self._connect_timeout = connect_timeout

        self._rpc_retry_attempts = config.getint('core', 'rpc-retry-attempts', 3)
        self._rpc_retry_wait = config.getint('core', 'rpc-retry-wait', 30)
        self._rpc_log_retries = config.getboolean('core', 'rpc-log-retries', True)

        if HAS_REQUESTS:
            self._fetcher = RequestsFetcher()
        else:
            self._fetcher = URLLibFetcher()

    def close(self):
        self._fetcher.close()

    def _get_retryer(self):
        def retry_logging(retry_state):
            if self._rpc_log_retries:
                logger.warning("Failed connecting to remote scheduler %r", self._url, exc_info=True)
                logger.info("Retrying attempt %r of %r (max)" % (retry_state.attempt_number + 1, self._rpc_retry_attempts))
                logger.info("Wait for %d seconds" % self._rpc_retry_wait)

        return Retrying(wait=wait_fixed(self._rpc_retry_wait),
                        stop=stop_after_attempt(self._rpc_retry_attempts),
                        reraise=True,
                        after=retry_logging)

    def _fetch(self, url_suffix, body):
        full_url = _urljoin(self._url, url_suffix)
        scheduler_retry = self._get_retryer()

        try:
            response = scheduler_retry(self._fetcher.fetch, full_url, body, self._connect_timeout)
        except self._fetcher.raises as e:
            raise RPCError(
                "Errors (%d attempts) when connecting to remote scheduler %r" %
                (self._rpc_retry_attempts, self._url),
                e
            )
        return response

    def _request(self, url, data, attempts=3, allow_null=True):
        body = {'data': json.dumps(data)}

        for _ in range(attempts):
            page = self._fetch(url, body)
            response = json.loads(page)["response"]
            if allow_null or response is not None:
                return response
        raise RPCError("Received null response from remote scheduler %r" % self._url)


for method_name, method in RPC_METHODS.items():
    setattr(RemoteScheduler, method_name, method)
