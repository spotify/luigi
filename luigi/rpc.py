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
import socket
import time

from luigi.six.moves.urllib.parse import urljoin, urlencode, urlparse
from luigi.six.moves.urllib.request import urlopen
from luigi.six.moves.urllib.error import URLError

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


class URLLibFetcher(object):
    raises = (URLError, socket.timeout)

    def fetch(self, full_url, body, timeout):
        body = urlencode(body).encode('utf-8')
        return urlopen(full_url, body, timeout).read().decode('utf-8')


class RequestsFetcher(object):
    def __init__(self, session):
        from requests import exceptions as requests_exceptions
        self.raises = requests_exceptions.RequestException
        self.session = session

    def fetch(self, full_url, body, timeout):
        resp = self.session.get(full_url, data=body, timeout=timeout)
        resp.raise_for_status()
        return resp.text


class RemoteScheduler(object):
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

        if HAS_REQUESTS:
            self._fetcher = RequestsFetcher(requests.Session())
        else:
            self._fetcher = URLLibFetcher()

    def _wait(self):
        logger.info("Wait for %d seconds" % self._rpc_retry_wait)
        time.sleep(self._rpc_retry_wait)

    def _fetch(self, url_suffix, body, log_exceptions=True):
        full_url = _urljoin(self._url, url_suffix)
        last_exception = None
        attempt = 0
        while attempt < self._rpc_retry_attempts:
            attempt += 1
            if last_exception:
                logger.info("Retrying attempt %r of %r (max)" % (attempt, self._rpc_retry_attempts))
                self._wait()  # wait for a bit and retry
            try:
                response = self._fetcher.fetch(full_url, body, self._connect_timeout)
                break
            except self._fetcher.raises as e:
                last_exception = e
                if log_exceptions:
                    logger.exception("Failed connecting to remote scheduler %r", self._url)
                continue
        else:
            raise RPCError(
                "Errors (%d attempts) when connecting to remote scheduler %r" %
                (self._rpc_retry_attempts, self._url),
                last_exception
            )
        return response

    def _request(self, url, data, log_exceptions=True, attempts=3, allow_null=True):
        body = {'data': json.dumps(data)}

        for _ in range(attempts):
            page = self._fetch(url, body, log_exceptions)
            response = json.loads(page)["response"]
            if allow_null or response is not None:
                return response
        raise RPCError("Received null response from remote scheduler %r" % self._url)


for method_name, method in RPC_METHODS.items():
    setattr(RemoteScheduler, method_name, method)
