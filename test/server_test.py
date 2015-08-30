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
import functools
import os
import multiprocessing
import random
import shutil
import signal
import time
import tempfile

from helpers import unittest, with_config, skipOnTravis
import luigi.rpc
import luigi.server
from luigi.scheduler import CentralPlannerScheduler
from luigi.six.moves.urllib.parse import (
    urlencode, ParseResult, quote as urlquote
)

from tornado.testing import AsyncHTTPTestCase
from nose.plugins.attrib import attr

try:
    from unittest import mock
except ImportError:
    import mock


class ServerTestBase(AsyncHTTPTestCase):

    def get_app(self):
        return luigi.server.app(CentralPlannerScheduler())

    def setUp(self):
        super(ServerTestBase, self).setUp()

        self._old_fetch = luigi.rpc.RemoteScheduler._fetch

        def _fetch(obj, url, body, *args, **kwargs):
            body = urlencode(body).encode('utf-8')
            response = self.fetch(url, body=body, method='POST')
            if response.code >= 400:
                raise luigi.rpc.RPCError(
                    'Errror when connecting to remote scheduler'
                )
            return response.body.decode('utf-8')

        luigi.rpc.RemoteScheduler._fetch = _fetch

    def tearDown(self):
        super(ServerTestBase, self).tearDown()
        luigi.rpc.RemoteScheduler._fetch = self._old_fetch


class ServerTest(ServerTestBase):

    def test_visualizer(self):
        page = self.fetch('/').body
        self.assertTrue(page.find(b'<title>') != -1)

    def _test_404(self, path):
        response = self.fetch(path)

        self.assertEqual(response.code, 404)

    def test_404(self):
        self._test_404('/foo')

    def test_api_404(self):
        self._test_404('/api/foo')


class INETServerClient(object):
    def __init__(self):
        self.port = random.randint(1024, 9999)

    def run_server(self):
        luigi.server.run(api_port=self.port, address='127.0.0.1')

    def scheduler(self):
        return luigi.rpc.RemoteScheduler('http://localhost:' + str(self.port))


class UNIXServerClient(object):
    def __init__(self):
        self.tempdir = tempfile.mkdtemp()
        self.unix_socket = os.path.join(self.tempdir, 'luigid.sock')

    def run_server(self):
        luigi.server.run(unix_socket=self.unix_socket)

    def scheduler(self):
        url = ParseResult(
            scheme='http+unix',
            netloc=urlquote(self.unix_socket, safe=''),
            path='',
            params='',
            query='',
            fragment='',
        ).geturl()
        return luigi.rpc.RemoteScheduler(url)


class ServerTestRun(unittest.TestCase):
    """Test to start and stop the server in a more "standard" way
    """

    server_client_class = INETServerClient

    def start_server(self):
        self._process = multiprocessing.Process(
            target=self.server_client.run_server
        )
        self._process.start()
        time.sleep(0.1)  # wait for server to start
        self.sch = self.server_client.scheduler()
        self.sch._wait = lambda: None

    def stop_server(self):
        self._process.terminate()
        self._process.join(1)
        if self._process.is_alive():
            os.kill(self._process.pid, signal.SIGKILL)

    def setUp(self):
        self.server_client = self.server_client_class()
        state_path = tempfile.mktemp(suffix=self.id())
        self.addCleanup(functools.partial(os.unlink, state_path))
        luigi.configuration.get_config().set('scheduler', 'state_path', state_path)
        self.start_server()

    def tearDown(self):
        self.stop_server()

    def test_ping(self):
        self.sch.ping(worker='xyz')

    def test_raw_ping(self):
        self.sch._request('/api/ping', {'worker': 'xyz'})

    def test_raw_ping_extended(self):
        self.sch._request('/api/ping', {'worker': 'xyz', 'foo': 'bar'})

    def test_404(self):
        with self.assertRaises(luigi.rpc.RPCError):
            self.sch._request('/api/fdsfds', {'dummy': 1})

    @skipOnTravis('https://travis-ci.org/spotify/luigi/jobs/72953884')
    def test_save_state(self):
        self.sch.add_task('X', 'B', deps=('A',))
        self.sch.add_task('X', 'A')
        self.assertEqual(self.sch.get_work('X')['task_id'], 'A')
        self.stop_server()
        self.start_server()
        work = self.sch.get_work('X')['running_tasks'][0]
        self.assertEqual(work['task_id'], 'A')


class URLLibServerTestRun(ServerTestRun):

    @mock.patch.object(luigi.rpc, 'HAS_REQUESTS', False)
    def start_server(self, *args, **kwargs):
        super(URLLibServerTestRun, self).start_server(*args, **kwargs)


@attr('unixsocket')
class UNIXServerTestRun(ServerTestRun):
    server_client_class = UNIXServerClient

    def tearDown(self):
        super(UNIXServerTestRun, self).tearDown()
        shutil.rmtree(self.server_client.tempdir)


if __name__ == '__main__':
    unittest.main()
