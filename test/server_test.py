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
import os
import multiprocessing
import random
import signal
import time

from helpers import unittest, with_config
import luigi.rpc
import luigi.server
from luigi.scheduler import CentralPlannerScheduler

from tornado.testing import AsyncHTTPTestCase


class ServerTestBase(AsyncHTTPTestCase):

    def get_app(self):
        return luigi.server.app(CentralPlannerScheduler())

    def setUp(self):
        super(ServerTestBase, self).setUp()

        self._old_fetch = luigi.rpc.RemoteScheduler._fetch

        def _fetch(obj, url, body, *args, **kwargs):
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


class ServerTestRun(unittest.TestCase):
    """Test to start and stop the server in a more "standard" way
    """

    def remove_state(self):
        if os.path.exists('/tmp/luigi-test-server-state'):
            os.remove('/tmp/luigi-test-server-state')

    @with_config({'scheduler': {'state_path': '/tmp/luigi-test-server-state'}})
    def run_server(self):
        luigi.server.run(api_port=self._api_port, address='127.0.0.1')

    def start_server(self):
        self._api_port = random.randint(1024, 9999)
        self._process = multiprocessing.Process(target=self.run_server)
        self._process.start()
        time.sleep(0.1)  # wait for server to start
        self.sch = luigi.rpc.RemoteScheduler(host='localhost', port=self._api_port)
        self.sch._wait = lambda: None

    def stop_server(self):
        self._process.terminate()
        self._process.join(1)
        if self._process.is_alive():
            os.kill(self._process.pid, signal.SIGKILL)

    def setUp(self):
        self.remove_state()
        self.start_server()

    def tearDown(self):
        self.remove_state()
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

    def test_save_state(self):
        self.sch.add_task('X', 'B', deps=('A',))
        self.sch.add_task('X', 'A')
        self.assertEqual(self.sch.get_work('X')['task_id'], 'A')
        self.stop_server()
        self.start_server()
        work = self.sch.get_work('X')['running_tasks'][0]
        self.assertEqual(work['task_id'], 'A')


if __name__ == '__main__':
    unittest.main()
