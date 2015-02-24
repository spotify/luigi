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

import multiprocessing
import random
import time
import unittest
try:
    from urllib2 import Request, urlopen, HTTPError
except ImportError:
    from urllib.request import Request, urlopen
    from urllib.error import HTTPError

import luigi.server


def run_server(api_port):
    luigi.server.run(api_port=api_port, address='127.0.0.1')


class ServerTestBase(unittest.TestCase):

    def setUp(self):
        self._api_port = random.randint(1024, 9999)
        self._process = multiprocessing.Process(target=run_server, args=(self._api_port,))
        self._process.start()
        time.sleep(0.1)  # wait for server to start

    def tearDown(self):
        self._process.terminate()
        self._process.join()


class ServerTest(ServerTestBase):

    def test_visualizer(self):
        uri = 'http://localhost:%d' % self._api_port
        req = Request(uri)
        response = urlopen(req, timeout=10)
        page = response.read().decode('utf8')
        self.assertTrue(page.find('<title>') != -1)

    def _test_404(self, path):
        uri = 'http://localhost:%d%s' % (self._api_port, path)
        req = Request(uri)
        try:
            _ = urlopen(req, timeout=10)
        except HTTPError as e:
            http_exc = e

        self.assertEqual(http_exc.code, 404)

    def test_404(self):
        self._test_404('/foo')

    def test_api_404(self):
        self._test_404('/api/foo')


if __name__ == '__main__':
    unittest.main()
