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

import mock
import random
import time
import threading
import unittest
import urllib2

import luigi.server


class ServerTestBase(unittest.TestCase):

    def run_server(self):
        # Pass IPv4 localhost to ensure that only a single address, and therefore single port, is bound
        luigi.server.run(api_port=self._api_port, address='127.0.0.1')

    def setUp(self):
        self._api_port = random.randint(1000, 9999)

        @mock.patch('signal.signal')
        def scheduler_thread(signal):
            # this is wrapped in a function so we get the instance
            # from the scheduler thread and not from the main thread

            self.run_server()

        self._thread = threading.Thread(target=scheduler_thread)
        self._thread.start()
        time.sleep(0.1)  # wait for server to start

    def tearDown(self):
        luigi.server.stop()
        self._thread.join()


class ServerTest(ServerTestBase):

    def test_visualizer(self):
        uri = 'http://localhost:%d' % self._api_port
        req = urllib2.Request(uri)
        response = urllib2.urlopen(req, timeout=10)
        page = response.read()
        self.assertTrue(page.find('<title>') != -1)

    def _test_404(self, path):
        uri = 'http://localhost:%d%s' % (self._api_port, path)
        req = urllib2.Request(uri)
        try:
            response = urllib2.urlopen(req, timeout=10)
        except urllib2.HTTPError as http_exc:
            pass

        self.assertEqual(http_exc.code, 404)

    def test_404(self):
        self._test_404('/foo')

    def test_api_404(self):
        self._test_404('/api/foo')


if __name__ == '__main__':
    unittest.main()
