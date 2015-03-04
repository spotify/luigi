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

import time
from helpers import unittest

import luigi.rpc
from luigi.scheduler import CentralPlannerScheduler
from central_planner_test import CentralPlannerTest
import random
import multiprocessing
import luigi.server


def run_server(api_port):
    sch = CentralPlannerScheduler(
        retry_delay=100,
        remove_delay=1000,
        worker_disconnect_delay=10,
        disable_persist=10,
        disable_window=10,
        disable_failures=3
    )
    luigi.server.run(api_port=api_port, address='127.0.0.1', scheduler=sch, load=False)


class RPCTest(CentralPlannerTest):

    def setUp(self):
        self.time = time.time
        self._api_port = random.randint(1024, 9999)
        self._process = multiprocessing.Process(target=run_server, args=(self._api_port,))
        self._process.start()
        time.sleep(0.1)  # wait for server to start
        self.sch = luigi.rpc.RemoteScheduler(host='localhost', port=self._api_port)
        self.sch._wait = lambda: None

    def tearDown(self):
        self._process.terminate()
        self._process.join()

    def setTime(self, t):
        raise unittest.SkipTest('Not able to set time of remote process')

    def test_ping(self):
        self.sch.ping(worker='xyz')

    def test_raw_ping(self):
        self.sch._request('/api/ping', {'worker': 'xyz'})

    def test_raw_ping_extended(self):
        self.sch._request('/api/ping', {'worker': 'xyz', 'foo': 'bar'})

    # FIXME: Those are test that are currently failing
    def test_multiple_resources_lock(self):
        raise unittest.SkipTest('Not working!!!')

    def test_lock_resources_while_running_lower_priority(self):
        raise unittest.SkipTest('Not working!!!')

    def test_lock_resources_when_one_of_multiple_workers_is_ready(self):
        raise unittest.SkipTest('Not working!!!')

    def test_lock_resources_for_second_worker(self):
        raise unittest.SkipTest('Not working!!!')

    def test_do_not_lock_resources_while_running_higher_priority(self):
        raise unittest.SkipTest('Not working!!!')

    def test_broken_dep(self):
        raise unittest.SkipTest('Not working!!!')


if __name__ == '__main__':
    unittest.main()
