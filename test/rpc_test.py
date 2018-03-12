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
from helpers import unittest
try:
    from unittest import mock
except ImportError:
    import mock

import luigi.rpc
from luigi.scheduler import Scheduler
import scheduler_api_test
import luigi.server
from server_test import ServerTestBase
import time
import socket
from multiprocessing import Process, Queue
import requests


class RemoteSchedulerTest(unittest.TestCase):
    def testUrlArgumentVariations(self):
        for url in ['http://zorg.com', 'http://zorg.com/']:
            for suffix in ['api/123', '/api/123']:
                s = luigi.rpc.RemoteScheduler(url, 42)
                with mock.patch.object(s, '_fetcher') as fetcher:
                    s._fetch(suffix, '{}')
                    fetcher.fetch.assert_called_once_with('http://zorg.com/api/123', '{}', 42)

    def get_work(self, fetcher_side_effect):
        class ShorterWaitRemoteScheduler(luigi.rpc.RemoteScheduler):
            """
            A RemoteScheduler which waits shorter than usual before retrying (to speed up tests).
            """

            def _wait(self):
                time.sleep(1)

        scheduler = ShorterWaitRemoteScheduler('http://zorg.com', 42)

        with mock.patch.object(scheduler, '_fetcher') as fetcher:
            fetcher.raises = socket.timeout
            fetcher.fetch.side_effect = fetcher_side_effect
            return scheduler.get_work("fake_worker")

    def test_retry_rpc_method(self):
        """
        Tests that a call to a RPC method is re-tried 3 times.
        """

        fetch_results = [socket.timeout, socket.timeout, '{"response":{}}']
        self.assertEqual({}, self.get_work(fetch_results))

    def test_retry_rpc_limited(self):
        """
        Tests that a call to an RPC method fails after the third attempt
        """

        fetch_results = [socket.timeout, socket.timeout, socket.timeout]
        self.assertRaises(luigi.rpc.RPCError, self.get_work, fetch_results)

    def test_get_work_retries_on_null(self):
        """
        Tests that get_work will retry if the response is null
        """

        fetch_results = ['{"response": null}', '{"response": {"pass": true}}']
        self.assertEqual({'pass': True}, self.get_work(fetch_results))

    def test_get_work_retries_on_null_limited(self):
        """
        Tests that get_work will give up after the third null response
        """

        fetch_results = ['{"response": null}'] * 3 + ['{"response": {}}']
        self.assertRaises(luigi.rpc.RPCError, self.get_work, fetch_results)


class RPCTest(scheduler_api_test.SchedulerApiTest, ServerTestBase):

    def get_app(self):
        conf = self.get_scheduler_config()
        sch = Scheduler(**conf)
        return luigi.server.app(sch)

    def setUp(self):
        super(RPCTest, self).setUp()
        self.sch = luigi.rpc.RemoteScheduler(self.get_url(''))
        self.sch._wait = lambda: None

    # disable test that doesn't work with remote scheduler

    def test_task_first_failure_time(self):
        pass

    def test_task_first_failure_time_remains_constant(self):
        pass

    def test_task_has_excessive_failures(self):
        pass

    def test_quadratic_behavior(self):
        """ This would be too slow to run through network """
        pass

    def test_get_work_speed(self):
        """ This would be too slow to run through network """
        pass


class RequestsFetcherTest(ServerTestBase):
    def test_fork_changes_session(self):
        session = requests.Session()
        fetcher = luigi.rpc.RequestsFetcher(session)

        q = Queue()

        def check_session(q):
            fetcher.check_pid()
            # make sure that check_pid has changed out the session
            q.put(fetcher.session != session)

        p = Process(target=check_session, args=(q,))
        p.start()
        p.join()

        self.assertTrue(q.get(), 'the requests.Session should have changed in the new process')
