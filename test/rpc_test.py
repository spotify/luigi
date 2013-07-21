# Copyright (c) 2013 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import unittest

import luigi.rpc

import server_test


class RPCTest(server_test.ServerTestBase):
    def _get_sch(self):
        sch = luigi.rpc.RemoteScheduler(host='localhost', port=self._api_port)
        sch._wait = lambda: None
        return sch

    def test_ping(self):
        sch = self._get_sch()
        sch.ping(worker='xyz')

    def test_raw_ping(self):
        sch = self._get_sch()
        sch._request('/api/ping', {'worker': 'xyz'})

    def test_raw_ping_extended(self):
        sch = self._get_sch()
        sch._request('/api/ping', {'worker': 'xyz', 'foo': 'bar'})


if __name__ == '__main__':
    unittest.main()

