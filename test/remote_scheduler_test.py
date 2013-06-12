# Copyright (c) 2012 Spotify AB
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

import os
import tempfile
import unittest

import luigi.server

tempdir = tempfile.mkdtemp()

class DummyTask(luigi.Task):
    id = luigi.Parameter()

    def run(self):
        f = self.output().open('w')
        f.close()

    def output(self):
        return luigi.LocalTarget(os.path.join(tempdir, str(self.id)))


class RemoteSchedulerTest(unittest.TestCase):
    def setUp(self):
        # Pass IPv4 localhost to ensure that only a single address, and therefore single port, is bound
        sock_names = luigi.server.run_api_threaded(0, address='127.0.0.1')
        _, self._api_port = sock_names[0]

    def tearDown(self):
        luigi.server.stop()

    def _test_run(self, workers):
        tasks = [DummyTask(id) for id in xrange(20)]
        luigi.build(tasks, scheduler_host='localhost', scheduler_port=self._api_port, workers=workers)

        for t in tasks:
            self.assertEqual(t.complete(), True)

    def test_single_worker(self):
        self._test_run(workers=1)

    def test_multiple_workers(self):
        self._test_run(workers=10)


if __name__ == '__main__':
    unittest.main()

