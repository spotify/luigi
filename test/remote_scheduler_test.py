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
import tempfile

import luigi.server
import server_test

tempdir = tempfile.mkdtemp()


class DummyTask(luigi.Task):
    id = luigi.IntParameter()

    def run(self):
        f = self.output().open('w')
        f.close()

    def output(self):
        return luigi.LocalTarget(os.path.join(tempdir, str(self.id)))


class RemoteSchedulerTest(server_test.ServerTestBase):

    def _test_run(self, workers):
        tasks = [DummyTask(id) for id in range(20)]
        luigi.build(tasks, workers=workers, scheduler_port=self.get_http_port())

        for t in tasks:
            self.assertEqual(t.complete(), True)
            self.assertTrue(os.path.exists(t.output().path))

    def test_single_worker(self):
        self._test_run(workers=1)

    def test_multiple_workers(self):
        self._test_run(workers=10)
