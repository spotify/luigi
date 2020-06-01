# -*- coding: utf-8 -*-
#
# Copyright 2012-2017 Spotify AB
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
import time
import threading
import tempfile
import shutil
import contextlib

import luigi
from luigi.scheduler import Scheduler
from luigi.worker import Worker

from helpers import LuigiTestCase


class WorkerSchedulerCommunicationTest(LuigiTestCase):
    """
    Tests related to communication between Worker and Scheduler that is based on the ping polling.

    See https://github.com/spotify/luigi/pull/1993
    """

    def run(self, result=None):
        self.sch = Scheduler()

        with Worker(scheduler=self.sch, worker_id='X', ping_interval=1, max_reschedules=0) as w:
            self.w = w

            # also save scheduler's worker struct
            self.sw = self.sch._state.get_worker(self.w._id)

            super(WorkerSchedulerCommunicationTest, self).run(result)

    def wrapper_task(test_self):
        tmp = tempfile.mkdtemp()

        class MyTask(luigi.Task):

            n = luigi.IntParameter()
            delay = 3

            def output(self):
                basename = "%s_%s.txt" % (self.__class__.__name__, self.n)
                return luigi.LocalTarget(os.path.join(tmp, basename))

            def run(self):
                time.sleep(self.delay)
                with self.output().open('w') as f:
                    f.write("content\n")

        class Wrapper(MyTask):

            delay = 0

            def requires(self):
                return [MyTask(n=n) for n in range(self.n)]

        return Wrapper, tmp

    def test_message_handling(self):
        # add some messages for that worker
        for i in range(10):
            self.sw.add_rpc_message('foo', i=i)
        self.assertEqual(10, len(self.sw.rpc_messages))
        self.assertEqual(9, self.sw.rpc_messages[-1]['kwargs']['i'])

        # fetch
        msgs = self.sw.fetch_rpc_messages()
        self.assertEqual(0, len(self.sw.rpc_messages))
        self.assertEqual(9, msgs[-1]['kwargs']['i'])

    def test_ping_content(self):
        # add some messages for that worker
        for i in range(10):
            self.sw.add_rpc_message('foo', i=i)

        # ping the scheduler and check the result
        res = self.sch.ping(worker=self.w._id)
        self.assertIn('rpc_messages', res)
        msgs = res['rpc_messages']
        self.assertEqual(10, len(msgs))
        self.assertEqual('foo', msgs[-1]['name'])
        self.assertEqual(9, msgs[-1]['kwargs']['i'])

        # there should be no message left
        self.assertEqual(0, len(self.sw.rpc_messages))

    @contextlib.contextmanager
    def run_wrapper(self, n):
        # assign the wrapper task to the worker
        Wrapper, tmp = self.wrapper_task()
        wrapper = Wrapper(n=n)
        self.assertTrue(self.w.add(wrapper))

        # check the initial number of worker processes
        self.assertEqual(1, self.w.worker_processes)

        # run the task in a thread and while running, increase the number of worker processes
        # via an rpc message
        t = threading.Thread(target=self.w.run)
        t.start()

        # yield
        yield wrapper, t

        # finally, check that thread is done
        self.assertFalse(t.is_alive())

        # cleanup the tmp dir
        shutil.rmtree(tmp)

    def test_dispatch_valid_message(self):
        with self.run_wrapper(3) as (wrapper, t):
            # each of the wrapper task's tasks runs 3 seconds, and the ping/message dispatch
            # interval is 1 second, so it should be safe to wait 1 second here, add the message
            # which is then fetched by the keep alive thread and dispatched, so after additional 3
            # seconds, the worker will have a changed number of processes
            t.join(1)
            self.sch.set_worker_processes(self.w._id, 2)

            t.join(3)
            self.assertEqual(2, self.w.worker_processes)

            # after additional 3 seconds, the wrapper task + all required tasks should be completed
            t.join(3)
            self.assertTrue(all(task.complete() for task in wrapper.requires()))
            self.assertTrue(wrapper.complete())

    def test_dispatch_invalid_message(self):
        # this test is identical to test_dispatch_valid_message, except that the number of processes
        # is not increased during running as we send an invalid rpc message
        # in addition, the wrapper will only have two requirements
        with self.run_wrapper(2) as (wrapper, t):
            # timing info as above
            t.join(1)
            self.sw.add_rpc_message('set_worker_processes_not_there', n=2)

            t.join(3)
            self.assertEqual(1, self.w.worker_processes)

            # after additional 3 seconds, the wrapper task and all required tasks should be completed
            t.join(3)
            self.assertTrue(all(task.complete() for task in wrapper.requires()))
            self.assertTrue(wrapper.complete())

    def test_dispatch_unregistered_message(self):
        # this test is identical to test_dispatch_valid_message, except that the number of processes
        # is not increased during running as we disable the particular callback to work as a
        # callback, so we want to achieve sth like
        # self.w.set_worker_processes.is_rpc_message_callback = False
        # but this is not possible in py 2 due to wrapped method lookup, see
        # http://stackoverflow.com/questions/9523370/adding-attributes-to-instance-methods-in-python
        set_worker_processes_orig = self.w.set_worker_processes

        def set_worker_processes_replacement(*args, **kwargs):
            return set_worker_processes_orig(*args, **kwargs)

        self.w.set_worker_processes = set_worker_processes_replacement
        self.assertFalse(getattr(self.w.set_worker_processes, "is_rpc_message_callback", False))

        with self.run_wrapper(2) as (wrapper, t):
            # timing info as above
            t.join(1)
            self.sw.add_rpc_message('set_worker_processes', n=2)

            t.join(3)
            self.assertEqual(1, self.w.worker_processes)

            # after additional 3 seconds, the wrapper task and all required tasks should be completed
            t.join(3)
            self.assertTrue(all(task.complete() for task in wrapper.requires()))
            self.assertTrue(wrapper.complete())
