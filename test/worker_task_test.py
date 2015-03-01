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

import luigi
import luigi.date_interval
import luigi.notifications
from luigi.worker import TaskException

luigi.notifications.DEBUG = True


class MyTask(luigi.Task):
    # Test overriding the constructor without calling the superconstructor
    # This is a simple mistake but caused an error that was very hard to understand

    def __init__(self):
        pass


class WorkerTaskTest(unittest.TestCase):

    def test_constructor(self):
        def f():
            luigi.build([MyTask()], local_scheduler=True)
        self.assertRaises(TaskException, f)

    def test_run_none(self):
        def f():
            luigi.build([None], local_scheduler=True)
        self.assertRaises(TaskException, f)

if __name__ == '__main__':
    unittest.main()
