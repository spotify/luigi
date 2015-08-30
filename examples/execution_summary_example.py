# -*- coding: utf-8 -*-
#
# Copyright 2015-2015 Spotify AB
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
"""
You can run this example like this:

    .. code:: console

            $ luigi --module examples.execution_summary_example examples.EntryPoint --local-scheduler
            ...
            ... lots of spammy output
            ...
            INFO: There are 11 pending tasks unique to this worker
            INFO: Worker Worker(salt=843361665, workers=1, host=arash-spotify-T440s, username=arash, pid=18534) was stopped. Shutting down Keep-Alive thread
            INFO:
            ===== Luigi Execution Summary =====

            Scheduled 218 tasks of which:
            * 195 present dependencies were encountered:
                - 195 examples.Bar(num=5...199)
            * 1 ran successfully:
                - 1 examples.Boom(...)
            * 22 were left pending, among these:
                * 1 were missing external dependencies:
                    - 1 MyExternal()
                * 21 had missing external dependencies:
                    - 1 examples.EntryPoint()
                    - examples.Foo(num=100, num2=16) and 9 other examples.Foo
                    - 10 examples.DateTask(date=1998-03-23...1998-04-01, num=5)

            This progress looks :| because there were missing external dependencies

            ===== Luigi Execution Summary =====
"""


from __future__ import print_function
import datetime

import luigi


class MyExternal(luigi.ExternalTask):

    def complete(self):
        return False


class Boom(luigi.Task):
    task_namespace = 'examples'
    this_is_a_really_long_I_mean_way_too_long_and_annoying_parameter = luigi.IntParameter()

    def run(self):
        print("Running Boom")

    def requires(self):
        for i in range(5, 200):
            yield Bar(i)


class Foo(luigi.Task):
    task_namespace = 'examples'
    num = luigi.IntParameter()
    num2 = luigi.IntParameter()

    def run(self):
        print("Running Foo")

    def requires(self):
        yield MyExternal()
        yield Boom(0)


class Bar(luigi.Task):
    task_namespace = 'examples'
    num = luigi.IntParameter()

    def run(self):
        self.output().open('w').close()

    def output(self):
        return luigi.LocalTarget('/tmp/bar/%d' % self.num)


class DateTask(luigi.Task):
    task_namespace = 'examples'
    date = luigi.DateParameter()
    num = luigi.IntParameter()

    def run(self):
        print("Running DateTask")

    def requires(self):
        yield MyExternal()
        yield Boom(0)


class EntryPoint(luigi.Task):
    task_namespace = 'examples'

    def run(self):
        print("Running EntryPoint")

    def requires(self):
        for i in range(10):
            yield Foo(100, 2 * i)
        for i in range(10):
            yield DateTask(datetime.date(1998, 3, 23) + datetime.timedelta(days=i), 5)
