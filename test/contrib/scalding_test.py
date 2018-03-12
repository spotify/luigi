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

import luigi
from luigi.contrib import scalding

import mock
import os
import random
import shutil
import tempfile
import unittest


class MyScaldingTask(scalding.ScaldingJobTask):
    scala_source = luigi.Parameter()

    def source(self):
        return self.scala_source


class ScaldingTest(unittest.TestCase):
    def setUp(self):
        self.scalding_home = os.path.join(tempfile.gettempdir(), 'scalding-%09d' % random.randint(0, 999999999))
        os.mkdir(self.scalding_home)
        self.lib_dir = os.path.join(self.scalding_home, 'lib')
        os.mkdir(self.lib_dir)
        os.mkdir(os.path.join(self.scalding_home, 'provided'))
        os.mkdir(os.path.join(self.scalding_home, 'libjars'))
        f = open(os.path.join(self.lib_dir, 'scalding-core-foo'), 'w')
        f.close()

        self.scala_source = os.path.join(self.scalding_home, 'my_source.scala')
        f = open(self.scala_source, 'w')
        f.write('class foo extends Job')
        f.close()

        os.environ['SCALDING_HOME'] = self.scalding_home

    def tearDown(self):
        shutil.rmtree(self.scalding_home)

    @mock.patch('subprocess.check_call')
    @mock.patch('luigi.contrib.hadoop.run_and_track_hadoop_job')
    def test_scalding(self, check_call, track_job):
        success = luigi.run(['MyScaldingTask', '--scala-source', self.scala_source, '--local-scheduler', '--no-lock'])
        self.assertTrue(success)
        # TODO: check more stuff
