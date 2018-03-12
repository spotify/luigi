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

import getpass
import os

import luigi.contrib.hadoop
import luigi.contrib.hdfs
from nose.plugins.attrib import attr

import unittest

try:
    from snakebite.minicluster import MiniCluster
except ImportError:
    raise unittest.SkipTest('To use minicluster, snakebite must be installed.')


@attr('minicluster')
class MiniClusterTestCase(unittest.TestCase):

    """ Base class for test cases that rely on Hadoop's minicluster functionality. This
    in turn depends on Snakebite's minicluster setup:

    http://hadoop.apache.org/docs/r2.5.1/hadoop-project-dist/hadoop-common/CLIMiniCluster.html
    https://github.com/spotify/snakebite"""
    cluster = None

    @classmethod
    def instantiate_cluster(cls):
        return MiniCluster(None, nnport=50030)

    @classmethod
    def setupClass(cls):
        if not cls.cluster:
            cls.cluster = cls.instantiate_cluster()
        cls.cluster.mkdir("/tmp")

    @classmethod
    def tearDownClass(cls):
        if cls.cluster:
            cls.cluster.terminate()

    def setUp(self):
        self.fs = luigi.contrib.hdfs.get_autoconfig_client()
        cfg_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "testconfig")
        hadoop_bin = os.path.join(os.environ['HADOOP_HOME'], 'bin/hadoop')
        cmd = "{} --config {}".format(hadoop_bin, cfg_path)
        self.stashed_hdfs_client = luigi.configuration.get_config().get('hadoop', 'command', None)
        luigi.configuration.get_config().set('hadoop', 'command', cmd)

    def tearDown(self):
        if self.fs.exists(self._test_dir()):
            self.fs.remove(self._test_dir(), skip_trash=True)
        if self.stashed_hdfs_client:
            luigi.configuration.get_config().set('hadoop', 'command', self.stashed_hdfs_client)

    @staticmethod
    def _test_dir():
        return '/tmp/luigi_tmp_testdir_%s' % getpass.getuser()

    @staticmethod
    def _test_file(suffix=""):
        return '%s/luigi_tmp_testfile%s' % (MiniClusterTestCase._test_dir(), suffix)


class MiniClusterHadoopJobRunner(luigi.contrib.hadoop.HadoopJobRunner):

    ''' The default job runner just reads from config and sets stuff '''

    def __init__(self):
        # Locate the hadoop streaming jar in the hadoop directory
        hadoop_tools_lib = os.path.join(os.environ['HADOOP_HOME'], 'share/hadoop/tools/lib')

        for path in os.listdir(hadoop_tools_lib):
            if path.startswith('hadoop-streaming') and path.endswith('.jar'):
                streaming_jar = os.path.join(hadoop_tools_lib, path)
                break
        else:
            raise Exception('Could not locate streaming jar in ' + hadoop_tools_lib)

        super(MiniClusterHadoopJobRunner, self).__init__(streaming_jar=streaming_jar)
