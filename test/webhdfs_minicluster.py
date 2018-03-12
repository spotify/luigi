# -*- coding: utf-8 -*-
#
# Copyright 2015 VNG Corporation
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

from minicluster import MiniClusterTestCase
import unittest
import subprocess
import select
import re

try:
    from snakebite.minicluster import MiniCluster
except ImportError:
    raise unittest.SkipTest('To use minicluster, snakebite must be installed.')


class WebHdfsMiniCluster(MiniCluster):
    '''
    This is a unclean class overriding of the snakebite minicluster.

    But since it seemed pretty inflexible I had to override private methods
    here.
    '''
    @property
    def webhdfs_port(self):
        return self.port

    def _start_mini_cluster(self, nnport=None):
        """
        Copied in an ugly manner from snakebite source code.
        """
        if self._jobclient_jar:
            hadoop_jar = self._jobclient_jar
        else:
            hadoop_jar = self._find_mini_cluster_jar(self._hadoop_home)
        if not hadoop_jar:
            raise Exception("No hadoop jobclient test jar found")
        cmd = [self._hadoop_cmd, 'jar', hadoop_jar,
               'minicluster', '-nomr', '-format']
        if nnport:
            cmd.extend(['-nnport', "%s" % nnport])
        if True:
            # luigi webhdfs version
            cmd.extend(['-Ddfs.webhdfs.enabled=true'])
        self.hdfs = subprocess.Popen(cmd, bufsize=0, stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE, universal_newlines=True)

    def _get_namenode_port(self):
        just_seen_webhdfs = False
        while self.hdfs.poll() is None:
            rlist, wlist, xlist = select.select([self.hdfs.stderr, self.hdfs.stdout], [], [])
            for f in rlist:
                line = f.readline()
                print(line.rstrip())

                m = re.match(".*Jetty bound to port (\d+).*", line)
                if just_seen_webhdfs and m:
                    return int(m.group(1))
                just_seen_webhdfs = re.match(".*namenode.*webhdfs.*", line)


class WebHdfsMiniClusterTestCase(MiniClusterTestCase):

    @classmethod
    def instantiate_cluster(cls):
        return WebHdfsMiniCluster(None, nnport=50030)
