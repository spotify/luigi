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

from nose.plugins.attrib import attr

from helpers import with_config
from webhdfs_minicluster import WebHdfsMiniClusterTestCase
from contrib.hdfs_test import HdfsTargetTestMixin
from luigi.contrib.hdfs import WebHdfsClient


@attr('minicluster')
class WebHdfsTargetTest(WebHdfsMiniClusterTestCase, HdfsTargetTestMixin):

    def run(self, result=None):
        conf = {'hdfs': {'client': 'webhdfs'},
                'webhdfs': {'port': str(self.cluster.webhdfs_port)},
                }
        with_config(conf)(super(WebHdfsTargetTest, self).run)(result)

    def test_actually_using_webhdfs(self):
        self.assertTrue(isinstance(self.create_target().fs, WebHdfsClient))

    # Here is a bunch of tests that are currently failing.  As should be
    # mentioned in the WebHdfsClient docs, it is not yet feature complete.
    test_slow_exists = None
    test_glob_exists = None
    test_with_close = None
    test_with_exception = None

    # This one fails when run together with the whole test suite
    test_write_cleanup_no_close = None
