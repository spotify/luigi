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

import gzip
import mock
import os
import tempfile
import unittest
from luigi import target_factory


class TestTargetFactory(unittest.TestCase):

    def setUp(self):
        self.data = 'someoutputdata'
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.write(self.data.encode('utf-8'))
        self.tmp.close()

        tmp_gz_file = tempfile.NamedTemporaryFile(delete=False, suffix=".gz")
        tmp_gz_file.close()
        self.tmp_gz = gzip.open(tmp_gz_file.name, 'wb')
        self.tmp_gz.write(self.data.encode('utf-8'))
        self.tmp_gz.close()

    def tearDown(self):
        if self.tmp:
            os.remove(self.tmp.name)
            self.tmp = None

    @mock.patch('luigi.target_factory.S3Target')
    def test_get_target_s3(self, mock_s3_target_cls):
        mock_s3_target = mock.Mock()
        mock_s3_target_cls.return_value = mock_s3_target
        s3_target = target_factory.get_target('s3://mybucket/mypath')
        self.assertEquals(mock_s3_target, s3_target)
        s3n_target = target_factory.get_target('s3n://mybucket/mypath')
        self.assertEquals(mock_s3_target, s3n_target)

    def test_get_target_local(self):
        local_target = target_factory.get_target(self.tmp.name)
        reread_data = local_target.open().read()
        self.assertEquals(self.data, reread_data)

    def test_get_target_file(self):
        file_target = target_factory.get_target('file://%s' % self.tmp.name)
        reread_data = file_target.open().read()
        self.assertEquals(self.data, reread_data)

    def test_get_target_file_gz(self):
        file_target = target_factory.get_target('file://%s' % self.tmp_gz.name)
        reread_data = file_target.open().read()
        self.assertEquals(self.data, reread_data)
