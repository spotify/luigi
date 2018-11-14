# -*- coding: utf-8 -*-
#
# Copyright 2018 Microsoft Corporation
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
"""
Integration tests for azureblob module.
"""

import unittest

from luigi.contrib.azureblob import *

client = AzureBlobClient(os.environ.get("ACCOUNT_NAME"), os.environ.get("ACCOUNT_KEY"),os.environ.get("SAS_TOKEN"))

class AzureBlobClientTest(unittest.TestCase):
    def setUp(self):
        self.client = client

    def tearDown(self):
        pass

    def test_splitfilepath_blob_none(self):
        container, blob = self.client.splitfilepath("abc")
        self.assertEquals(container, "abc")
        self.assertIsNone(blob)

    def test_splitfilepath_blob_toplevel(self):
        container, blob = self.client.splitfilepath("abc/cde")
        self.assertEquals(container, "abc")
        self.assertEquals(blob, "cde")

    def test_splitfilepath_blob_nested(self):
        container, blob = self.client.splitfilepath("abc/cde/xyz.txt")
        self.assertEquals(container, "abc")
        self.assertEquals(blob, "cde/xyz.txt")

    def test_create_delete_container(self):
        import datetime, hashlib
        m = hashlib.md5()
        m.update(datetime.datetime.now().__str__().encode())
        container_name = m.hexdigest()

        self.assertFalse(self.client.exists(container_name))
        self.assertTrue(self.client.create_container(container_name))
        self.assertTrue(self.client.exists(container_name))
        self.client.delete_container(container_name)
        self.assertFalse(self.client.exists(container_name))

    def test_upload_copy_move_remove_blob(self):
        import datetime, hashlib, tempfile
        m = hashlib.md5()
        m.update(datetime.datetime.now().__str__().encode())
        container_name = m.hexdigest()
        m.update(datetime.datetime.now().__str__().encode())
        from_blob_name = m.hexdigest()
        from_path = "{container_name}/{from_blob_name}".format(container_name=container_name,
                                                               from_blob_name=from_blob_name)
        m.update(datetime.datetime.now().__str__().encode())
        to_blob_name = m.hexdigest()
        to_path = "{container_name}/{to_blob_name}".format(container_name=container_name, to_blob_name=to_blob_name)
        message = datetime.datetime.now().__str__().encode()

        self.assertTrue(self.client.create_container(container_name))
        with tempfile.NamedTemporaryFile() as f:
            f.write(message)
            f.flush()

            # upload
            self.client.upload(f.name, container_name, from_blob_name)
            self.assertTrue(self.client.exists(from_path))

        # copy
        print("Trying to copy from '{}' to '{}'".format(from_path, to_path))
        self.assertEquals(self.client.copy(from_path, to_path).status, "success")
        self.assertTrue(self.client.exists(to_path))

        # remove
        self.assertTrue(self.client.remove(from_path))
        self.assertFalse(self.client.exists(from_path))

        # move back file
        self.client.move(to_path, from_path)
        self.assertTrue(self.client.exists(from_path))
        self.assertFalse(self.client.exists(to_path))

        self.assertTrue(self.client.remove(from_path))
        self.assertFalse(self.client.exists(from_path))

        # delete container
        self.client.delete_container(container_name)
        self.assertFalse(self.client.exists(container_name))