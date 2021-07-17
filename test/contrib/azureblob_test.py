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

import os
import unittest
import json

import pytest

import luigi
from luigi.contrib.azureblob import AzureBlobClient, AzureBlobTarget

account_name = os.environ.get("ACCOUNT_NAME")
account_key = os.environ.get("ACCOUNT_KEY")
sas_token = os.environ.get("SAS_TOKEN")
is_emulated = False if account_name else True
client = AzureBlobClient(account_name, account_key, sas_token, is_emulated=is_emulated)


@pytest.mark.azureblob
class AzureBlobClientTest(unittest.TestCase):
    def setUp(self):
        self.client = client

    def tearDown(self):
        pass

    def test_splitfilepath_blob_none(self):
        container, blob = self.client.splitfilepath("abc")
        self.assertEqual(container, "abc")
        self.assertIsNone(blob)

    def test_splitfilepath_blob_toplevel(self):
        container, blob = self.client.splitfilepath("abc/cde")
        self.assertEqual(container, "abc")
        self.assertEqual(blob, "cde")

    def test_splitfilepath_blob_nested(self):
        container, blob = self.client.splitfilepath("abc/cde/xyz.txt")
        self.assertEqual(container, "abc")
        self.assertEqual(blob, "cde/xyz.txt")

    def test_create_delete_container(self):
        import datetime
        import hashlib
        m = hashlib.md5()
        m.update(datetime.datetime.now().__str__().encode())
        container_name = m.hexdigest()

        self.assertFalse(self.client.exists(container_name))
        self.assertTrue(self.client.create_container(container_name))
        self.assertTrue(self.client.exists(container_name))
        self.client.delete_container(container_name)
        self.assertFalse(self.client.exists(container_name))

    def test_upload_copy_move_remove_blob(self):
        import datetime
        import hashlib
        import tempfile
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
        self.assertIn(self.client.copy(from_path, to_path).status, ["success", "pending"])
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


class MovieScriptTask(luigi.Task):
    def output(self):
        return AzureBlobTarget("luigi-test", "movie-cheesy.txt", client, download_when_reading=False)

    def run(self):
        client.connection.create_container("luigi-test")
        with self.output().open("w") as op:
            op.write("I'm going to make him an offer he can't refuse.\n")
            op.write("Toto, I've got a feeling we're not in Kansas anymore.\n")
            op.write("May the Force be with you.\n")
            op.write("Bond. James Bond.\n")
            op.write("Greed, for lack of a better word, is good.\n")


class AzureJsonDumpTask(luigi.Task):
    def output(self):
        return AzureBlobTarget("luigi-test", "stats.json", client)

    def run(self):
        with self.output().open("w") as op:
            json.dump([1, 2, 3], op)


class FinalTask(luigi.Task):
    def requires(self):
        return {"movie": self.clone(MovieScriptTask), "np": self.clone(AzureJsonDumpTask)}

    def run(self):
        with self.input()["movie"].open('r') as movie, self.input()["np"].open('r') as np, self.output().open('w') as output:
            movie_lines = movie.read()
            assert "Toto, I've got a feeling" in movie_lines
            output.write(movie_lines)

            data = json.load(np)
            assert data == [1, 2, 3]
            output.write(data.__str__())

    def output(self):
        return luigi.LocalTarget("samefile")


@pytest.mark.azureblob
class AzureBlobTargetTest(unittest.TestCase):
    def setUp(self):
        self.client = client

    def tearDown(self):
        pass

    def test_AzureBlobTarget(self):
        final_task = FinalTask()
        luigi.build([final_task], local_scheduler=True, log_level='NOTSET')
        output = final_task.output().open("r").read()
        assert "Toto" in output
