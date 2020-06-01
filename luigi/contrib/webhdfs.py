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
"""
Provides a :class:`WebHdfsTarget` using the `Python hdfs
<https://pypi.python.org/pypi/hdfs/>`_

This module is DEPRECATED and does not play well with rest of luigi's hdfs
contrib module. You can consider migrating to
:class:`luigi.contrib.hdfs.webhdfs_client.WebHdfsClient`
"""

import logging

from luigi.target import FileSystemTarget, AtomicLocalFile
from luigi.format import get_default_format
import luigi.contrib.hdfs

logger = logging.getLogger("luigi-interface")


class WebHdfsTarget(FileSystemTarget):
    fs = None

    def __init__(self, path, client=None, format=None):
        super(WebHdfsTarget, self).__init__(path)
        path = self.path
        self.fs = client or WebHdfsClient()
        if format is None:
            format = get_default_format()

        self.format = format

    def open(self, mode='r'):
        if mode not in ('r', 'w'):
            raise ValueError("Unsupported open mode '%s'" % mode)

        if mode == 'r':
            return self.format.pipe_reader(
                ReadableWebHdfsFile(path=self.path, client=self.fs)
            )

        return self.format.pipe_writer(
            AtomicWebHdfsFile(path=self.path, client=self.fs)
        )


class ReadableWebHdfsFile:

    def __init__(self, path, client):
        self.path = path
        self.client = client
        self.generator = None

    def read(self):
        self.generator = self.client.read(self.path)
        res = list(self.generator)[0]
        return res

    def readlines(self, char='\n'):
        self.generator = self.client.read(self.path, buffer_char=char)
        return self.generator

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()

    def __iter__(self):
        self.generator = self.readlines('\n')
        yield from self.generator
        self.close()

    def close(self):
        self.generator.close()


class AtomicWebHdfsFile(AtomicLocalFile):
    """
    An Hdfs file that writes to a temp file and put to WebHdfs on close.
    """

    def __init__(self, path, client):
        self.client = client
        super(AtomicWebHdfsFile, self).__init__(path)

    def move_to_final_destination(self):
        if not self.client.exists(self.path):
            self.client.upload(self.path, self.tmp_path)


WebHdfsClient = luigi.contrib.hdfs.WebHdfsClient
