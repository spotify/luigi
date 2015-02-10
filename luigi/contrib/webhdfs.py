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
Provides a WebHdfsTarget and WebHdfsClient using the
python [hdfs](https://pypi.python.org/pypi/hdfs/) library.
"""

from __future__ import absolute_import

import logging
import os
import random
import tempfile

from luigi import configuration
from luigi.target import FileSystemTarget

logger = logging.getLogger("luigi-interface")

try:
    import hdfs as webhdfs
except ImportError:
    logger.warning("Loading webhdfs module without `hdfs` package installed. "
                   "Will crash at runtime if webhdfs functionality is used.")


class WebHdfsTarget(FileSystemTarget):
    fs = None

    def __init__(self, path, client=None):
        super(WebHdfsTarget, self).__init__(path)
        path = self.path
        self.fs = client or WebHdfsClient()

    def open(self, mode='r'):
        if mode not in ('r', 'w'):
            raise ValueError("Unsupported open mode '%s'" % mode)
        if mode == 'r':
            return ReadableWebHdfsFile(path=self.path, client=self.fs)
        elif mode == 'w':
            return AtomicWebHdfsFile(path=self.path, client=self.fs)


class ReadableWebHdfsFile(object):

    def __init__(self, path, client):
        self.path = path
        self.client = client
        self.generator = None

    def read(self):
        self.generator = self.client.read(self.path)
        return list(self.generator)[0]

    def readlines(self, char='\n'):
        self.generator = self.client.read(self.path, buffer_char=char)
        return self.generator

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()

    def __iter__(self):
        self.generator = self.readlines('\n')
        has_next = True
        while has_next:
            try:
                chunk = self.generator.next()
                yield chunk
            except StopIteration:
                has_next = False
        self.close()

    def close(self):
        self.generator.close()


class AtomicWebHdfsFile(file):
    """
    An Hdfs file that writes to a temp file and put to WebHdfs on close.
    """

    def __init__(self, path, client):
        unique_name = 'luigi-webhdfs-tmp-%09d' % random.randrange(0, 1e10)
        self.tmp_path = os.path.join(tempfile.gettempdir(), unique_name)
        self.path = path
        self.client = client
        super(AtomicWebHdfsFile, self).__init__(self.tmp_path, 'w')

    def close(self):
        super(AtomicWebHdfsFile, self).close()
        if not self.client.exists(self.path):
            self.client.upload(self.path, self.tmp_path)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        """
        Close/commit the file if there are no exception.
        """
        if exc_type:
            return
        return file.__exit__(self, exc_type, exc, traceback)

    def __del__(self):
        """
        Remove the temporary directory.
        """
        if os.path.exists(self.tmp_path):
            os.remove(self.tmp_path)


class WebHdfsClient(object):

    def __init__(self, host=None, port=None, user=None):
        host = self.get_config('namenode_host') if host is None else host
        port = self.get_config('namenode_port') if port is None else port
        user = self.get_config('user') if user is None else os.environ['USER']

        url = 'http://' + host + ':' + port
        self.webhdfs = webhdfs.InsecureClient(url=url, user=user)

    def get_config(self, key):
        config = configuration.get_config()
        try:
            return config.get('hdfs', key)
        except:
            raise RuntimeError("You must specify %s in the [hdfs] section of "
                               "the luigi client.cfg file" % key)

    def walk(self, path, depth=1):
        return self.webhdfs.walk(path, depth=depth)

    def exists(self, path):
        """
        Returns true if the path exists and false otherwise.
        """
        try:
            self.webhdfs.status(path)
            return True
        except webhdfs.util.HdfsError as e:
            if str(e).startswith('File does not exist: '):
                return False
            else:
                raise e

    def upload(self, hdfs_path, local_path, overwrite=False):
        return self.webhdfs.upload(hdfs_path, local_path, overwrite=overwrite)

    def download(self, hdfs_path, local_path, overwrite=False, n_threads=-1):
        return self.webhdfs.download(hdfs_path, local_path, overwrite=overwrite,
                                     n_threads=n_threads)

    def remove(self, hdfs_path, recursive=False):
        return self.webhdfs.delete(hdfs_path, recursive=recursive)

    def read(self, hdfs_path, offset=0, length=None, buffer_size=None,
             chunk_size=1024, buffer_char=None):
        return self.webhdfs.read(hdfs_path, offset=offset, length=length,
                                 buffer_size=buffer_size, chunk_size=chunk_size,
                                 buffer_char=buffer_char)
