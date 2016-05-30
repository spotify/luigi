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

"""
A luigi file system client that wraps around the hdfs-library (a webhdfs
client)

This is a sensible fast alternative to snakebite. In particular for python3
users, where snakebite is not supported at the time of writing (dec 2015).

Note. This wrapper client is not feature complete yet. As with most software
the authors only implement the features they need.  If you need to wrap more of
the file system operations, please do and contribute back.
"""


from luigi.contrib.hdfs import config as hdfs_config
from luigi.contrib.hdfs import abstract_client as hdfs_abstract_client
import luigi.contrib.target
import logging
import os
import warnings

logger = logging.getLogger('luigi-interface')


class webhdfs(luigi.Config):
    port = luigi.IntParameter(default=50070,
                              description='Port for webhdfs')
    user = luigi.Parameter(default=None, description='Defaults to $USER envvar',
                           config_path=dict(section='hdfs', name='user'))


class WebHdfsClient(hdfs_abstract_client.HdfsFileSystem):
    """
    A webhdfs that tries to confirm to luigis interface for file existence.

    The library is using `this api
    <https://hdfscli.readthedocs.io/en/latest/api.html>`__.
    """
    def __init__(self, host=None, port=None, user=None):
        self.host = host or hdfs_config.hdfs().namenode_host
        self.port = port or webhdfs().port
        self.user = user or webhdfs().user or os.environ['USER']

    @property
    def url(self):
        return 'http://' + self.host + ':' + str(self.port)

    @property
    def client(self):
        # A naive benchmark showed that 1000 existence checks took 2.5 secs
        # when not recreating the client, and 4.0 secs when recreating it. So
        # not urgent to memoize it. Note that it *might* be issues with process
        # forking and whatnot (as the one in the snakebite client) if we
        # memoize it too trivially.
        import hdfs
        return hdfs.InsecureClient(url=self.url, user=self.user)

    def walk(self, path, depth=1):
        return self.client.walk(path, depth=depth)

    def exists(self, path):
        """
        Returns true if the path exists and false otherwise.
        """
        import hdfs
        try:
            self.client.status(path)
            return True
        except hdfs.util.HdfsError as e:
            if str(e).startswith('File does not exist: '):
                return False
            else:
                raise e

    def upload(self, hdfs_path, local_path, overwrite=False):
        return self.client.upload(hdfs_path, local_path, overwrite=overwrite)

    def download(self, hdfs_path, local_path, overwrite=False, n_threads=-1):
        return self.client.download(hdfs_path, local_path, overwrite=overwrite,
                                    n_threads=n_threads)

    def remove(self, hdfs_path, recursive=True, skip_trash=False):
        assert skip_trash  # Yes, you need to explicitly say skip_trash=True
        return self.client.delete(hdfs_path, recursive=recursive)

    def read(self, hdfs_path, offset=0, length=None, buffer_size=None,
             chunk_size=1024, buffer_char=None):
        return self.client.read(hdfs_path, offset=offset, length=length,
                                buffer_size=buffer_size, chunk_size=chunk_size,
                                buffer_char=buffer_char)

    def move(self, path, dest):
        parts = dest.rstrip('/').split('/')
        if len(parts) > 1:
            dir_path = '/'.join(parts[0:-1])
            if not self.exists(dir_path):
                self.mkdir(dir_path, parents=True)
        self.client.rename(path, dest)

    def mkdir(self, path, parents=True, mode=0o755, raise_if_exists=False):
        """
        Has no returnvalue (just like WebHDFS)
        """
        if not parents or raise_if_exists:
            warnings.warn('webhdfs mkdir: parents/raise_if_exists not implemented')
        permission = int(oct(mode)[2:])  # Convert from int(decimal) to int(octal)
        self.client.makedirs(path, permission=permission)

    def chmod(self, path, permissions, recursive=False):
        """
        Raise a NotImplementedError exception.
        """
        raise NotImplementedError("Webhdfs in luigi doesn't implement chmod")

    def chown(self, path, owner, group, recursive=False):
        """
        Raise a NotImplementedError exception.
        """
        raise NotImplementedError("Webhdfs in luigi doesn't implement chown")

    def count(self, path):
        """
        Raise a NotImplementedError exception.
        """
        raise NotImplementedError("Webhdfs in luigi doesn't implement count")

    def copy(self, path, destination):
        """
        Raise a NotImplementedError exception.
        """
        raise NotImplementedError("Webhdfs in luigi doesn't implement copy")

    def put(self, local_path, destination):
        """
        Restricted version of upload
        """
        self.upload(local_path, destination)

    def get(self, path, local_destination):
        """
        Restricted version of download
        """
        self.download(path, local_destination)

    def listdir(self, path, ignore_directories=False, ignore_files=False,
                include_size=False, include_type=False, include_time=False,
                recursive=False):
        assert not recursive
        return self.client.list(path, status=False)

    def touchz(self, path):
        """
        To touchz using the web hdfs "write" cmd.
        """
        self.client.write(path, data='', overwrite=False)
