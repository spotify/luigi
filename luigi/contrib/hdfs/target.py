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
Provides access to HDFS using the :py:class:`HdfsTarget`, a subclass of :py:class:`~luigi.target.Target`.
"""

import luigi
import random
import warnings
from luigi.target import FileSystemTarget
from luigi.contrib.hdfs.config import tmppath
from luigi.contrib.hdfs import format as hdfs_format
from luigi.contrib.hdfs import clients as hdfs_clients
from luigi.six.moves.urllib import parse as urlparse
from luigi.six.moves import range


class HdfsTarget(FileSystemTarget):

    def __init__(self, path=None, format=None, is_tmp=False, fs=None):
        if path is None:
            assert is_tmp
            path = tmppath()
        super(HdfsTarget, self).__init__(path)

        if format is None:
            format = luigi.format.get_default_format() >> hdfs_format.Plain

        old_format = (
            (
                hasattr(format, 'hdfs_writer') or
                hasattr(format, 'hdfs_reader')
            ) and
            not hasattr(format, 'output')
        )

        if not old_format and getattr(format, 'output', '') != 'hdfs':
            format = format >> hdfs_format.Plain

        if old_format:
            warnings.warn(
                'hdfs_writer and hdfs_reader method for format is deprecated,'
                'specify the property output of your format as \'hdfs\' instead',
                DeprecationWarning,
                stacklevel=2
            )

            if hasattr(format, 'hdfs_writer'):
                format_writer = format.hdfs_writer
            else:
                w_format = format >> hdfs_format.Plain
                format_writer = w_format.pipe_writer

            if hasattr(format, 'hdfs_reader'):
                format_reader = format.hdfs_reader
            else:
                r_format = format >> hdfs_format.Plain
                format_reader = r_format.pipe_reader

            format = hdfs_format.CompatibleHdfsFormat(
                format_writer,
                format_reader,
            )

        else:
            format = hdfs_format.CompatibleHdfsFormat(
                format.pipe_writer,
                format.pipe_reader,
                getattr(format, 'input', None),
            )

        self.format = format

        self.is_tmp = is_tmp
        (scheme, netloc, path, query, fragment) = urlparse.urlsplit(path)
        if ":" in path:
            raise ValueError('colon is not allowed in hdfs filenames')
        self._fs = fs or hdfs_clients.get_autoconfig_client()

    def __del__(self):
        # TODO: not sure is_tmp belongs in Targets construction arguments
        if self.is_tmp and self.exists():
            self.remove(skip_trash=True)

    @property
    def fs(self):
        return self._fs

    def glob_exists(self, expected_files):
        ls = list(self.fs.listdir(self.path))
        if len(ls) == expected_files:
            return True
        return False

    def open(self, mode='r'):
        if mode not in ('r', 'w'):
            raise ValueError("Unsupported open mode '%s'" % mode)

        if mode == 'r':
            return self.format.pipe_reader(self.path)
        else:
            return self.format.pipe_writer(self.path)

    def remove(self, skip_trash=False):
        self.fs.remove(self.path, skip_trash=skip_trash)

    def rename(self, path, raise_if_exists=False):
        """
        Does not change self.path.

        Unlike ``move_dir()``, ``rename()`` might cause nested directories.
        See spotify/luigi#522
        """
        if isinstance(path, HdfsTarget):
            path = path.path
        if raise_if_exists and self.fs.exists(path):
            raise RuntimeError('Destination exists: %s' % path)
        self.fs.rename(self.path, path)

    def move(self, path, raise_if_exists=False):
        """
        Alias for ``rename()``
        """
        self.rename(path, raise_if_exists=raise_if_exists)

    def move_dir(self, path):
        """
        Move using :py:class:`~luigi.contrib.hdfs.abstract_client.HdfsFileSystem.rename_dont_move`

        New since after luigi v2.1: Does not change self.path

        One could argue that the implementation should use the
        mkdir+raise_if_exists approach, but we at Spotify have had more trouble
        with that over just using plain mv.  See spotify/luigi#557
        """
        self.fs.rename_dont_move(self.path, path)

    def copy(self, dst_dir):
        """
        Copy to destination directory.
        """
        self.fs.copy(self.path, dst_dir)

    def is_writable(self):
        """
        Currently only works with hadoopcli
        """
        if "/" in self.path:
            # example path: /log/ap/2013-01-17/00
            parts = self.path.split("/")
            # start with the full path and then up the tree until we can check
            length = len(parts)
            for part in range(length):
                path = "/".join(parts[0:length - part]) + "/"
                if self.fs.exists(path):
                    # if the path exists and we can write there, great!
                    if self._is_writable(path):
                        return True
                    # if it exists and we can't =( sad panda
                    else:
                        return False
            # We went through all parts of the path and we still couldn't find
            # one that exists.
            return False

    def _is_writable(self, path):
        test_path = path + '.test_write_access-%09d' % random.randrange(1e10)
        try:
            self.fs.touchz(test_path)
            self.fs.remove(test_path, recursive=False)
            return True
        except hdfs_clients.HDFSCliError:
            return False
