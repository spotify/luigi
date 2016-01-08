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
A luigi file system client that wraps around snakebite

Originally written by Alan Brenner <alan@magnetic.com> github.com/alanbbr
"""


from luigi.contrib.hdfs import config as hdfs_config
from luigi.contrib.hdfs import error as hdfs_error
from luigi.contrib.hdfs import abstract_client as hdfs_abstract_client
from luigi import six
import luigi.contrib.target
import logging
import datetime
import os

logger = logging.getLogger('luigi-interface')


class SnakebiteHdfsClient(hdfs_abstract_client.HdfsFileSystem):
    """
    A hdfs client using snakebite. Since Snakebite has a python API, it'll be
    about 100 times faster than the hadoop cli client, which does shell out to
    a java program on each file system operation.
    """

    def __init__(self):
        super(SnakebiteHdfsClient, self).__init__()
        self._bite = None
        self.pid = -1

    @staticmethod
    def list_path(path):
        if isinstance(path, list) or isinstance(path, tuple):
            return path
        # TODO: Should this be:
        # isinstance(path, (six.text_type, six.binary_type))?
        if isinstance(path, six.string_types):
            return [path, ]
        return [str(path), ]

    def get_bite(self):
        """
        If Luigi has forked, we have a different PID, and need to reconnect.
        """
        config = hdfs_config.hdfs()
        if self.pid != os.getpid() or not self._bite:
            client_kwargs = dict(filter(
                lambda k_v: k_v[1] is not None and k_v[1] != '', six.iteritems({
                    'hadoop_version': config.client_version,
                    'effective_user': config.effective_user,
                })
            ))
            if config.snakebite_autoconfig:
                """
                This is fully backwards compatible with the vanilla Client and can be used for a non HA cluster as well.
                This client tries to read ``${HADOOP_PATH}/conf/hdfs-site.xml`` to get the address of the namenode.
                The behaviour is the same as Client.
                """
                from snakebite.client import AutoConfigClient
                self._bite = AutoConfigClient(**client_kwargs)
            else:
                from snakebite.client import Client
                self._bite = Client(config.namenode_host, config.namenode_port, **client_kwargs)
        return self._bite

    def exists(self, path):
        """
        Use snakebite.test to check file existence.

        :param path: path to test
        :type path: string
        :return: boolean, True if path exists in HDFS
        """
        try:
            return self.get_bite().test(path, exists=True)
        except Exception as err:    # IGNORE:broad-except
            raise hdfs_error.HDFSCliError("snakebite.test", -1, str(err), repr(err))

    def move(self, path, dest):
        """
        Use snakebite.rename, if available.

        :param path: source file(s)
        :type path: either a string or sequence of strings
        :param dest: destination file (single input) or directory (multiple)
        :type dest: string
        :return: list of renamed items
        """
        parts = dest.rstrip('/').split('/')
        if len(parts) > 1:
            dir_path = '/'.join(parts[0:-1])
            if not self.exists(dir_path):
                self.mkdir(dir_path, parents=True)
        return list(self.get_bite().rename(self.list_path(path), dest))

    def rename_dont_move(self, path, dest):
        """
        Use snakebite.rename_dont_move, if available.

        :param path: source path (single input)
        :type path: string
        :param dest: destination path
        :type dest: string
        :return: True if succeeded
        :raises: snakebite.errors.FileAlreadyExistsException
        """
        from snakebite.errors import FileAlreadyExistsException
        try:
            self.get_bite().rename2(path, dest, overwriteDest=False)
        except FileAlreadyExistsException:
            # Unfortunately python2 don't allow exception chaining.
            raise luigi.target.FileAlreadyExists()

    def remove(self, path, recursive=True, skip_trash=False):
        """
        Use snakebite.delete, if available.

        :param path: delete-able file(s) or directory(ies)
        :type path: either a string or a sequence of strings
        :param recursive: delete directories trees like \*nix: rm -r
        :type recursive: boolean, default is True
        :param skip_trash: do or don't move deleted items into the trash first
        :type skip_trash: boolean, default is False (use trash)
        :return: list of deleted items
        """
        return list(self.get_bite().delete(self.list_path(path), recurse=recursive))

    def chmod(self, path, permissions, recursive=False):
        """
        Use snakebite.chmod, if available.

        :param path: update-able file(s)
        :type path: either a string or sequence of strings
        :param permissions: \*nix style permission number
        :type permissions: octal
        :param recursive: change just listed entry(ies) or all in directories
        :type recursive: boolean, default is False
        :return: list of all changed items
        """
        if type(permissions) == str:
            permissions = int(permissions, 8)
        return list(self.get_bite().chmod(self.list_path(path),
                                          permissions, recursive))

    def chown(self, path, owner, group, recursive=False):
        """
        Use snakebite.chown/chgrp, if available.

        One of owner or group must be set. Just setting group calls chgrp.

        :param path: update-able file(s)
        :type path: either a string or sequence of strings
        :param owner: new owner, can be blank
        :type owner: string
        :param group: new group, can be blank
        :type group: string
        :param recursive: change just listed entry(ies) or all in directories
        :type recursive: boolean, default is False
        :return: list of all changed items
        """
        bite = self.get_bite()
        if owner:
            if group:
                return all(bite.chown(self.list_path(path), "%s:%s" % (owner, group),
                                      recurse=recursive))
            return all(bite.chown(self.list_path(path), owner, recurse=recursive))
        return list(bite.chgrp(self.list_path(path), group, recurse=recursive))

    def count(self, path):
        """
        Use snakebite.count, if available.

        :param path: directory to count the contents of
        :type path: string
        :return: dictionary with content_size, dir_count and file_count keys
        """
        try:
            res = self.get_bite().count(self.list_path(path)).next()
            dir_count = res['directoryCount']
            file_count = res['fileCount']
            content_size = res['spaceConsumed']
        except StopIteration:
            dir_count = file_count = content_size = 0
        return {'content_size': content_size, 'dir_count': dir_count,
                'file_count': file_count}

    def copy(self, path, destination):
        """
        Raise a NotImplementedError exception.
        """
        raise NotImplementedError("SnakebiteClient in luigi doesn't implement copy")

    def put(self, local_path, destination):
        """
        Raise a NotImplementedError exception.
        """
        raise NotImplementedError("Snakebite doesn't implement put")

    def get(self, path, local_destination):
        """
        Use snakebite.copyToLocal, if available.

        :param path: HDFS file
        :type path: string
        :param local_destination: path on the system running Luigi
        :type local_destination: string
        """
        return list(self.get_bite().copyToLocal(self.list_path(path),
                                                local_destination))

    def mkdir(self, path, parents=True, mode=0o755, raise_if_exists=False):
        """
        Use snakebite.mkdir, if available.

        Snakebite's mkdir method allows control over full path creation, so by
        default, tell it to build a full path to work like ``hadoop fs -mkdir``.

        :param path: HDFS path to create
        :type path: string
        :param parents: create any missing parent directories
        :type parents: boolean, default is True
        :param mode: \*nix style owner/group/other permissions
        :type mode: octal, default 0755
        """
        result = list(self.get_bite().mkdir(self.list_path(path),
                                            create_parent=parents, mode=mode))
        if raise_if_exists and "ile exists" in result[0].get('error', ''):
            raise luigi.target.FileAlreadyExists("%s exists" % (path, ))
        return result

    def listdir(self, path, ignore_directories=False, ignore_files=False,
                include_size=False, include_type=False, include_time=False,
                recursive=False):
        """
        Use snakebite.ls to get the list of items in a directory.

        :param path: the directory to list
        :type path: string
        :param ignore_directories: if True, do not yield directory entries
        :type ignore_directories: boolean, default is False
        :param ignore_files: if True, do not yield file entries
        :type ignore_files: boolean, default is False
        :param include_size: include the size in bytes of the current item
        :type include_size: boolean, default is False (do not include)
        :param include_type: include the type (d or f) of the current item
        :type include_type: boolean, default is False (do not include)
        :param include_time: include the last modification time of the current item
        :type include_time: boolean, default is False (do not include)
        :param recursive: list subdirectory contents
        :type recursive: boolean, default is False (do not recurse)
        :return: yield with a string, or if any of the include_* settings are
            true, a tuple starting with the path, and include_* items in order
        """
        bite = self.get_bite()
        for entry in bite.ls(self.list_path(path), recurse=recursive):
            if ignore_directories and entry['file_type'] == 'd':
                continue
            if ignore_files and entry['file_type'] == 'f':
                continue
            rval = [entry['path'], ]
            if include_size:
                rval.append(entry['length'])
            if include_type:
                rval.append(entry['file_type'])
            if include_time:
                rval.append(datetime.datetime.fromtimestamp(entry['modification_time'] / 1000))
            if len(rval) > 1:
                yield tuple(rval)
            else:
                yield rval[0]

    def touchz(self, path):
        """
        Raise a NotImplementedError exception.
        """
        raise NotImplementedError("SnakebiteClient in luigi doesn't implement touchz")
