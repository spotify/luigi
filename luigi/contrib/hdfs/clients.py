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
The implementations of the hdfs clients. The hadoop cli client and the
snakebite client.
"""


from luigi.target import FileAlreadyExists, FileSystem
from luigi.contrib.hdfs.config import load_hadoop_cmd
from luigi.contrib.hdfs import config as hdfs_config
from luigi import six
import luigi.contrib.target
import logging
import subprocess
import datetime
import os
import re
import warnings

logger = logging.getLogger('luigi-interface')


class HDFSCliError(Exception):

    def __init__(self, command, returncode, stdout, stderr):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        msg = ("Command %r failed [exit code %d]\n" +
               "---stdout---\n" +
               "%s\n" +
               "---stderr---\n" +
               "%s" +
               "------------") % (command, returncode, stdout, stderr)
        super(HDFSCliError, self).__init__(msg)


class HdfsClient(FileSystem):
    """
    This client uses Apache 2.x syntax for file system commands, which also matched CDH4.
    """

    recursive_listdir_cmd = ['-ls', '-R']

    @staticmethod
    def call_check(command):
        p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True, universal_newlines=True)
        stdout, stderr = p.communicate()
        if p.returncode != 0:
            raise HDFSCliError(command, p.returncode, stdout, stderr)
        return stdout

    def exists(self, path):
        """
        Use ``hadoop fs -stat`` to check file existence.
        """

        cmd = load_hadoop_cmd() + ['fs', '-stat', path]
        logger.debug('Running file existence check: %s', u' '.join(cmd))
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True, universal_newlines=True)
        stdout, stderr = p.communicate()
        if p.returncode == 0:
            return True
        else:
            not_found_pattern = "^.*No such file or directory$"
            not_found_re = re.compile(not_found_pattern)
            for line in stderr.split('\n'):
                if not_found_re.match(line):
                    return False
            raise HDFSCliError(cmd, p.returncode, stdout, stderr)

    def rename(self, path, dest):
        parent_dir = os.path.dirname(dest)
        if parent_dir != '' and not self.exists(parent_dir):
            self.mkdir(parent_dir)
        if type(path) not in (list, tuple):
            path = [path]
        else:
            warnings.warn("Renaming multiple files at once is not atomic.")
        self.call_check(load_hadoop_cmd() + ['fs', '-mv'] + path + [dest])

    def rename_dont_move(self, path, dest):
        """
        Override this method with an implementation that uses rename2,
        which is a rename operation that never moves.

        For instance, `rename2 a b` never moves `a` into `b` folder.

        Currently, the hadoop cli does not support this operation.

        We keep the interface simple by just aliasing this to
        normal rename and let individual implementations redefine the method.

        rename2 -
        https://github.com/apache/hadoop/blob/ae91b13/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/protocol/ClientProtocol.java
        (lines 483-523)
        """
        warnings.warn("Configured HDFS client doesn't support rename_dont_move, using normal mv operation instead.")
        if self.exists(dest):
            return False
        self.rename(path, dest)
        return True

    def remove(self, path, recursive=True, skip_trash=False):
        if recursive:
            cmd = load_hadoop_cmd() + ['fs', '-rm', '-r']
        else:
            cmd = load_hadoop_cmd() + ['fs', '-rm']

        if skip_trash:
            cmd = cmd + ['-skipTrash']

        cmd = cmd + [path]
        self.call_check(cmd)

    def chmod(self, path, permissions, recursive=False):
        if recursive:
            cmd = load_hadoop_cmd() + ['fs', '-chmod', '-R', permissions, path]
        else:
            cmd = load_hadoop_cmd() + ['fs', '-chmod', permissions, path]
        self.call_check(cmd)

    def chown(self, path, owner, group, recursive=False):
        if owner is None:
            owner = ''
        if group is None:
            group = ''
        ownership = "%s:%s" % (owner, group)
        if recursive:
            cmd = load_hadoop_cmd() + ['fs', '-chown', '-R', ownership, path]
        else:
            cmd = load_hadoop_cmd() + ['fs', '-chown', ownership, path]
        self.call_check(cmd)

    def count(self, path):
        cmd = load_hadoop_cmd() + ['fs', '-count', path]
        stdout = self.call_check(cmd)
        lines = stdout.split('\n')
        for line in stdout.split('\n'):
            if line.startswith("OpenJDK 64-Bit Server VM warning") or line.startswith("It's highly recommended") or not line:
                lines.pop(lines.index(line))
            else:
                (dir_count, file_count, content_size, ppath) = stdout.split()
        results = {'content_size': content_size, 'dir_count': dir_count, 'file_count': file_count}
        return results

    def copy(self, path, destination):
        self.call_check(load_hadoop_cmd() + ['fs', '-cp', path, destination])

    def put(self, local_path, destination):
        self.call_check(load_hadoop_cmd() + ['fs', '-put', local_path, destination])

    def get(self, path, local_destination):
        self.call_check(load_hadoop_cmd() + ['fs', '-get', path, local_destination])

    def getmerge(self, path, local_destination, new_line=False):
        if new_line:
            cmd = load_hadoop_cmd() + ['fs', '-getmerge', '-nl', path, local_destination]
        else:
            cmd = load_hadoop_cmd() + ['fs', '-getmerge', path, local_destination]
        self.call_check(cmd)

    def mkdir(self, path, parents=True, raise_if_exists=False):
        if (parents and raise_if_exists):
            raise NotImplementedError("HdfsClient.mkdir can't raise with -p")
        try:
            cmd = (load_hadoop_cmd() + ['fs', '-mkdir'] +
                   (['-p'] if parents else []) +
                   [path])
            self.call_check(cmd)
        except HDFSCliError as ex:
            if "File exists" in ex.stderr:
                if raise_if_exists:
                    raise FileAlreadyExists(ex.stderr)
            else:
                raise

    def listdir(self, path, ignore_directories=False, ignore_files=False,
                include_size=False, include_type=False, include_time=False, recursive=False):
        if not path:
            path = "."  # default to current/home catalog

        if recursive:
            cmd = load_hadoop_cmd() + ['fs'] + self.recursive_listdir_cmd + [path]
        else:
            cmd = load_hadoop_cmd() + ['fs', '-ls', path]
        lines = self.call_check(cmd).split('\n')

        for line in lines:
            if not line:
                continue
            elif line.startswith('OpenJDK 64-Bit Server VM warning') or line.startswith('It\'s highly recommended') or line.startswith('Found'):
                continue  # "hadoop fs -ls" outputs "Found %d items" as its first line
            elif ignore_directories and line[0] == 'd':
                continue
            elif ignore_files and line[0] == '-':
                continue
            data = line.split(' ')

            file = data[-1]
            size = int(data[-4])
            line_type = line[0]
            extra_data = ()

            if include_size:
                extra_data += (size,)
            if include_type:
                extra_data += (line_type,)
            if include_time:
                time_str = '%sT%s' % (data[-3], data[-2])
                modification_time = datetime.datetime.strptime(time_str,
                                                               '%Y-%m-%dT%H:%M')
                extra_data += (modification_time,)

            if len(extra_data) > 0:
                yield (file,) + extra_data
            else:
                yield file

    def touchz(self, path):
        self.call_check(load_hadoop_cmd() + ['fs', '-touchz', path])


class SnakebiteHdfsClient(HdfsClient):
    """
    This client uses Spotify's snakebite client whenever possible.

    @author: Alan Brenner <alan@magnetic.com> github.com/alanbbr
    """

    def __init__(self):
        super(SnakebiteHdfsClient, self).__init__()
        try:
            from snakebite.client import Client
            self._bite = None
            self.pid = -1
        except Exception as err:    # IGNORE:broad-except
            raise RuntimeError("You must specify namenode_host and namenode_port "
                               "in the [hdfs] section of your luigi config in "
                               "order to use luigi's snakebite support", err)

    def __new__(cls):
        try:
            from snakebite.client import Client
            this = super(SnakebiteHdfsClient, cls).__new__(cls)
            return this
        except ImportError:
            logger.warning("Failed to load snakebite.client. Using HdfsClient.")
            return HdfsClient()

    @staticmethod
    def list_path(path):
        if isinstance(path, list) or isinstance(path, tuple):
            return path
        if isinstance(path, str) or isinstance(path, unicode):
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
            raise HDFSCliError("snakebite.test", -1, str(err), repr(err))

    def rename(self, path, dest):
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
            return True
        except FileAlreadyExistsException:
            return False

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


class HdfsClientCdh3(HdfsClient):
    """
    This client uses CDH3 syntax for file system commands.
    """

    def mkdir(self, path):
        """
        No -p switch, so this will fail creating ancestors.
        """
        try:
            self.call_check(load_hadoop_cmd() + ['fs', '-mkdir', path])
        except HDFSCliError as ex:
            if "File exists" in ex.stderr:
                raise FileAlreadyExists(ex.stderr)
            else:
                raise

    def remove(self, path, recursive=True, skip_trash=False):
        if recursive:
            cmd = load_hadoop_cmd() + ['fs', '-rmr']
        else:
            cmd = load_hadoop_cmd() + ['fs', '-rm']

        if skip_trash:
            cmd = cmd + ['-skipTrash']

        cmd = cmd + [path]
        self.call_check(cmd)


class HdfsClientApache1(HdfsClientCdh3):
    """
    This client uses Apache 1.x syntax for file system commands,
    which are similar to CDH3 except for the file existence check.
    """

    recursive_listdir_cmd = ['-lsr']

    def exists(self, path):
        cmd = load_hadoop_cmd() + ['fs', '-test', '-e', path]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
        stdout, stderr = p.communicate()
        if p.returncode == 0:
            return True
        elif p.returncode == 1:
            return False
        else:
            raise HDFSCliError(cmd, p.returncode, stdout, stderr)


def create_hadoopcli_client():
    """
    Given that we want one of the hadoop cli clients (unlike snakebite),
    this one will return the right one.
    """
    version = hdfs_config.get_configured_hadoop_version()
    if version == "cdh4":
        return HdfsClient()
    elif version == "cdh3":
        return HdfsClientCdh3()
    elif version == "apache1":
        return HdfsClientApache1()
    else:
        raise Exception("Error: Unknown version specified in Hadoop version"
                        "configuration parameter")


def get_autoconfig_client(show_warnings=True):
    """
    Creates the client as specified in the `client.cfg` configuration.
    """
    configured_client = hdfs_config.get_configured_hdfs_client(show_warnings=show_warnings)
    if configured_client == "snakebite":
        return SnakebiteHdfsClient()
    if configured_client == "snakebite_with_hadoopcli_fallback":
        return luigi.contrib.target.CascadingClient([SnakebiteHdfsClient(),
                                                     create_hadoopcli_client()])
    if configured_client == "hadoopcli":
        return create_hadoopcli_client()
    raise Exception("Unknown hdfs client " + hdfs_config.get_configured_hdfs_client())

# Suppress warnings so that importing luigi.contrib.hdfs doesn't show a deprecated warning.
client = get_autoconfig_client(show_warnings=False)
exists = client.exists
rename = client.rename
remove = client.remove
mkdir = client.mkdir
listdir = client.listdir
