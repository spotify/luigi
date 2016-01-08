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


from luigi.target import FileAlreadyExists
from luigi.contrib.hdfs.config import load_hadoop_cmd
from luigi.contrib.hdfs import abstract_client as hdfs_abstract_client
from luigi.contrib.hdfs import config as hdfs_config
from luigi.contrib.hdfs import error as hdfs_error
import logging
import subprocess
import datetime
import os
import re
import warnings

logger = logging.getLogger('luigi-interface')


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
        raise ValueError("Error: Unknown version specified in Hadoop version"
                         "configuration parameter")


class HdfsClient(hdfs_abstract_client.HdfsFileSystem):
    """
    This client uses Apache 2.x syntax for file system commands, which also matched CDH4.
    """

    recursive_listdir_cmd = ['-ls', '-R']

    @staticmethod
    def call_check(command):
        p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True, universal_newlines=True)
        stdout, stderr = p.communicate()
        if p.returncode != 0:
            raise hdfs_error.HDFSCliError(command, p.returncode, stdout, stderr)
        return stdout

    def exists(self, path):
        """
        Use ``hadoop fs -stat`` to check file existence.
        """

        cmd = load_hadoop_cmd() + ['fs', '-stat', path]
        logger.debug('Running file existence check: %s', subprocess.list2cmdline(cmd))
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
            raise hdfs_error.HDFSCliError(cmd, p.returncode, stdout, stderr)

    def move(self, path, dest):
        parent_dir = os.path.dirname(dest)
        if parent_dir != '' and not self.exists(parent_dir):
            self.mkdir(parent_dir)
        if not isinstance(path, (list, tuple)):
            path = [path]
        else:
            warnings.warn("Renaming multiple files at once is not atomic.", stacklevel=2)
        self.call_check(load_hadoop_cmd() + ['fs', '-mv'] + path + [dest])

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
        if parents and raise_if_exists:
            raise NotImplementedError("HdfsClient.mkdir can't raise with -p")
        try:
            cmd = (load_hadoop_cmd() + ['fs', '-mkdir'] +
                   (['-p'] if parents else []) +
                   [path])
            self.call_check(cmd)
        except hdfs_error.HDFSCliError as ex:
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
        except hdfs_error.HDFSCliError as ex:
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
            raise hdfs_error.HDFSCliError(cmd, p.returncode, stdout, stderr)
