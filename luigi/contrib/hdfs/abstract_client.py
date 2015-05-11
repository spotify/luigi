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
Module containing abstract class about hdfs clients.
"""

import abc
from luigi import six
import luigi.target
import warnings


@six.add_metaclass(abc.ABCMeta)
class HdfsFileSystem(luigi.target.FileSystem):
    """
    This client uses Apache 2.x syntax for file system commands, which also matched CDH4.
    """

    @abc.abstractmethod
    def rename(self, path, dest):
        """
        Rename or move a file
        """
        pass

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

    @abc.abstractmethod
    def remove(self, path, recursive=True, skip_trash=False):
        pass

    @abc.abstractmethod
    def chmod(self, path, permissions, recursive=False):
        pass

    @abc.abstractmethod
    def chown(self, path, owner, group, recursive=False):
        pass

    @abc.abstractmethod
    def count(self, path):
        """
        Count contents in a directory
        """
        pass

    @abc.abstractmethod
    def copy(self, path, destination):
        pass

    @abc.abstractmethod
    def put(self, local_path, destination):
        pass

    @abc.abstractmethod
    def get(self, path, local_destination):
        pass

    @abc.abstractmethod
    def mkdir(self, path, parents=True, raise_if_exists=False):
        pass

    @abc.abstractmethod
    def listdir(self, path, ignore_directories=False, ignore_files=False,
                include_size=False, include_type=False, include_time=False, recursive=False):
        pass

    @abc.abstractmethod
    def touchz(self, path):
        pass
