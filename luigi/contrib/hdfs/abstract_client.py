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
import luigi.target


class HdfsFileSystem(luigi.target.FileSystem, metaclass=abc.ABCMeta):
    """
    This client uses Apache 2.x syntax for file system commands, which also matched CDH4.
    """

    def rename(self, path, dest):
        """
        Rename or move a file.

        In hdfs land, "mv" is often called rename. So we add an alias for
        ``move()`` called ``rename()``. This is also to keep backward
        compatibility since ``move()`` became standardized in luigi's
        filesystem interface.
        """
        return self.move(path, dest)

    def rename_dont_move(self, path, dest):
        """
        Override this method with an implementation that uses rename2,
        which is a rename operation that never moves.

        rename2 -
        https://github.com/apache/hadoop/blob/ae91b13/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/protocol/ClientProtocol.java
        (lines 483-523)
        """
        # We only override this method to be able to provide a more specific
        # docstring.
        return super(HdfsFileSystem, self).rename_dont_move(path, dest)

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
