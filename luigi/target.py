# Copyright (c) 2012 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import abc


class Target(object):  # interface
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def exists(self):
        pass


class FileSystemException(Exception):
    """Base class for generic file system exceptions """
    pass


class FileAlreadyExists(FileSystemException):
    """ Raised when a file system operation can't be performed because a direcoty exists but is required to not exist
    """
    pass


class FileSystem(object):
    """ File system abstraction class """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def exists(self, path):
        """ Return `True` if file or directory at `path` exist, False otherwise """
        pass

    @abc.abstractmethod
    def remove(self, path, recursive=True):
        """ Remove file or directory at location `path` """
        pass

    def mkdir(self, path):
        """ Create directory at location `path`
        Create parent catalogs if they don't exist

        Not an abstract method, since not all File System-like storage systems support mkdir
        """
        raise NotImplementedError("mkdir() not implemented on {0}".format(self.__class__.__name__))

    def isdir(self, path):
        raise NotImplementedError("isdir() not implemented on {0}".format(self.__class__.__name__))


class FileSystemTarget(Target):
    """Common target abstract base class for file system targets like LocalTarget and HdfsTarget
    """
    def __init__(self, path):
        self.path = path

    @abc.abstractproperty
    def fs(self):
        raise

    @abc.abstractmethod
    def open(self, mode):
        pass

    def exists(self):
        return self.fs.exists(self.path)

    def remove(self):
        self.fs.remove(self.path)
