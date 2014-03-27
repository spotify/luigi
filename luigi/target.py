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
import logging
logger = logging.getLogger('luigi-interface')


class Target(object):
    """A Target is a resource generated by a :py:class:`~luigi.Task`.

    For example, a Target might correspond to a file in HDFS or data in a database. The Target
    interface defines one method that must be overridden: :py:meth:`exists`, which signifies if the
    Target has been created or not.

    Typically, a :py:class:`~luigi.Task` will define one or more Targets as output, and the Task
    is considered complete if and only if each of its output Targets exist.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def exists(self):
        """Returns ``True`` if the :py:class:`Target` exists and ``False`` otherwise.
        """
        pass


class FileSystemException(Exception):
    """Base class for generic file system exceptions. """
    pass


class FileAlreadyExists(FileSystemException):
    """Raised when a file system operation can't be performed because a directory exists but is
    required to not exist.
    """
    pass


class FileSystem(object):
    """FileSystem abstraction used in conjunction with :py:class:`FileSystemTarget`.

    Typically, a FileSystem is associated with instances of a :py:class:`FileSystemTarget`. The
    instances of the py:class:`FileSystemTarget` will delegate methods such as
    :py:meth:`FileSystemTarget.exists` and :py:meth:`FileSystemTarget.remove` to the FileSystem.

    Methods of FileSystem raise :py:class:`FileSystemException` if there is a problem completing the
    operation.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def exists(self, path):
        """ Return ``True`` if file or directory at ``path`` exist, ``False`` otherwise

        :param str path: a path within the FileSystem to check for existence.
        """
        pass

    @abc.abstractmethod
    def remove(self, path, recursive=True):
        """ Remove file or directory at location ``path``

        :param str path: a path within the FileSystem to remove.
        :param bool recursive: if the path is a directory, recursively remove the directory and all
                               of its descendants. Defaults to ``True``.
        """
        pass

    def mkdir(self, path):
        """ Create directory at location ``path``

        Creates the directory at ``path`` and implicitly create parent directories if they do not
        already exist.

        :param str path: a path within the FileSystem to create as a directory.

        *Note*: This method is optional, not all FileSystem subclasses implements it.

        """
        raise NotImplementedError("mkdir() not implemented on {0}".format(self.__class__.__name__))

    def isdir(self, path):
        """Return ``True`` if the location at ``path`` is a directory. If not, return ``False``.

        :param str path: a path within the FileSystem to check as a directory.

        *Note*: This method is optional, not all FileSystem subclasses implements it.
        """
        raise NotImplementedError("isdir() not implemented on {0}".format(self.__class__.__name__))


class FileSystemTarget(Target):
    """Base class for FileSystem Targets like LocalTarget and HdfsTarget.

    A FileSystemTarget has an associated :py:class:`FileSystem` to which certain operations can be
    delegated. By default, :py:meth:`exists` and :py:meth:`remove` are delegated to the
    :py:class:`FileSystem`, which is determined by the :py:meth:`fs` property.

    Methods of FileSystemTarget raise :py:class:`FileSystemException` if there is a problem
    completing the operation.
    """

    def __init__(self, path):
        """
        :param str path: the path associated with this FileSystemTarget.
        """
        self.path = path

    @abc.abstractproperty
    def fs(self):
        """The :py:class:`FileSystem` associated with this FileSystemTarget."""
        raise

    @abc.abstractmethod
    def open(self, mode):
        """Open the FileSystem target.

        This method returns a file-like object which can either be read from or written to depending
        on the specified mode.

        :param str mode: the mode `r` opens the FileSystemTarget in read-only mode, whereas `w` will
                         open the FileSystemTarget in write mode. Subclasses can implement
                         additional options.
        """
        pass

    def exists(self):
        """Returns ``True`` if the path for this FileSystemTarget exists and ``False`` otherwise.

        This method is implemented by using :py:meth:`fs`.
        """
        path = self.path
        if FileSystemTarget.exists.__func__ == self.exists.__func__: # supress warning when called from derived class with super
            if '*' in path or '?' in path or '[' in path or '{' in path:
                logger.warning("Using wildcards in path %s might lead to processing of an incomplete dataset; "
                               "override exists() to suppress the warning." % path)
        return self.fs.exists(path)

    def remove(self):
        """Remove the resource at the path specified by this FileSystemTarget.

        This method is implemented by using :py:meth:`fs`.
        """
        self.fs.remove(self.path)
