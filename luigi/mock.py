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
This module provides a class :class:`MockTarget`, an implementation of :py:class:`~luigi.target.Target`.
:class:`MockTarget` contains all data in-memory.
The main purpose is unit testing workflows without writing to disk.
"""

import multiprocessing
from io import BytesIO

import sys

from luigi import target
from luigi.format import get_default_format


class MockFileSystem(target.FileSystem):
    """
    MockFileSystem inspects/modifies _data to simulate file system operations.
    """
    _data = None

    def copy(self, path, dest, raise_if_exists=False):
        """
        Copies the contents of a single file path to dest
        """
        if raise_if_exists and dest in self.get_all_data():
            raise RuntimeError('Destination exists: %s' % path)
        contents = self.get_all_data()[path]
        self.get_all_data()[dest] = contents

    def get_all_data(self):
        # This starts a server in the background, so we don't want to do it in the global scope
        if MockFileSystem._data is None:
            MockFileSystem._data = multiprocessing.Manager().dict()
        return MockFileSystem._data

    def get_data(self, fn):
        return self.get_all_data()[fn]

    def exists(self, path):
        return MockTarget(path).exists()

    def remove(self, path, recursive=True, skip_trash=True):
        """
        Removes the given mockfile. skip_trash doesn't have any meaning.
        """
        if recursive:
            to_delete = []
            for s in self.get_all_data().keys():
                if s.startswith(path):
                    to_delete.append(s)
            for s in to_delete:
                self.get_all_data().pop(s)
        else:
            self.get_all_data().pop(path)

    def move(self, path, dest, raise_if_exists=False):
        """
        Moves a single file from path to dest
        """
        if raise_if_exists and dest in self.get_all_data():
            raise RuntimeError('Destination exists: %s' % path)
        contents = self.get_all_data().pop(path)
        self.get_all_data()[dest] = contents

    def listdir(self, path):
        """
        listdir does a prefix match of self.get_all_data(), but doesn't yet support globs.
        """
        return [s for s in self.get_all_data().keys()
                if s.startswith(path)]

    def isdir(self, path):
        return any(self.listdir(path))

    def mkdir(self, path, parents=True, raise_if_exists=False):
        """
        mkdir is a noop.
        """
        pass

    def clear(self):
        self.get_all_data().clear()


class MockTarget(target.FileSystemTarget):
    fs = MockFileSystem()

    def __init__(self, fn, is_tmp=None, mirror_on_stderr=False, format=None):
        self._mirror_on_stderr = mirror_on_stderr
        self.path = fn
        self.format = format or get_default_format()

    def exists(self,):
        return self.path in self.fs.get_all_data()

    def move(self, path, raise_if_exists=False):
        """
        Call MockFileSystem's move command
        """
        self.fs.move(self.path, path, raise_if_exists)

    def rename(self, *args, **kwargs):
        """
        Call move to rename self
        """
        self.move(*args, **kwargs)

    def open(self, mode='r'):
        fn = self.path
        mock_target = self

        class Buffer(BytesIO):
            # Just to be able to do writing + reading from the same buffer

            _write_line = True

            def set_wrapper(self, wrapper):
                self.wrapper = wrapper

            def write(self, data):
                if mock_target._mirror_on_stderr:
                    if self._write_line:
                        sys.stderr.write(fn + ": ")
                    if bytes:
                        sys.stderr.write(data.decode('utf8'))
                    else:
                        sys.stderr.write(data)
                    if (data[-1]) == '\n':
                        self._write_line = True
                    else:
                        self._write_line = False
                super(Buffer, self).write(data)

            def close(self):
                if mode[0] == 'w':
                    try:
                        mock_target.wrapper.flush()
                    except AttributeError:
                        pass
                    mock_target.fs.get_all_data()[fn] = self.getvalue()
                super(Buffer, self).close()

            def __exit__(self, exc_type, exc_val, exc_tb):
                if not exc_type:
                    self.close()

            def __enter__(self):
                return self

            def readable(self):
                return mode[0] == 'r'

            def writeable(self):
                return mode[0] == 'w'

            def seekable(self):
                return False

        if mode[0] == 'w':
            wrapper = self.format.pipe_writer(Buffer())
            wrapper.set_wrapper(wrapper)
            return wrapper
        else:
            return self.format.pipe_reader(Buffer(self.fs.get_all_data()[fn]))
