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
This moduel provides a class :class:`MockTarget`, an implementation of :py:class:`~luigi.target.Target`.
:class:`MockTarget` contains all data in-memory.
The main purpose is unit testing workflows without writing to disk.
"""

import multiprocessing
from io import BytesIO

import sys
import warnings

from luigi import six
import luigi.util
from luigi import target
from luigi.format import get_default_format, MixedUnicodeBytes


class MockFileSystem(target.FileSystem):
    """
    MockFileSystem inspects/modifies _data to simulate file system operations.
    """
    _data = None

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

    def listdir(self, path):
        """
        listdir does a prefix match of self.get_all_data(), but doesn't yet support globs.
        """
        return [s for s in self.get_all_data().keys()
                if s.startswith(path)]

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
        self._fn = fn
        if format is None:
            format = get_default_format()

        # Allow to write unicode in file for retrocompatibility
        if six.PY2:
            format = format >> MixedUnicodeBytes

        self.format = format

    def exists(self,):
        return self._fn in self.fs.get_all_data()

    def rename(self, path, raise_if_exists=False):
        if raise_if_exists and path in self.fs.get_all_data():
            raise RuntimeError('Destination exists: %s' % path)
        contents = self.fs.get_all_data().pop(self._fn)
        self.fs.get_all_data()[path] = contents

    @property
    def path(self):
        return self._fn

    def open(self, mode):
        fn = self._fn

        class Buffer(BytesIO):
            # Just to be able to do writing + reading from the same buffer

            _write_line = True

            def set_wrapper(self, wrapper):
                self.wrapper = wrapper

            def write(self2, data):
                if six.PY3:
                    stderrbytes = sys.stderr.buffer
                else:
                    stderrbytes = sys.stderr

                if self._mirror_on_stderr:
                    if self2._write_line:
                        sys.stderr.write(fn + ": ")
                    stderrbytes.write(data)
                    if (data[-1]) == '\n':
                        self2._write_line = True
                    else:
                        self2._write_line = False
                super(Buffer, self2).write(data)

            def close(self2):
                if mode == 'w':
                    try:
                        self.wrapper.flush()
                    except AttributeError:
                        pass
                    self.fs.get_all_data()[fn] = self2.getvalue()
                super(Buffer, self2).close()

            def __exit__(self, exc_type, exc_val, exc_tb):
                if not exc_type:
                    self.close()

            def __enter__(self2):
                return self2

            def readable(self2):
                return mode == 'r'

            def writeable(self2):
                return mode == 'w'

            def seekable(self2):
                return False

        if mode == 'w':
            wrapper = self.format.pipe_writer(Buffer())
            wrapper.set_wrapper(wrapper)
            return wrapper
        else:
            return self.format.pipe_reader(Buffer(self.fs.get_all_data()[fn]))


class MockFile(MockTarget):
    def __init__(self, *args, **kwargs):
        warnings.warn("MockFile has been renamed MockTarget", DeprecationWarning, stacklevel=2)
        super(MockFile, self).__init__(*args, **kwargs)
