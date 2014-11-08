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

import StringIO
import target
import sys
import os
import luigi.util
import multiprocessing


class MockFileSystem(target.FileSystem):
    """MockFileSystem inspects/modifies _data to simulate
    file system operations"""
    _data = None

    def get_all_data(self):
        # This starts a server in the background, so we don't want to do it in the global scope
        if MockFileSystem._data is None:
            MockFileSystem._data = multiprocessing.Manager().dict()
        return MockFileSystem._data

    def get_data(self, fn):
        return self.get_all_data()[fn]

    def exists(self, path):
        return MockFile(path).exists()

    def remove(self, path, recursive=True, skip_trash=True):
        """Removes the given mockfile. skip_trash doesn't have any meaning."""
        if recursive:
            to_delete=[]
            for s in self.get_all_data().keys():
                if s.startswith(path):
                    to_delete.append(s)
            for s in to_delete:
                self.get_all_data().pop(s)
        else:
            self.get_all_data().pop(path)

    def listdir(self, path):
        """listdir does a prefix match of self.get_all_data(), but
        doesn't yet support globs"""
        return [s for s in self.get_all_data().keys()
                if s.startswith(path)]

    def mkdir(self, path):
        """mkdir is a noop"""
        pass

    def clear(self):
        self.get_all_data().clear()


class MockFile(target.FileSystemTarget):
    fs = MockFileSystem()

    def __init__(self, fn, is_tmp=None, mirror_on_stderr=False):
        self._mirror_on_stderr = mirror_on_stderr
        self._fn = fn

    def exists(self,):
        return self._fn in self.fs.get_all_data()

    @luigi.util.deprecate_kwarg('fail_if_exists', 'raise_if_exists', False)
    def rename(self, path, fail_if_exists=False):
        if fail_if_exists and path in self.fs.get_all_data():
            raise RuntimeError('Destination exists: %s' % path)
        contents = self.fs.get_all_data().pop(self._fn)
        self.fs.get_all_data()[path] = contents

    def move_dir(self, path):
        self.move(path, raise_if_exists=True)

    @property
    def path(self):
        return self._fn

    def open(self, mode):
        fn = self._fn

        class StringBuffer(StringIO.StringIO):
            # Just to be able to do writing + reading from the same buffer
            def write(self2, data):
                if self._mirror_on_stderr:
                    self2.seek(-1, os.SEEK_END)
                    if self2.tell() <= 0 or self2.read(1) == '\n':
                        sys.stderr.write(fn + ": ")
                    sys.stderr.write(data)
                StringIO.StringIO.write(self2, data)

            def close(self2):
                if mode == 'w':
                    self.fs.get_all_data()[fn] = self2.getvalue()
                StringIO.StringIO.close(self2)

            def __exit__(self, type, value, traceback):
                if not type:
                    self.close()

            def __enter__(self):
                return self

        if mode == 'w':
            return StringBuffer()
        else:
            return StringBuffer(self.fs.get_all_data()[fn])


def skip(func):
    """ Sort of a substitute for unittest.skip*, which is 2.7+ """
    def wrapper():
        pass
    return wrapper
