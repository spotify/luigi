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

import os
import random
import shutil
import tempfile

import luigi.util
from luigi.format import FileWrapper
from target import FileSystem, FileSystemTarget


class atomic_file(file):
    # Simple class that writes to a temp file and moves it on close()
    # Also cleans up the temp file if close is not invoked

    def __init__(self, path):
        self.__tmp_path = path + '-luigi-tmp-%09d' % random.randrange(0, 1e10)
        self.path = path
        super(atomic_file, self).__init__(self.__tmp_path, 'w')

    def close(self):
        super(atomic_file, self).close()
        os.rename(self.__tmp_path, self.path)

    def __del__(self):
        if os.path.exists(self.__tmp_path):
            os.remove(self.__tmp_path)

    @property
    def tmp_path(self):
        return self.__tmp_path

    def __exit__(self, exc_type, exc, traceback):
        " Close/commit the file if there are no exception "
        if exc_type:
            return
        return file.__exit__(self, exc_type, exc, traceback)


class LocalFileSystem(FileSystem):
    """
    Wrapper for access to file system operations.

    Work in progress - add things as needed.
    """

    def exists(self, path):
        return os.path.exists(path)

    def mkdir(self, path, parents=True, raise_if_exists=False):
        os.makedirs(path)

    def isdir(self, path):
        return os.path.isdir(path)

    def remove(self, path, recursive=True):
        if recursive and self.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)


class File(FileSystemTarget):
    fs = LocalFileSystem()

    def __init__(self, path=None, format=None, is_tmp=False):
        if not path:
            if not is_tmp:
                raise Exception('path or is_tmp must be set')
            path = os.path.join(tempfile.gettempdir(), 'luigi-tmp-%09d' % random.randint(0, 999999999))
        super(File, self).__init__(path)
        self.format = format
        self.is_tmp = is_tmp

    def makedirs(self):
        """
        Create all parent folders if they do not exist.
        """
        normpath = os.path.normpath(self.path)
        parentfolder = os.path.dirname(normpath)
        if parentfolder and not os.path.exists(parentfolder):
            os.makedirs(parentfolder)

    def open(self, mode='r'):
        if mode == 'w':
            self.makedirs()
            if self.format:
                return self.format.pipe_writer(atomic_file(self.path))
            else:
                return atomic_file(self.path)

        elif mode == 'r':
            fileobj = FileWrapper(open(self.path, 'r'))
            if self.format:
                return self.format.pipe_reader(fileobj)
            return fileobj
        else:
            raise Exception('mode must be r/w')

    @luigi.util.deprecate_kwarg('fail_if_exists', 'raise_if_exists', False)
    def move(self, new_path, fail_if_exists=False):
        if fail_if_exists and os.path.exists(new_path):
            raise RuntimeError('Destination exists: %s' % new_path)
        d = os.path.dirname(new_path)
        if d and not os.path.exists(d):
            self.fs.mkdir(d)
        os.rename(self.path, new_path)

    def move_dir(self, new_path):
        self.move(new_path)

    def remove(self):
        self.fs.remove(self.path)

    @luigi.util.deprecate_kwarg('fail_if_exists', 'raise_if_exists', False)
    def copy(self, new_path, fail_if_exists=False):
        if fail_if_exists and os.path.exists(new_path):
            raise RuntimeError('Destination exists: %s' % new_path)
        tmp = File(new_path + '-luigi-tmp-%09d' % random.randrange(0, 1e10), is_tmp=True)
        tmp.makedirs()
        shutil.copy(self.path, tmp.fn)
        tmp.move(new_path)

    @property
    def fn(self):
        return self.path

    def __del__(self):
        if self.is_tmp and self.exists():
            self.remove()
