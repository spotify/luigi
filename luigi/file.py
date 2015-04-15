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
:class:`LocalTarget` provides a concrete implementation of a :py:class:`~luigi.target.Target` class that uses files on the local file system
"""

import os
import random
import shutil
import tempfile
import io
import sys
import warnings

import luigi.util
from luigi.format import FileWrapper, get_default_format, MixedUnicodeBytes
from luigi.target import FileSystem, FileSystemTarget, AtomicLocalFile


class atomic_file(AtomicLocalFile):
    """Simple class that writes to a temp file and moves it on close()
    Also cleans up the temp file if close is not invoked
    """

    def move_to_final_destination(self):
        os.rename(self.tmp_path, self.path)

    def generate_tmp_path(self, path):
        return path + '-luigi-tmp-%09d' % random.randrange(0, 1e10)


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


class LocalTarget(FileSystemTarget):
    fs = LocalFileSystem()

    def __init__(self, path=None, format=None, is_tmp=False):
        if format is None:
            format = get_default_format()

        # Allow to write unicode in file for retrocompatibility
        if sys.version_info[:2] <= (2, 6):
            format = format >> MixedUnicodeBytes

        if not path:
            if not is_tmp:
                raise Exception('path or is_tmp must be set')
            path = os.path.join(tempfile.gettempdir(), 'luigi-tmp-%09d' % random.randint(0, 999999999))
        super(LocalTarget, self).__init__(path)
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
            return self.format.pipe_writer(atomic_file(self.path))

        elif mode == 'r':
            fileobj = FileWrapper(io.BufferedReader(io.FileIO(self.path, 'r')))
            return self.format.pipe_reader(fileobj)

        else:
            raise Exception('mode must be r/w')

    def move(self, new_path, raise_if_exists=False):
        if raise_if_exists and os.path.exists(new_path):
            raise RuntimeError('Destination exists: %s' % new_path)
        d = os.path.dirname(new_path)
        if d and not os.path.exists(d):
            self.fs.mkdir(d)
        os.rename(self.path, new_path)

    def move_dir(self, new_path):
        self.move(new_path)

    def remove(self):
        self.fs.remove(self.path)

    def copy(self, new_path, raise_if_exists=False):
        if raise_if_exists and os.path.exists(new_path):
            raise RuntimeError('Destination exists: %s' % new_path)
        tmp = LocalTarget(new_path + '-luigi-tmp-%09d' % random.randrange(0, 1e10), is_tmp=True)
        tmp.makedirs()
        shutil.copy(self.path, tmp.fn)
        tmp.move(new_path)

    @property
    def fn(self):
        return self.path

    def __del__(self):
        if self.is_tmp and self.exists():
            self.remove()


class File(LocalTarget):
    def __init__(self, *args, **kwargs):
        warnings.warn("File has been renamed LocalTarget", DeprecationWarning, stacklevel=2)
        super(File, self).__init__(*args, **kwargs)
