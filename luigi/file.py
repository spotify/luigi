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

import os
import random
import tempfile

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


class File(object):
    def __init__(self, path=None, format=None, is_tmp=False):
        if not path:
            if not is_tmp:
                raise Exception('path or is_tmp must be set')
            _, path = tempfile.mkstemp()
        self.path = path
        self.format = format
        self.is_tmp = is_tmp

    def exists(self):
        return os.path.exists(self.path)

    def open(self, mode='r'):
        if mode == 'w':
            # Create folder if it does not exist
            normpath = os.path.normpath(self.path)
            parentfolder = os.path.dirname(normpath)
            if not os.path.exists(parentfolder):
                os.makedirs(parentfolder)

            if self.format:
                return self.format.pipe_writer(atomic_file(self.path))
            else:
                return atomic_file(self.path)

        elif mode == 'r':
            if self.format:
                return self.format.pipe_reader(file(self.path))
            else:
                return open(self.path, mode)
        else:
            raise Exception('mode must be r/w')

    def move(self, new_path, fail_if_exists=False):
        if fail_if_exists and os.path.exists(new_path):
            raise RuntimeError('Destination exists: %s' % new_path)
        d = os.path.dirname(new_path)
        if not os.path.exists(d):
            os.makedirs(d)
        os.rename(self.path, new_path)

    def move_dir(self, new_path):
        self.move(new_path)

    def remove(self):
        os.remove(self.path)

    @property
    def fn(self):
        return self.path

    def __del__(self):
        if self.is_tmp and os.path.exists(self.path):
            self.remove()
