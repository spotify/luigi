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


class MockFile(target.Target):
    _file_contents = {}

    def __init__(self, fn, is_tmp=None, mirror_on_stderr=False):
        self._mirror_on_stderr = mirror_on_stderr
        self._fn = fn

    def exists(self,):
        return self._fn in MockFile._file_contents

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
                    MockFile._file_contents[fn] = self2.getvalue()
                StringIO.StringIO.close(self2)

        if mode == 'w':
            return StringBuffer()
        else:
            return StringBuffer(MockFile._file_contents[fn])
