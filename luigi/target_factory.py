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

import datetime
from luigi.format import Gzip
from luigi.s3 import S3Target
from luigi.target import FileSystemTarget
from luigi import LocalTarget


def get_target(path):
    """
    Factory method to create a Luigi Target from a path string.
    Supports the following Target types:
    * S3Target: s3://my-bucket/my-path
    * LocalTarget: /path/to/file or file:///path/to/file
    :type path: str
    :param path: s3 or file URL, or local path
    :rtype: Target:
    :returns: Target for path string
    """
    file_format = None
    if path.endswith(".gz"):
        file_format = Gzip

    if path.startswith('s3:') or path.startswith('s3n:'):
        return S3Target(path, format=file_format)
    elif path.startswith('/'):
        return LocalTarget(path, format=file_format)
    elif path.startswith('file://'):
        # remove the file portion
        actual_path = path[7:]
        return LocalTarget(actual_path, format=file_format)
    else:
        raise RuntimeError("Unknown scheme for path: %s" % path)


def write_file(out_target, text=None):
    """
    Factory method to write a token file to a Luigi Target.
    :type out_target: Target
    :param out_target: Target where token file should be written
    :type text: str
    :param text: Optional text to write to token file. Default: write current UTC time.
    """
    with out_target.open('w') as token_file:
        if text:
            token_file.write('%s\n' % text)
        else:
            token_file.write('%s' % datetime.datetime.utcnow().isoformat())
