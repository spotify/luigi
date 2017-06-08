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
Implementation of Simple Storage Service support.
:py:class:`S3Target` is a subclass of the Target class to support S3 file
system operations. The `boto` library is required to use S3 targets.
"""

from __future__ import division

import datetime
import itertools
import logging
import os
import os.path

import time
from multiprocessing.pool import ThreadPool

try:
    from urlparse import urlsplit
except ImportError:
    from urllib.parse import urlsplit
import warnings

try:
    from ConfigParser import NoSectionError
except ImportError:
    from configparser import NoSectionError

from luigi import six
from luigi.six.moves import range

from luigi import configuration
from luigi.format import get_default_format
from luigi.parameter import Parameter
from luigi.target import FileAlreadyExists, FileSystem, FileSystemException, FileSystemTarget, AtomicLocalFile, MissingParentDirectory
from luigi.task import ExternalTask

logger = logging.getLogger('luigi-interface')


# two different ways of marking a directory
# with a suffix in S3
AZ_DIRECTORY_MARKER_SUFFIX_0 = '_$folder$'
AZ_DIRECTORY_MARKER_SUFFIX_1 = '/'


class InvalidDeleteException(FileSystemException):
    pass


class FileNotFoundException(FileSystemException):
    pass


class AzureStorageClient(FileSystem):
    """
    Microsoft Azure BlobStorage client.
    """

    _az = None

    def __init__(self, az_storage_account_name=None, az_storage_account_key=None,
                 **kwargs):
        options = self._get_s3_config()
        options.update(kwargs)
        if aws_access_key_id:
            options['az_storage_account_name'] = az_storage_account_name
        if aws_secret_access_key:
            options['az_storage_account_key'] = az_storage_account_key

        self._options = options
        
    def _get_az_config(self, key=None):
        try:
            config = dict(configuration.get_config().items('azure'))
        except NoSectionError:
            return {}
        # So what ports etc can be read without us having to specify all dtypes
        for k, v in six.iteritems(config):
            try:
                config[k] = int(v)
            except ValueError:
                pass
        if key:
            return config.get(key)
        return config
