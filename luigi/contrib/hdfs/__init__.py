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
Provides access to HDFS using the :py:class:`HdfsTarget`, a subclass of :py:class:`~luigi.target.Target`.
You can configure what client by setting the "client" config under the "hdfs" section in the configuration, or using the ``--hdfs-client`` command line option.
"hadoopcli" is the slowest, but should work out of the box. "snakebite" is the fastest, but requires Snakebite to be installed.
"""

# Here follows a series of deprecated imports that have been moved out there own files.

# clients.py
from luigi.contrib.hdfs import clients as hdfs_clients
HDFSCliError = hdfs_clients.HDFSCliError
call_check = hdfs_clients.call_check
list_path = hdfs_clients.list_path
HdfsClient = hdfs_clients.HdfsClient
SnakebiteHdfsClient = hdfs_clients.SnakebiteHdfsClient
HdfsClientCdh3 = hdfs_clients.HdfsClientCdh3
HdfsClientApache1 = hdfs_clients.HdfsClientApache1
create_hadoopcli_client = hdfs_clients.create_hadoopcli_client
get_autoconfig_client = hdfs_clients.get_autoconfig_client
client = hdfs_clients.client
exists = hdfs_clients.exists
rename = hdfs_clients.rename
remove = hdfs_clients.remove
mkdir = hdfs_clients.mkdir
listdir = hdfs_clients.listdir


# format.py
from luigi.contrib.hdfs import format as hdfs_format

HdfsReadPipe = hdfs_format.HdfsReadPipe
HdfsAtomicWritePipe = hdfs_format.HdfsAtomicWritePipe
HdfsAtomicWriteDirPipe = hdfs_format.HdfsAtomicWriteDirPipe
PlainFormat = hdfs_format.PlainFormat
PlainDirFormat = hdfs_format.PlainDirFormat
Plain = hdfs_format.Plain
PlainDir = hdfs_format.PlainDir
CompatibleHdfsFormat = hdfs_format.CompatibleHdfsFormat


# target.py
from luigi.contrib.hdfs import target as hdfs_target
HdfsTarget = hdfs_target.HdfsTarget
