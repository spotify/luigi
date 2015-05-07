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

Currently (4th May) the :py:mod:`luigi.contrib.hdfs` module is under
reorganization. We recommend importing the reexports from
:py:mod:`luigi.contrib.hdfs` instead of the sub-modules, as we're not yet sure
how the final structure of the sub-modules will be. Eventually this module
will be empty and you'll have to import directly from the sub modules like
:py:mod:`luigi.contrib.hdfs.config`.
"""

# config.py
from luigi.contrib.hdfs import config as hdfs_config
hdfs = hdfs_config.hdfs
load_hadoop_cmd = hdfs_config.load_hadoop_cmd
get_configured_hadoop_version = hdfs_config.get_configured_hadoop_version
get_configured_hdfs_client = hdfs_config.get_configured_hdfs_client
tmppath = hdfs_config.tmppath


# clients
from luigi.contrib.hdfs import clients as hdfs_clients
from luigi.contrib.hdfs import error as hdfs_error
from luigi.contrib.hdfs import snakebite_client as hdfs_snakebite_client
from luigi.contrib.hdfs import hadoopcli_clients as hdfs_hadoopcli_clients
HDFSCliError = hdfs_error.HDFSCliError
call_check = hdfs_hadoopcli_clients.HdfsClient.call_check
list_path = hdfs_snakebite_client.SnakebiteHdfsClient.list_path
HdfsClient = hdfs_hadoopcli_clients.HdfsClient
SnakebiteHdfsClient = hdfs_snakebite_client.SnakebiteHdfsClient
HdfsClientCdh3 = hdfs_hadoopcli_clients.HdfsClientCdh3
HdfsClientApache1 = hdfs_hadoopcli_clients.HdfsClientApache1
create_hadoopcli_client = hdfs_hadoopcli_clients.create_hadoopcli_client
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
