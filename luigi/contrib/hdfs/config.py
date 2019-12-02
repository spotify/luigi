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
You can configure what client by setting the "client" config under the "hdfs" section in the configuration, or using the ``--hdfs-client`` command line option.
"hadoopcli" is the slowest, but should work out of the box.
"""

import random
import luigi
import luigi.configuration
import os
import getpass

from urllib.parse import urlparse, urlunparse


class hdfs(luigi.Config):
    client_version = luigi.IntParameter(default=None)
    namenode_host = luigi.OptionalParameter(default=None)
    namenode_port = luigi.IntParameter(default=None)
    client = luigi.Parameter(default='hadoopcli')
    tmp_dir = luigi.OptionalParameter(
        default=None,
        config_path=dict(section='core', name='hdfs-tmp-dir'),
    )


class hadoopcli(luigi.Config):
    command = luigi.Parameter(default="hadoop",
                              config_path=dict(section="hadoop", name="command"),
                              description='The hadoop command, will run split() on it, '
                                          'so you can pass something like "hadoop --param"')
    version = luigi.Parameter(default="cdh4",
                              config_path=dict(section="hadoop", name="version"),
                              description='Can also be cdh3 or apache1')


def load_hadoop_cmd():
    return hadoopcli().command.split()


def get_configured_hadoop_version():
    """
    CDH4 (hadoop 2+) has a slightly different syntax for interacting with hdfs
    via the command line.

    The default version is CDH4, but one can override
    this setting with "cdh3" or "apache1" in the hadoop section of the config
    in order to use the old syntax.
    """
    return hadoopcli().version.lower()


def get_configured_hdfs_client():
    """
    This is a helper that fetches the configuration value for 'client' in
    the [hdfs] section. It will return the client that retains backwards
    compatibility when 'client' isn't configured.
    """
    return hdfs().client


def tmppath(path=None, include_unix_username=True):
    """
    @param path: target path for which it is needed to generate temporary location
    @type path: str
    @type include_unix_username: bool
    @rtype: str

    Note that include_unix_username might work on windows too.
    """
    addon = "luigitemp-%08d" % random.randrange(1e9)
    temp_dir = '/tmp'  # default tmp dir if none is specified in config

    # 1. Figure out to which temporary directory to place
    configured_hdfs_tmp_dir = hdfs().tmp_dir
    if configured_hdfs_tmp_dir is not None:
        # config is superior
        base_dir = configured_hdfs_tmp_dir
    elif path is not None:
        # need to copy correct schema and network location
        parsed = urlparse(path)
        base_dir = urlunparse((parsed.scheme, parsed.netloc, temp_dir, '', '', ''))
    else:
        # just system temporary directory
        base_dir = temp_dir

    # 2. Figure out what to place
    if path is not None:
        if path.startswith(temp_dir + '/'):
            # Not 100%, but some protection from directories like /tmp/tmp/file
            subdir = path[len(temp_dir):]
        else:
            # Protection from /tmp/hdfs:/dir/file
            parsed = urlparse(path)
            subdir = parsed.path
        subdir = subdir.lstrip('/') + '-'
    else:
        # just return any random temporary location
        subdir = ''

    if include_unix_username:
        subdir = os.path.join(getpass.getuser(), subdir)

    return os.path.join(base_dir, subdir + addon)
