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
Databricks job wrapper for Luigi


"""

import logging
from time import sleep

import luigi

logger = logging.getLogger('luigi-interface')

try:
    import requests
except ImportError:
    logger.warning("This module requires the python package 'requests'.")


class databricks(luigi.Config):
    """
    Get Databricks auth credentials from 'databricks' section in configuration file
    """
    username = luigi.Parameter(default='')
    password = luigi.Parameter(default='')
    instance = luigi.Parameter(default='community.cloud.databricks.com')


class DatabricksRunSubmitTask(luigi.Task):
    """
    R using the Runs Submit endpoint. This submits a workload
    directly without requiring job creation.
    """

    databricks_config = databricks()

    def __init__(self):
        self.__logger = logger
