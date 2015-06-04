#!/usr/bin/env python
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
The pyspark program.

This module will be run by spark-submit for PySparkTask jobs.

The first argument is a path to the pickled instance of the PySparkTask,
other arguments are the ones returned by PySparkTask.app_options()

"""

from __future__ import print_function

try:
    import cPickle as pickle
except ImportError:
    import pickle
import logging
import sys


class PySparkRunner(object):

    def __init__(self, job, *args):
        with open(job, "rb") as fd:
            self.job = pickle.load(fd)
        self.args = args

    def run(self):
        from pyspark import SparkContext, SparkConf
        conf = SparkConf()
        self.job.setup(conf)
        with SparkContext(conf=conf) as sc:
            self.job.setup_remote(sc)
            self.job.main(sc, *self.args)


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARN)
    PySparkRunner(*sys.argv[1:]).run()
