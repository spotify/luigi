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

import os

from helpers import unittest


class ImportTest(unittest.TestCase):

    def import_test(self):
        """Test that all module can be imported
        """

        luigidir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            '..'
        )

        packagedir = os.path.join(luigidir, 'luigi')

        for root, subdirs, files in os.walk(packagedir):
            package = os.path.relpath(root, luigidir).replace('/', '.')

            if '__init__.py' in files:
                __import__(package)

            for f in files:
                if f.endswith('.py') and not f.startswith('_'):
                    __import__(package + '.' + f[:-3])

    def import_luigi_test(self):
        """
        Test that the top luigi package can be imported and contains the usual suspects.
        """
        import luigi

        # These should exist (if not, this will cause AttributeErrors)
        expected = [
            luigi.Event,
            luigi.Config,
            luigi.Task, luigi.ExternalTask, luigi.WrapperTask,
            luigi.Target, luigi.LocalTarget,
            luigi.namespace,
            luigi.RemoteScheduler,
            luigi.RPCError,
            luigi.run, luigi.build,
            luigi.Parameter,
            luigi.DateHourParameter, luigi.DateMinuteParameter, luigi.DateSecondParameter, luigi.DateParameter,
            luigi.MonthParameter, luigi.YearParameter,
            luigi.DateIntervalParameter, luigi.TimeDeltaParameter,
            luigi.IntParameter, luigi.FloatParameter,
            luigi.BoolParameter,
        ]
        self.assertGreater(len(expected), 0)
