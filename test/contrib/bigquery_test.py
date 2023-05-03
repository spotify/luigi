# -*- coding: utf-8 -*-
#
# Copyright 2019 Spotify AB
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
These are the unit tests for the BigQueryLoadAvro class.
"""

import unittest

import mock
import pytest

from luigi.contrib.bigquery import BigQueryLoadTask, BigQueryTarget, BQDataset, \
    BigQueryRunQueryTask, BigQueryExtractTask
from luigi.contrib.gcs import GCSTarget


@pytest.mark.gcloud
class BigQueryLoadTaskTest(unittest.TestCase):

    @mock.patch('luigi.contrib.bigquery.BigQueryClient.run_job')
    def test_configure_job(self, run_job):
        class MyBigQueryLoadTask(BigQueryLoadTask):
            def source_uris(self):
                return ['gs://_']

            def configure_job(self, configuration):
                configuration['load']['destinationTableProperties'] = {
                    'description': 'Nice table'
                }
                return configuration

            def output(self):
                return BigQueryTarget(project_id='proj', dataset_id='ds', table_id='t')

        job = MyBigQueryLoadTask()
        job.run()

        expected_body = {
            'configuration': {
                'load': {
                    'destinationTable': {'projectId': 'proj', 'datasetId': 'ds', 'tableId': 't'},
                    'encoding': 'UTF-8',
                    'sourceFormat': 'NEWLINE_DELIMITED_JSON',
                    'writeDisposition': 'WRITE_EMPTY',
                    'sourceUris': ['gs://_'],
                    'maxBadRecords': 0,
                    'ignoreUnknownValues': False,
                    'autodetect': True,
                    'destinationTableProperties': {'description': 'Nice table'}
                }
            }
        }
        run_job.assert_called_with('proj', expected_body, dataset=BQDataset('proj', 'ds', None))


@pytest.mark.gcloud
class BigQueryRunQueryTaskTest(unittest.TestCase):
    @mock.patch('luigi.contrib.bigquery.BigQueryClient.run_job')
    def test_configure_job(self, run_job):
        class MyBigQueryRunQuery(BigQueryRunQueryTask):
            query = 'SELECT @thing'
            use_legacy_sql = False

            def configure_job(self, configuration):
                configuration['query']['parameterMode'] = 'NAMED'
                configuration['query']['queryParameters'] = {
                    'name': 'thing',
                    'parameterType': {'type': 'STRING'},
                    'parameterValue': {'value': 'Nice Thing'}
                }
                return configuration

            def output(self):
                return BigQueryTarget(project_id='proj', dataset_id='ds', table_id='t')

        job = MyBigQueryRunQuery()
        job.run()

        expected_body = {
            'configuration': {
                'query': {
                    'query': 'SELECT @thing',
                    'priority': 'INTERACTIVE',
                    'destinationTable': {'projectId': 'proj', 'datasetId': 'ds', 'tableId': 't'},
                    'allowLargeResults': True,
                    'createDisposition': 'CREATE_IF_NEEDED',
                    'writeDisposition': 'WRITE_TRUNCATE',
                    'flattenResults': True,
                    'userDefinedFunctionResources': [],
                    'useLegacySql': False,
                    'parameterMode': 'NAMED',
                    'queryParameters': {
                        'name': 'thing',
                        'parameterType': {'type': 'STRING'},
                        'parameterValue': {'value': 'Nice Thing'}
                    }
                }
            }
        }
        run_job.assert_called_with('proj', expected_body, dataset=BQDataset('proj', 'ds', None))


@pytest.mark.gcloud
class BigQueryExtractTaskTest(unittest.TestCase):
    @mock.patch('luigi.contrib.bigquery.BigQueryClient.run_job')
    def test_configure_job(self, run_job):
        class MyBigQueryExtractTask(BigQueryExtractTask):
            destination_format = 'AVRO'

            def configure_job(self, configuration):
                configuration['extract']['useAvroLogicalTypes'] = True
                return configuration

            def input(self):
                return BigQueryTarget(project_id='proj', dataset_id='ds', table_id='t')

            def output(self):
                return GCSTarget('gs://_')

        job = MyBigQueryExtractTask()
        job.run()

        expected_body = {
            'configuration': {
                'extract': {
                    'sourceTable': {'projectId': 'proj', 'datasetId': 'ds', 'tableId': 't'},
                    'destinationUris': ['gs://_'],
                    'destinationFormat': 'AVRO',
                    'compression': 'NONE',
                    'useAvroLogicalTypes': True
                }
            }
        }
        run_job.assert_called_with('proj', expected_body, dataset=BQDataset('proj', 'ds', None))
