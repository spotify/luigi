# -*- coding: utf-8 -*-
#
# Copyright 2015 Twitter Inc
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
These are the unit tests for the BigQuery-luigi binding.
"""


import luigi
from luigi.contrib import bigquery
from luigi.contrib.gcs import GCSTarget

from helpers import unittest
from mock import MagicMock
import pytest

PROJECT_ID = 'projectid'
DATASET_ID = 'dataset'


class TestRunQueryTask(bigquery.BigQueryRunQueryTask):
    client = MagicMock()
    query = ''' SELECT 'hello' as field1, 2 as field2 '''
    table = luigi.Parameter()

    def output(self):
        return bigquery.BigQueryTarget(PROJECT_ID, DATASET_ID, self.table, client=self.client)


class TestRunQueryTaskDontFlattenResults(TestRunQueryTask):

    @property
    def flatten_results(self):
        return False


class TestRunQueryTaskWithRequires(bigquery.BigQueryRunQueryTask):
    client = MagicMock()
    table = luigi.Parameter()

    def requires(self):
        return TestRunQueryTask(table='table1')

    @property
    def query(self):
        requires = self.requires().output().table
        dataset = requires.dataset_id
        table = requires.table_id
        return 'SELECT * FROM [{dataset}.{table}]'.format(dataset=dataset, table=table)

    def output(self):
        return bigquery.BigQueryTarget(PROJECT_ID, DATASET_ID, self.table, client=self.client)


class TestRunQueryTaskWithUdf(bigquery.BigqueryRunQueryTask):
    client = MagicMock()
    table = luigi.Parameter()

    @property
    def udf_resource_uris(self):
        return ["gs://test/file1.js", "gs://test/file2.js"]

    @property
    def query(self):
        return 'SELECT 1'

    def output(self):
        return bigquery.BigqueryTarget(PROJECT_ID, DATASET_ID, self.table, client=self.client)


class TestRunQueryTaskWithoutLegacySql(bigquery.BigqueryRunQueryTask):
    client = MagicMock()
    table = luigi.Parameter()

    @property
    def use_legacy_sql(self):
        return False

    @property
    def query(self):
        return 'SELECT 1'

    def output(self):
        return bigquery.BigqueryTarget(PROJECT_ID, DATASET_ID, self.table, client=self.client)


class TestExternalBigQueryTask(bigquery.ExternalBigQueryTask):
    client = MagicMock()

    def output(self):
        return bigquery.BigQueryTarget(PROJECT_ID, DATASET_ID, 'table1', client=self.client)


class TestCreateViewTask(bigquery.BigQueryCreateViewTask):
    client = MagicMock()
    view = '''SELECT * FROM table LIMIT 10'''

    def output(self):
        return bigquery.BigQueryTarget(PROJECT_ID, DATASET_ID, 'view1', client=self.client)


class TestExtractTask(bigquery.BigQueryExtractTask):
    client = MagicMock()

    def output(self):
        return GCSTarget('gs://test/unload_file.csv', client=self.client)

    def requires(self):
        return TestExternalBigQueryTask()


@pytest.mark.contrib
class BigQueryTest(unittest.TestCase):

    def test_bulk_complete(self):
        parameters = ['table1', 'table2']

        client = MagicMock()
        client.dataset_exists.return_value = True
        client.list_tables.return_value = ['table2', 'table3']
        TestRunQueryTask.client = client

        complete = list(TestRunQueryTask.bulk_complete(parameters))
        self.assertEqual(complete, ['table2'])

        # Test that bulk_complete accepts lazy sequences in addition to lists
        def parameters_gen():
            yield 'table1'
            yield 'table2'

        complete = list(TestRunQueryTask.bulk_complete(parameters_gen()))
        self.assertEqual(complete, ['table2'])

    def test_dataset_doesnt_exist(self):
        client = MagicMock()
        client.dataset_exists.return_value = False
        TestRunQueryTask.client = client

        complete = list(TestRunQueryTask.bulk_complete(['table1']))
        self.assertEqual(complete, [])

    def test_query_property(self):
        task = TestRunQueryTask(table='table2')
        task.client = MagicMock()
        task.run()

        (_, job), _ = task.client.run_job.call_args
        query = job['configuration']['query']['query']
        self.assertEqual(query, TestRunQueryTask.query)

    def test_override_query_property(self):
        task = TestRunQueryTaskWithRequires(table='table2')
        task.client = MagicMock()
        task.run()

        (_, job), _ = task.client.run_job.call_args
        query = job['configuration']['query']['query']

        expected_table = '[' + DATASET_ID + '.' + task.requires().output().table.table_id + ']'
        self.assertIn(expected_table, query)
        self.assertEqual(query, task.query)

    def test_query_udf(self):
        task = TestRunQueryTaskWithUdf(table='table2')
        task.client = MagicMock()
        task.run()

        (_, job), _ = task.client.run_job.call_args

        udfs = [
            {'resourceUri': 'gs://test/file1.js'},
            {'resourceUri': 'gs://test/file2.js'},
        ]

        self.assertEqual(job['configuration']['query']['userDefinedFunctionResources'], udfs)

    def test_query_with_legacy_sql(self):
        task = TestRunQueryTask(table='table2')
        task.client = MagicMock()
        task.run()

        (_, job), _ = task.client.run_job.call_args

        self.assertEqual(job['configuration']['query']['useLegacySql'], True)

    def test_query_without_legacy_sql(self):
        task = TestRunQueryTaskWithoutLegacySql(table='table2')
        task.client = MagicMock()
        task.run()

        (_, job), _ = task.client.run_job.call_args

        self.assertEqual(job['configuration']['query']['useLegacySql'], False)

    def test_external_task(self):
        task = TestExternalBigQueryTask()
        self.assertIsInstance(task, luigi.ExternalTask)
        self.assertIsInstance(task, bigquery.MixinBigQueryBulkComplete)

    def test_create_view(self):
        task = TestCreateViewTask()

        task.client.get_view.return_value = None
        self.assertFalse(task.complete())

        task.run()
        (table, view), _ = task.client.update_view.call_args
        self.assertEqual(task.output().table, table)
        self.assertEqual(task.view, view)

    def test_update_view(self):
        task = TestCreateViewTask()

        task.client.get_view.return_value = 'some other query'
        self.assertFalse(task.complete())

        task.run()
        (table, view), _ = task.client.update_view.call_args
        self.assertEqual(task.output().table, table)
        self.assertEqual(task.view, view)

    def test_view_completed(self):
        task = TestCreateViewTask()

        task.client.get_view.return_value = task.view
        self.assertTrue(task.complete())

    def test_flatten_results(self):
        task = TestRunQueryTask(table='table3')
        self.assertTrue(task.flatten_results)

    def test_dont_flatten_results(self):
        task = TestRunQueryTaskDontFlattenResults(table='table3')
        self.assertFalse(task.flatten_results)

    def test_extract_table(self):
        task = TestExtractTask()
        task.run()

        bq_client = luigi.task.flatten(task.input())[0].client
        (_, job), _ = bq_client.run_job.call_args

        destination_uris = job['configuration']['extract']['destinationUris']

        self.assertEqual(destination_uris, task.destination_uris)
