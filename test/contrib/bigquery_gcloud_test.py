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
This is an integration test for the BigQuery-luigi binding.

This test requires credentials that can access GCS & access to a bucket below.
Follow the directions in the gcloud tools to set up local credentials.
"""

import json
import os

import luigi
import unittest

try:
    import googleapiclient.errors
    import google.auth
except ImportError:
    raise unittest.SkipTest('Unable to load googleapiclient module')
from luigi.contrib import bigquery, bigquery_avro, gcs
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from luigi.contrib.gcs import GCSTarget
from luigi.contrib.bigquery import BigQueryExecutionError

import pytest
from helpers import unittest

# In order to run this test, you should set your GCS/BigQuery project/bucket.
# Unfortunately there's no mock
PROJECT_ID = os.environ.get('GCS_TEST_PROJECT_ID', 'your_project_id_here')
BUCKET_NAME = os.environ.get('GCS_TEST_BUCKET', 'your_test_bucket_here')
TEST_FOLDER = os.environ.get('TRAVIS_BUILD_ID', 'bigquery_test_folder')
DATASET_ID = os.environ.get('BQ_TEST_DATASET_ID', 'luigi_tests')
EU_DATASET_ID = os.environ.get('BQ_TEST_EU_DATASET_ID', 'luigi_tests_eu')
EU_LOCATION = 'EU'
US_LOCATION = 'US'

CREDENTIALS, _ = google.auth.default()


def bucket_url(suffix):
    """
    Actually it's bucket + test folder name
    """
    return 'gs://{}/{}/{}'.format(BUCKET_NAME, TEST_FOLDER, suffix)


@pytest.mark.gcloud
class TestLoadTask(bigquery.BigQueryLoadTask):
    source = luigi.Parameter()
    table = luigi.Parameter()
    dataset = luigi.Parameter()
    location = luigi.Parameter(default=None)

    @property
    def schema(self):
        return [
            {'mode': 'NULLABLE', 'name': 'field1', 'type': 'STRING'},
            {'mode': 'NULLABLE', 'name': 'field2', 'type': 'INTEGER'},
        ]

    def source_uris(self):
        return [self.source]

    def output(self):
        return bigquery.BigQueryTarget(PROJECT_ID, self.dataset, self.table, location=self.location)


@pytest.mark.gcloud
class TestRunQueryTask(bigquery.BigQueryRunQueryTask):
    query = ''' SELECT 'hello' as field1, 2 as field2 '''
    table = luigi.Parameter()
    dataset = luigi.Parameter()

    def output(self):
        return bigquery.BigQueryTarget(PROJECT_ID, self.dataset, self.table)


@pytest.mark.gcloud
class TestExtractTask(bigquery.BigQueryExtractTask):
    source = luigi.Parameter()
    table = luigi.Parameter()
    dataset = luigi.Parameter()
    location = luigi.Parameter(default=None)
    extract_gcs_file = luigi.Parameter()

    destination_format = luigi.Parameter(
        default=bigquery.DestinationFormat.CSV)
    print_header = luigi.Parameter(
        default=bigquery.PrintHeader.TRUE)
    field_delimiter = luigi.Parameter(
        default=bigquery.FieldDelimiter.COMMA)

    def output(self):
        return GCSTarget(bucket_url(self.extract_gcs_file))

    def requires(self):
        return TestLoadTask(
            source=self.source,
            dataset=self.dataset,
            table=self.table)


@pytest.mark.gcloud
class BigQueryGcloudTest(unittest.TestCase):
    def setUp(self):
        self.bq_client = bigquery.BigQueryClient(CREDENTIALS)
        self.gcs_client = gcs.GCSClient(CREDENTIALS)

        # Setup GCS input data
        try:
            self.gcs_client.client.buckets().insert(
                project=PROJECT_ID, body={'name': BUCKET_NAME, 'location': EU_LOCATION}).execute()
        except googleapiclient.errors.HttpError as ex:
            # todo verify that existing dataset is not US
            if ex.resp.status != 409:  # bucket already exists
                raise

        self.gcs_client.remove(bucket_url(''), recursive=True)
        self.gcs_client.mkdir(bucket_url(''))

        text = '\n'.join(map(json.dumps, [{'field1': 'hi', 'field2': 1}, {'field1': 'bye', 'field2': 2}]))
        self.gcs_file = bucket_url(self.id())
        self.gcs_client.put_string(text, self.gcs_file)

        # Setup BigQuery datasets
        self.table = bigquery.BQTable(project_id=PROJECT_ID, dataset_id=DATASET_ID,
                                      table_id=self.id().split('.')[-1], location=None)
        self.table_eu = bigquery.BQTable(project_id=PROJECT_ID, dataset_id=EU_DATASET_ID,
                                         table_id=self.id().split('.')[-1] + '_eu', location=EU_LOCATION)

        self.addCleanup(self.gcs_client.remove, bucket_url(''), recursive=True)
        self.addCleanup(self.bq_client.delete_dataset, self.table.dataset)
        self.addCleanup(self.bq_client.delete_dataset, self.table_eu.dataset)

        self.bq_client.delete_dataset(self.table.dataset)
        self.bq_client.delete_dataset(self.table_eu.dataset)
        self.bq_client.make_dataset(self.table.dataset, body={})
        self.bq_client.make_dataset(self.table_eu.dataset, body={})

    def test_extract_to_gcs_csv(self):
        task1 = TestLoadTask(
            source=self.gcs_file,
            dataset=self.table.dataset.dataset_id,
            table=self.table.table_id)
        task1.run()

        task2 = TestExtractTask(
            source=self.gcs_file,
            dataset=self.table.dataset.dataset_id,
            table=self.table.table_id,
            extract_gcs_file=self.id() + '_extract_file',
            destination_format=bigquery.DestinationFormat.CSV)
        task2.run()

        self.assertTrue(task2.output().exists)

    def test_extract_to_gcs_csv_alternate(self):
        task1 = TestLoadTask(
            source=self.gcs_file,
            dataset=self.table.dataset.dataset_id,
            table=self.table.table_id
        )
        task1.run()

        task2 = TestExtractTask(
            source=self.gcs_file,
            dataset=self.table.dataset.dataset_id,
            table=self.table.table_id,
            extract_gcs_file=self.id() + '_extract_file',
            destination_format=bigquery.DestinationFormat.CSV,
            print_header=bigquery.PrintHeader.FALSE,
            field_delimiter=bigquery.FieldDelimiter.PIPE
        )
        task2.run()

        self.assertTrue(task2.output().exists)

    def test_extract_to_gcs_json(self):
        task1 = TestLoadTask(
            source=self.gcs_file,
            dataset=self.table.dataset.dataset_id,
            table=self.table.table_id)
        task1.run()

        task2 = TestExtractTask(
            source=self.gcs_file,
            dataset=self.table.dataset.dataset_id,
            table=self.table.table_id,
            extract_gcs_file=self.id() + '_extract_file',
            destination_format=bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON)
        task2.run()

        self.assertTrue(task2.output().exists)

    def test_extract_to_gcs_avro(self):
        task1 = TestLoadTask(
            source=self.gcs_file,
            dataset=self.table.dataset.dataset_id,
            table=self.table.table_id)
        task1.run()

        task2 = TestExtractTask(
            source=self.gcs_file,
            dataset=self.table.dataset.dataset_id,
            table=self.table.table_id,
            extract_gcs_file=self.id() + '_extract_file',
            destination_format=bigquery.DestinationFormat.AVRO)
        task2.run()

        self.assertTrue(task2.output().exists)

    def test_load_eu_to_undefined(self):
        task = TestLoadTask(source=self.gcs_file,
                            dataset=self.table.dataset.dataset_id,
                            table=self.table.table_id,
                            location=EU_LOCATION)
        self.assertRaises(Exception, task.run)

    def test_load_us_to_eu(self):
        task = TestLoadTask(source=self.gcs_file,
                            dataset=self.table_eu.dataset.dataset_id,
                            table=self.table_eu.table_id,
                            location=US_LOCATION)
        self.assertRaises(Exception, task.run)

    def test_load_eu_to_eu(self):
        task = TestLoadTask(source=self.gcs_file,
                            dataset=self.table_eu.dataset.dataset_id,
                            table=self.table_eu.table_id,
                            location=EU_LOCATION)
        task.run()

        self.assertTrue(self.bq_client.dataset_exists(self.table_eu))
        self.assertTrue(self.bq_client.table_exists(self.table_eu))
        self.assertIn(self.table_eu.dataset_id,
                      list(self.bq_client.list_datasets(self.table_eu.project_id)))
        self.assertIn(self.table_eu.table_id,
                      list(self.bq_client.list_tables(self.table_eu.dataset)))

    def test_load_undefined_to_eu(self):
        task = TestLoadTask(source=self.gcs_file,
                            dataset=self.table_eu.dataset.dataset_id,
                            table=self.table_eu.table_id)
        task.run()

        self.assertTrue(self.bq_client.dataset_exists(self.table_eu))
        self.assertTrue(self.bq_client.table_exists(self.table_eu))
        self.assertIn(self.table_eu.dataset_id,
                      list(self.bq_client.list_datasets(self.table_eu.project_id)))
        self.assertIn(self.table_eu.table_id,
                      list(self.bq_client.list_tables(self.table_eu.dataset)))

    def test_load_new_eu_dataset(self):
        self.bq_client.delete_dataset(self.table.dataset)
        self.bq_client.delete_dataset(self.table_eu.dataset)

        self.assertFalse(self.bq_client.dataset_exists(self.table_eu))

        task = TestLoadTask(source=self.gcs_file,
                            dataset=self.table_eu.dataset.dataset_id,
                            table=self.table_eu.table_id,
                            location=EU_LOCATION)
        task.run()

        self.assertTrue(self.bq_client.dataset_exists(self.table_eu))
        self.assertTrue(self.bq_client.table_exists(self.table_eu))
        self.assertIn(self.table_eu.dataset_id,
                      list(self.bq_client.list_datasets(self.table_eu.project_id)))
        self.assertIn(self.table_eu.table_id,
                      list(self.bq_client.list_tables(self.table_eu.dataset)))

    def test_copy(self):
        task = TestLoadTask(source=self.gcs_file,
                            dataset=self.table.dataset.dataset_id,
                            table=self.table.table_id)
        task.run()

        self.assertTrue(self.bq_client.dataset_exists(self.table))
        self.assertTrue(self.bq_client.table_exists(self.table))
        self.assertIn(self.table.dataset_id,
                      list(self.bq_client.list_datasets(self.table.project_id)))
        self.assertIn(self.table.table_id,
                      list(self.bq_client.list_tables(self.table.dataset)))

        new_table = self.table._replace(table_id=self.table.table_id + '_copy')
        self.bq_client.copy(
            source_table=self.table,
            dest_table=new_table
        )
        self.assertTrue(self.bq_client.table_exists(new_table))
        self.bq_client.delete_table(new_table)
        self.assertFalse(self.bq_client.table_exists(new_table))

    def test_table_uri(self):
        intended_uri = "bq://" + PROJECT_ID + "/" + \
                       DATASET_ID + "/" + self.table.table_id
        self.assertTrue(self.table.uri == intended_uri)

    def test_run_query(self):
        task = TestRunQueryTask(table=self.table.table_id,
                                dataset=self.table.dataset.dataset_id)
        task._BIGQUERY_CLIENT = self.bq_client
        task.run()

        self.assertTrue(self.bq_client.table_exists(self.table))

    def test_run_successful_job(self):
        body = {'configuration': {'query': {'query': 'select count(*) from unnest([1,2,3])'}}}

        job_id = self.bq_client.run_job(PROJECT_ID, body)

        self.assertIsNotNone(job_id)
        self.assertNotEqual('', job_id)

    def test_run_failing_job(self):
        body = {'configuration': {'query': {'query': 'this is not a valid query'}}}

        self.assertRaises(BigQueryExecutionError, lambda: self.bq_client.run_job(PROJECT_ID, body))


@pytest.mark.gcloud
class BigQueryLoadAvroTest(unittest.TestCase):
    def _produce_test_input(self):
        schema = avro.schema.parse("""
        {
          "type":"record",
          "name":"TrackEntity2",
          "namespace":"com.spotify.entity.schema",
          "doc":"Track entity merged from various sources",
          "fields":[
            {
              "name":"map_record",
              "type":{
                "type":"map",
                "values":{
                  "type":"record",
                  "name":"MapNestedRecordObj",
                  "doc":"Nested Record in a map doc",
                  "fields":[
                    {
                      "name":"element1",
                      "type":"string",
                      "doc":"element 1 doc"
                    },
                    {
                      "name":"element2",
                      "type":[
                        "null",
                        "string"
                      ],
                      "doc":"element 2 doc"
                    }
                  ]
                }
              },
              "doc":"doc for map"
            },
            {
              "name":"additional",
              "type":{
                "type":"map",
                "values":"string"
              },
              "doc":"doc for second map record"
            },
            {
              "name":"track_gid",
              "type":"string",
              "doc":"Track GID in hexadecimal string"
            },
            {
              "name":"track_uri",
              "type":"string",
              "doc":"Track URI in base62 string"
            },
            {
              "name":"Suit",
              "type":{
                "type":"enum",
                "name":"Suit",
                "doc":"enum documentation broz",
                "symbols":[
                  "SPADES",
                  "HEARTS",
                  "DIAMONDS",
                  "CLUBS"
                ]
              }
            },
            {
              "name":"FakeRecord",
              "type":{
                "type":"record",
                "name":"FakeRecord",
                "namespace":"com.spotify.data.types.coolType",
                "doc":"My Fake Record doc",
                "fields":[
                  {
                    "name":"coolName",
                    "type":"string",
                    "doc":"Cool Name doc"
                  }
                ]
              }
            },
            {
              "name":"master_metadata",
              "type":[
                "null",
                {
                  "type":"record",
                  "name":"MasterMetadata",
                  "namespace":"com.spotify.data.types.metadata",
                  "doc":"metadoc",
                  "fields":[
                    {
                      "name":"track",
                      "type":[
                        "null",
                        {
                          "type":"record",
                          "name":"Track",
                          "doc":"Sqoop import of track",
                          "fields":[
                            {
                              "name":"id",
                              "type":[
                                "null",
                                "int"
                              ],
                              "doc":"id description field",
                              "default":null,
                              "columnName":"id",
                              "sqlType":"4"
                            },
                            {
                              "name":"name",
                              "type":[
                                "null",
                                "string"
                              ],
                              "doc":"name description field",
                              "default":null,
                              "columnName":"name",
                              "sqlType":"12"
                            }
                          ],
                          "tableName":"track"
                        }
                      ],
                      "default":null
                    }
                  ]
                }
              ]
            },
            {
              "name":"children",
              "type":{
                "type":"array",
                "items":{
                  "type":"record",
                  "name":"Child",
                  "doc":"array of children documentation",
                  "fields":[
                    {
                      "name":"name",
                      "type":"string",
                      "doc":"my specific child\'s doc"
                    }
                  ]
                }
              }
            }
          ]
        }""")
        self.addCleanup(os.remove, "tmp.avro")
        writer = DataFileWriter(open("tmp.avro", "wb"), DatumWriter(), schema)
        writer.append({
            u'track_gid': u'Cool guid',
            u'map_record': {
                u'Cool key': {
                    u'element1': u'element 1 data',
                    u'element2': u'element 2 data'
                }
            },
            u'additional': {
                u'key1': u'value1'
            }, u'master_metadata': {
                u'track': {
                    u'id': 1,
                    u'name': u'Cool Track Name'
                }
            }, u'track_uri': u'Totally a url here',
            u'FakeRecord': {
                u'coolName': u'Cool Fake Record Name'
            },
            u'Suit': u'DIAMONDS',
            u'children': [
                {
                    u'name': u'Bob'
                },
                {
                    u'name': u'Joe'
                }
            ]
        })
        writer.close()
        self.gcs_client.put("tmp.avro", self.gcs_dir_url + "/tmp.avro")

    def setUp(self):
        self.gcs_client = gcs.GCSClient(CREDENTIALS)
        self.bq_client = bigquery.BigQueryClient(CREDENTIALS)

        self.table_id = "avro_bq_table"
        self.gcs_dir_url = 'gs://' + BUCKET_NAME + "/foo"
        self.addCleanup(self.gcs_client.remove, self.gcs_dir_url)
        self.addCleanup(self.bq_client.delete_dataset, bigquery.BQDataset(PROJECT_ID, DATASET_ID, EU_LOCATION))
        self._produce_test_input()

    def test_load_avro_dir_and_propagate_doc(self):
        class BigQueryLoadAvroTestInput(luigi.ExternalTask):
            def output(_):
                return gcs.GCSTarget(self.gcs_dir_url)

        class BigQueryLoadAvroTestTask(bigquery_avro.BigQueryLoadAvro):
            def requires(_):
                return BigQueryLoadAvroTestInput()

            def output(_):
                return bigquery.BigQueryTarget(PROJECT_ID, DATASET_ID, self.table_id, location=EU_LOCATION)

        task = BigQueryLoadAvroTestTask()
        self.assertFalse(task.complete())
        task.run()
        self.assertTrue(task.complete())

        table = self.bq_client.client.tables().get(projectId=PROJECT_ID,
                                                   datasetId=DATASET_ID,
                                                   tableId=self.table_id).execute()
        self.assertEqual(table['description'], 'Track entity merged from various sources')
        # First map
        self.assertEqual(table['schema']['fields'][0]['description'], 'doc for map')
        # key
        self.assertFalse('description' in table['schema']['fields'][0]['fields'][0])
        # Value
        self.assertEqual(table['schema']['fields'][0]['fields'][1]['description'], 'Nested Record in a map doc')
        # Value record data
        self.assertEqual(table['schema']['fields'][0]['fields'][1]['fields'][0]['description'], 'element 1 doc')
        self.assertEqual(table['schema']['fields'][0]['fields'][1]['fields'][1]['description'], 'element 2 doc')

        # Second map
        self.assertEqual(table['schema']['fields'][1]['description'], 'doc for second map record')
        # key
        self.assertFalse('description' in table['schema']['fields'][1]['fields'][0])
        # Value
        self.assertFalse('description' in table['schema']['fields'][1]['fields'][1])

        # Several top level Primitive and Enums
        self.assertEqual(table['schema']['fields'][2]['description'], 'Track GID in hexadecimal string')
        self.assertEqual(table['schema']['fields'][3]['description'], 'Track URI in base62 string')
        self.assertEqual(table['schema']['fields'][4]['description'], 'enum documentation broz')

        # Nested Record containing primitive
        self.assertEqual(table['schema']['fields'][5]['description'], 'My Fake Record doc')
        self.assertEqual(table['schema']['fields'][5]['fields'][0]['description'], 'Cool Name doc')

        # Union with internal Record
        self.assertEqual(table['schema']['fields'][6]['description'], 'metadoc')
        self.assertEqual(table['schema']['fields'][6]['fields'][0]['description'], 'Sqoop import of track')
        self.assertEqual(table['schema']['fields'][6]['fields'][0]['fields'][0]['description'], 'id description field')
        self.assertEqual(table['schema']['fields'][6]['fields'][0]['fields'][1]['description'], 'name description field')

        # Array of Primitive
        self.assertEqual(table['schema']['fields'][7]['description'], 'array of children documentation')
        self.assertEqual(table['schema']['fields'][7]['fields'][0]['description'], 'my specific child\'s doc')
