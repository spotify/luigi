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

"""
We're using Redshift as the test bed since Redshift implements RDBMS. We could
have opted for PSQL but we're less familiar with that contrib and there are
less examples on how to test it.
"""

import luigi
import luigi.contrib.redshift
import mock

import unittest

import pytest


# Fake AWS and S3 credentials taken from `../redshift_test.py`.
AWS_ACCESS_KEY = 'key'
AWS_SECRET_KEY = 'secret'

AWS_ACCOUNT_ID = '0123456789012'
AWS_ROLE_NAME = 'MyRedshiftRole'

BUCKET = 'bucket'
KEY = 'key'


class DummyS3CopyToTableBase(luigi.contrib.redshift.S3CopyToTable):
    # Class attributes taken from `DummyPostgresImporter` in
    # `../postgres_test.py`.
    host = 'dummy_host'
    database = 'dummy_database'
    user = 'dummy_user'
    password = 'dummy_password'
    table = luigi.Parameter(default='dummy_table')
    columns = luigi.TupleParameter(
        default=(
            ('some_text', 'varchar(255)'),
            ('some_int', 'int'),
        )
    )

    copy_options = ''
    prune_table = ''
    prune_column = ''
    prune_date = ''

    def s3_load_path(self):
        return 's3://%s/%s' % (BUCKET, KEY)


class DummyS3CopyToTableKey(DummyS3CopyToTableBase):
    aws_access_key_id = AWS_ACCESS_KEY
    aws_secret_access_key = AWS_SECRET_KEY


@pytest.mark.aws
class TestS3CopyToTableWithMetaColumns(unittest.TestCase):
    @mock.patch("luigi.contrib.redshift.S3CopyToTable.enable_metadata_columns", new_callable=mock.PropertyMock, return_value=True)
    @mock.patch("luigi.contrib.redshift.S3CopyToTable.metadata_columns", new_callable=mock.PropertyMock, return_value=[('created_tz', 'TIMESTAMP')])
    @mock.patch("luigi.contrib.redshift.RedshiftTarget")
    def test_copy_check_meta_columns_to_table_if_exists(self,
                                                        mock_redshift_target,
                                                        mock_metadata_columns,
                                                        mock_metadata_columns_enabled):
        task = DummyS3CopyToTableKey(table='my_test_table')
        task.run()

        mock_cursor = (mock_redshift_target.return_value
                                           .connect
                                           .return_value
                                           .cursor
                                           .return_value)

        executed_query = mock_cursor.execute.call_args_list[1][0][0]

        expected_output = "SELECT 1 AS column_exists FROM information_schema.columns " \
                          "WHERE table_name = LOWER('{table}') " \
                          "AND column_name = LOWER('{column}') " \
                          "LIMIT 1;".format(table='my_test_table', column='created_tz')

        self.assertEqual(executed_query, expected_output)

    @mock.patch("luigi.contrib.redshift.S3CopyToTable.enable_metadata_columns", new_callable=mock.PropertyMock, return_value=True)
    @mock.patch("luigi.contrib.redshift.S3CopyToTable.metadata_columns", new_callable=mock.PropertyMock, return_value=[('created_tz', 'TIMESTAMP')])
    @mock.patch("luigi.contrib.redshift.RedshiftTarget")
    def test_copy_check_meta_columns_to_schematable_if_exists(self,
                                                              mock_redshift_target,
                                                              mock_metadata_columns,
                                                              mock_metadata_columns_enabled):
        task = DummyS3CopyToTableKey(table='test.my_test_table')
        task.run()

        mock_cursor = (mock_redshift_target.return_value
                                           .connect
                                           .return_value
                                           .cursor
                                           .return_value)

        executed_query = mock_cursor.execute.call_args_list[2][0][0]

        expected_output = "SELECT 1 AS column_exists FROM information_schema.columns " \
                          "WHERE table_schema = LOWER('{schema}') " \
                          "AND table_name = LOWER('{table}') " \
                          "AND column_name = LOWER('{column}') " \
                          "LIMIT 1;".format(schema='test', table='my_test_table', column='created_tz')

        self.assertEqual(executed_query, expected_output)

    @mock.patch("luigi.contrib.redshift.S3CopyToTable.enable_metadata_columns", new_callable=mock.PropertyMock, return_value=True)
    @mock.patch("luigi.contrib.redshift.S3CopyToTable.metadata_columns", new_callable=mock.PropertyMock, return_value=[('created_tz', 'TIMESTAMP')])
    @mock.patch("luigi.contrib.redshift.S3CopyToTable._column_exists",  return_value=True)
    @mock.patch("luigi.contrib.redshift.S3CopyToTable._add_column_to_table")
    @mock.patch("luigi.contrib.redshift.RedshiftTarget")
    def test_copy_not_add_if_meta_columns_already_exists(self,
                                                         mock_redshift_target,
                                                         mock_add_to_table,
                                                         mock_columns_exists,
                                                         mock_metadata_columns,
                                                         mock_metadata_columns_enabled):
        task = DummyS3CopyToTableKey()
        task.run()

        self.assertFalse(mock_add_to_table.called)

    @mock.patch("luigi.contrib.redshift.S3CopyToTable.enable_metadata_columns", new_callable=mock.PropertyMock, return_value=True)
    @mock.patch("luigi.contrib.redshift.S3CopyToTable.metadata_columns", new_callable=mock.PropertyMock, return_value=[('created_tz', 'TIMESTAMP')])
    @mock.patch("luigi.contrib.redshift.S3CopyToTable._column_exists",  return_value=False)
    @mock.patch("luigi.contrib.redshift.S3CopyToTable._add_column_to_table")
    @mock.patch("luigi.contrib.redshift.RedshiftTarget")
    def test_copy_add_if_meta_columns_not_already_exists(self,
                                                         mock_redshift_target,
                                                         mock_add_to_table,
                                                         mock_columns_exists,
                                                         mock_metadata_columns,
                                                         mock_metadata_columns_enabled):
        task = DummyS3CopyToTableKey()
        task.run()

        self.assertTrue(mock_add_to_table.called)

    @mock.patch("luigi.contrib.redshift.S3CopyToTable.enable_metadata_columns", new_callable=mock.PropertyMock, return_value=True)
    @mock.patch("luigi.contrib.redshift.S3CopyToTable.metadata_columns", new_callable=mock.PropertyMock, return_value=[('created_tz', 'TIMESTAMP')])
    @mock.patch("luigi.contrib.redshift.S3CopyToTable._column_exists",  return_value=False)
    @mock.patch("luigi.contrib.redshift.RedshiftTarget")
    def test_copy_add_regular_column(self,
                                     mock_redshift_target,
                                     mock_columns_exists,
                                     mock_metadata_columns,
                                     mock_metadata_columns_enabled):
        task = DummyS3CopyToTableKey(table='my_test_table')
        task.run()

        mock_cursor = (mock_redshift_target.return_value
                                           .connect
                                           .return_value
                                           .cursor
                                           .return_value)

        executed_query = mock_cursor.execute.call_args_list[1][0][0]

        expected_output = "ALTER TABLE {table} " \
                          "ADD COLUMN {column} {type};".format(table='my_test_table', column='created_tz', type='TIMESTAMP')

        self.assertEqual(executed_query, expected_output)

    @mock.patch("luigi.contrib.redshift.S3CopyToTable.enable_metadata_columns", new_callable=mock.PropertyMock, return_value=True)
    @mock.patch("luigi.contrib.redshift.S3CopyToTable.metadata_columns", new_callable=mock.PropertyMock, return_value=[('created_tz', 'TIMESTAMP', 'bytedict')])
    @mock.patch("luigi.contrib.redshift.S3CopyToTable._column_exists",  return_value=False)
    @mock.patch("luigi.contrib.redshift.RedshiftTarget")
    def test_copy_add_encoded_column(self,
                                     mock_redshift_target,
                                     mock_columns_exists,
                                     mock_metadata_columns,
                                     mock_metadata_columns_enabled):
        task = DummyS3CopyToTableKey(table='my_test_table')
        task.run()

        mock_cursor = (mock_redshift_target.return_value
                                           .connect
                                           .return_value
                                           .cursor
                                           .return_value)

        executed_query = mock_cursor.execute.call_args_list[1][0][0]

        expected_output = "ALTER TABLE {table} " \
                          "ADD COLUMN {column} {type} ENCODE {encoding};".format(table='my_test_table', column='created_tz',
                                                                                 type='TIMESTAMP',
                                                                                 encoding='bytedict')

        self.assertEqual(executed_query, expected_output)

    @mock.patch("luigi.contrib.redshift.S3CopyToTable.enable_metadata_columns", new_callable=mock.PropertyMock, return_value=True)
    @mock.patch("luigi.contrib.redshift.S3CopyToTable.metadata_columns", new_callable=mock.PropertyMock, return_value=[('created_tz')])
    @mock.patch("luigi.contrib.redshift.S3CopyToTable._column_exists",  return_value=False)
    @mock.patch("luigi.contrib.redshift.RedshiftTarget")
    def test_copy_raise_error_on_no_column_type(self,
                                                mock_redshift_target,
                                                mock_columns_exists,
                                                mock_metadata_columns,
                                                mock_metadata_columns_enabled):
        task = DummyS3CopyToTableKey()

        with self.assertRaises(ValueError):
            task.run()

    @mock.patch("luigi.contrib.redshift.S3CopyToTable.enable_metadata_columns", new_callable=mock.PropertyMock, return_value=True)
    @mock.patch("luigi.contrib.redshift.S3CopyToTable.metadata_columns", new_callable=mock.PropertyMock,
                return_value=[('created_tz', 'TIMESTAMP', 'bytedict', '42')])
    @mock.patch("luigi.contrib.redshift.S3CopyToTable._column_exists",  return_value=False)
    @mock.patch("luigi.contrib.redshift.RedshiftTarget")
    def test_copy_raise_error_on_invalid_column(self,
                                                mock_redshift_target,
                                                mock_columns_exists,
                                                mock_metadata_columns,
                                                mock_metadata_columns_enabled):
        task = DummyS3CopyToTableKey()

        with self.assertRaises(ValueError):
            task.run()

    @mock.patch("luigi.contrib.redshift.S3CopyToTable.enable_metadata_columns", new_callable=mock.PropertyMock, return_value=True)
    @mock.patch("luigi.contrib.redshift.S3CopyToTable.metadata_queries",  new_callable=mock.PropertyMock, return_value=['SELECT 1 FROM X', 'SELECT 2 FROM Y'])
    @mock.patch("luigi.contrib.redshift.RedshiftTarget")
    def test_post_copy_metacolumns(self,
                                   mock_redshift_target,
                                   mock_metadata_queries,
                                   mock_metadata_columns_enabled):
        task = DummyS3CopyToTableKey()
        task.run()

        mock_cursor = (mock_redshift_target.return_value
                                           .connect
                                           .return_value
                                           .cursor
                                           .return_value)

        executed_query = mock_cursor.execute.call_args_list[2][0][0]
        expected_output = "SELECT 1 FROM X"
        self.assertEqual(executed_query, expected_output)

        executed_query = mock_cursor.execute.call_args_list[3][0][0]
        expected_output = "SELECT 2 FROM Y"
        self.assertEqual(executed_query, expected_output)
