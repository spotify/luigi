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

import json
import os
import sys

import mock
from moto import mock_s3

import luigi
import luigi.contrib.redshift
import luigi.notifications
from helpers import unittest, with_config
from luigi.contrib import redshift
from luigi.contrib.s3 import S3Client

import pytest

if (3, 4, 0) <= sys.version_info[:3] < (3, 4, 3):
    # spulec/moto#308
    mock_s3 = unittest.skip('moto mock doesn\'t work with python3.4')  # NOQA


# Fake AWS and S3 credentials taken from `../redshift_test.py`.
AWS_ACCESS_KEY = 'key'
AWS_SECRET_KEY = 'secret'

AWS_ACCOUNT_ID = '0123456789012'
AWS_ROLE_NAME = 'MyRedshiftRole'

BUCKET = 'bucket'
KEY = 'key'
KEY_2 = 'key2'
FILES = ['file1', 'file2', 'file3']


def generate_manifest_json(path_to_folders, file_names):
    entries = []
    for path_to_folder in path_to_folders:
        for file_name in file_names:
            entries.append({
                'url': '%s/%s' % (path_to_folder, file_name),
                'mandatory': True
            })
    return {'entries': entries}


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
    table_constraints = luigi.Parameter(default='')

    copy_options = ''
    prune_table = ''
    prune_column = ''
    prune_date = ''

    def s3_load_path(self):
        return 's3://%s/%s' % (BUCKET, KEY)


class DummyS3CopyJSONToTableBase(luigi.contrib.redshift.S3CopyJSONToTable):
    # Class attributes taken from `DummyPostgresImporter` in
    # `../postgres_test.py`.
    aws_access_key_id = AWS_ACCESS_KEY
    aws_secret_access_key = AWS_SECRET_KEY

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

    jsonpath = ''
    copy_json_options = ''

    def s3_load_path(self):
        return 's3://%s/%s' % (BUCKET, KEY)


class DummyS3CopyToTableKey(DummyS3CopyToTableBase):
    aws_access_key_id = AWS_ACCESS_KEY
    aws_secret_access_key = AWS_SECRET_KEY


class DummyS3CopyToTableWithCompressionEncodings(DummyS3CopyToTableKey):
    columns = (
        ('some_text', 'varchar(255)', 'LZO'),
        ('some_int', 'int', 'DELTA'),
    )


class DummyS3CopyToTableRole(DummyS3CopyToTableBase):
    aws_account_id = AWS_ACCESS_KEY
    aws_arn_role_name = AWS_SECRET_KEY


class DummyS3CopyToTempTable(DummyS3CopyToTableKey):
    # Extend/alter DummyS3CopyToTable for temp table copying
    table = luigi.Parameter(default='stage_dummy_table')

    table_type = 'TEMP'

    prune_date = 'current_date - 30'
    prune_column = 'dumb_date'
    prune_table = 'stage_dummy_table'

    queries = ["insert into dummy_table select * from stage_dummy_table;"]


@pytest.mark.aws
class TestInternalCredentials(unittest.TestCase, DummyS3CopyToTableKey):
    def test_from_property(self):
        self.assertEqual(self.aws_access_key_id, AWS_ACCESS_KEY)
        self.assertEqual(self.aws_secret_access_key, AWS_SECRET_KEY)


@pytest.mark.aws
class TestExternalCredentials(unittest.TestCase, DummyS3CopyToTableBase):
    @mock.patch.dict(os.environ, {"AWS_ACCESS_KEY_ID": "env_key",
                                  "AWS_SECRET_ACCESS_KEY": "env_secret"})
    def test_from_env(self):
        self.assertEqual(self.aws_access_key_id, "env_key")
        self.assertEqual(self.aws_secret_access_key, "env_secret")

    @with_config({"redshift": {"aws_access_key_id": "config_key",
                               "aws_secret_access_key": "config_secret"}})
    def test_from_config(self):
        self.assertEqual(self.aws_access_key_id, "config_key")
        self.assertEqual(self.aws_secret_access_key, "config_secret")


@pytest.mark.aws
class TestS3CopyToTableWithMetaColumns(unittest.TestCase):
    @mock.patch("luigi.contrib.redshift.S3CopyToTable.enable_metadata_columns", new_callable=mock.PropertyMock, return_value=True)
    @mock.patch("luigi.contrib.redshift.S3CopyToTable._add_metadata_columns")
    @mock.patch("luigi.contrib.redshift.S3CopyToTable.post_copy_metacolumns")
    @mock.patch("luigi.contrib.redshift.RedshiftTarget")
    def test_copy_with_metadata_columns_enabled(self,
                                                mock_redshift_target,
                                                mock_add_columns,
                                                mock_update_columns,
                                                mock_metadata_columns_enabled):
        task = DummyS3CopyToTableKey()
        task.run()

        self.assertTrue(mock_add_columns.called)
        self.assertTrue(mock_update_columns.called)

    @mock.patch("luigi.contrib.redshift.S3CopyToTable.enable_metadata_columns", new_callable=mock.PropertyMock, return_value=False)
    @mock.patch("luigi.contrib.redshift.S3CopyToTable._add_metadata_columns")
    @mock.patch("luigi.contrib.redshift.S3CopyToTable.post_copy_metacolumns")
    @mock.patch("luigi.contrib.redshift.RedshiftTarget")
    def test_copy_with_metadata_columns_disabled(self,
                                                 mock_redshift_target,
                                                 mock_add_columns,
                                                 mock_update_columns,
                                                 mock_metadata_columns_enabled):
        task = DummyS3CopyToTableKey()
        task.run()

        self.assertFalse(mock_add_columns.called)
        self.assertFalse(mock_update_columns.called)

    @mock.patch("luigi.contrib.redshift.S3CopyToTable.enable_metadata_columns", new_callable=mock.PropertyMock, return_value=True)
    @mock.patch("luigi.contrib.redshift.S3CopyToTable._add_metadata_columns")
    @mock.patch("luigi.contrib.redshift.S3CopyToTable.post_copy_metacolumns")
    @mock.patch("luigi.contrib.redshift.RedshiftTarget")
    def test_json_copy_with_metadata_columns_enabled(self,
                                                     mock_redshift_target,
                                                     mock_add_columns,
                                                     mock_update_columns,
                                                     mock_metadata_columns_enabled):
        task = DummyS3CopyJSONToTableBase()
        task.run()

        self.assertTrue(mock_add_columns.called)
        self.assertTrue(mock_update_columns.called)

    @mock.patch("luigi.contrib.redshift.S3CopyToTable.enable_metadata_columns", new_callable=mock.PropertyMock, return_value=False)
    @mock.patch("luigi.contrib.redshift.S3CopyToTable._add_metadata_columns")
    @mock.patch("luigi.contrib.redshift.S3CopyToTable.post_copy_metacolumns")
    @mock.patch("luigi.contrib.redshift.RedshiftTarget")
    def test_json_copy_with_metadata_columns_disabled(self,
                                                      mock_redshift_target,
                                                      mock_add_columns,
                                                      mock_update_columns,
                                                      mock_metadata_columns_enabled):
        task = DummyS3CopyJSONToTableBase()
        task.run()

        self.assertFalse(mock_add_columns.called)
        self.assertFalse(mock_update_columns.called)


@pytest.mark.aws
class TestS3CopyToTable(unittest.TestCase):
    @mock.patch("luigi.contrib.redshift.RedshiftTarget")
    def test_copy_missing_creds(self, mock_redshift_target):

        # Make sure credentials are not set as env vars
        try:
            del os.environ['AWS_ACCESS_KEY_ID']
            del os.environ['AWS_SECRET_ACCESS_KEY']
        except KeyError:
            pass

        task = DummyS3CopyToTableBase()

        # The mocked connection cursor passed to
        # S3CopyToTable.copy(self, cursor, f).
        mock_cursor = (mock_redshift_target.return_value
                       .connect
                       .return_value
                       .cursor
                       .return_value)

        with self.assertRaises(NotImplementedError):
            task.copy(mock_cursor, task.s3_load_path())

    @mock.patch("luigi.contrib.redshift.S3CopyToTable.copy")
    @mock.patch("luigi.contrib.redshift.RedshiftTarget")
    def test_s3_copy_to_table(self, mock_redshift_target, mock_copy):
        task = DummyS3CopyToTableKey()
        task.run()

        # The mocked connection cursor passed to
        # S3CopyToTable.copy(self, cursor, f).
        mock_cursor = (mock_redshift_target.return_value
                                           .connect
                                           .return_value
                                           .cursor
                                           .return_value)

        # `mock_redshift_target` is the mocked `RedshiftTarget` object
        # returned by S3CopyToTable.output(self).
        mock_redshift_target.assert_called_with(database=task.database,
                                                host=task.host,
                                                update_id=task.task_id,
                                                user=task.user,
                                                table=task.table,
                                                password=task.password)

        # Check if the `S3CopyToTable.s3_load_path` class attribute was
        # successfully referenced in the `S3CopyToTable.run` method, which is
        # in-turn passed to `S3CopyToTable.copy` and other functions in `run`
        # (see issue #995).
        mock_copy.assert_called_with(mock_cursor, task.s3_load_path())

        # Check the SQL query in `S3CopyToTable.does_table_exist`.
        mock_cursor.execute.assert_called_with("select 1 as table_exists "
                                               "from pg_table_def "
                                               "where tablename = lower(%s) limit 1",
                                               (task.table,))

        return

    @mock.patch("luigi.contrib.redshift.S3CopyToTable.does_table_exist",
                return_value=False)
    @mock.patch("luigi.contrib.redshift.RedshiftTarget")
    def test_s3_copy_to_missing_table(self,
                                      mock_redshift_target,
                                      mock_does_exist):
        """
        Test missing table creation
        """
        # Ensure `S3CopyToTable.create_table` does not throw an error.
        task = DummyS3CopyToTableKey()
        task.run()

        # Make sure the cursor was successfully used to create the table in
        # `create_table` as expected.
        mock_cursor = (mock_redshift_target.return_value
                                           .connect
                                           .return_value
                                           .cursor
                                           .return_value)
        assert mock_cursor.execute.call_args_list[0][0][0].startswith(
            "CREATE  TABLE %s" % task.table)

        return

    @mock.patch("luigi.contrib.redshift.S3CopyToTable.does_schema_exist", return_value=False)
    @mock.patch("luigi.contrib.redshift.RedshiftTarget")
    def test_s3_copy_to_missing_schema(self, mock_redshift_target, mock_does_exist):
        task = DummyS3CopyToTableKey(table='schema.table_with_schema')
        task.run()

        mock_cursor = (mock_redshift_target.return_value
                                           .connect
                                           .return_value
                                           .cursor
                                           .return_value)
        executed_query = mock_cursor.execute.call_args_list[0][0][0]
        assert executed_query.startswith("CREATE SCHEMA IF NOT EXISTS schema")

    @mock.patch("luigi.contrib.redshift.S3CopyToTable.does_schema_exist", return_value=False)
    @mock.patch("luigi.contrib.redshift.RedshiftTarget")
    def test_s3_copy_to_missing_schema_with_no_schema(self, mock_redshift_target, mock_does_exist):
        task = DummyS3CopyToTableKey(table='table_with_no_schema')
        task.run()

        mock_cursor = (mock_redshift_target.return_value
                                           .connect
                                           .return_value
                                           .cursor
                                           .return_value)
        executed_query = mock_cursor.execute.call_args_list[0][0][0]
        assert not executed_query.startswith("CREATE SCHEMA IF NOT EXISTS")

    @mock.patch("luigi.contrib.redshift.S3CopyToTable.does_schema_exist", return_value=True)
    @mock.patch("luigi.contrib.redshift.RedshiftTarget")
    def test_s3_copy_to_existing_schema_with_schema(self, mock_redshift_target, mock_does_exist):
        task = DummyS3CopyToTableKey(table='schema.table_with_schema')
        task.run()

        mock_cursor = (mock_redshift_target.return_value
                                           .connect
                                           .return_value
                                           .cursor
                                           .return_value)
        executed_query = mock_cursor.execute.call_args_list[0][0][0]
        assert not executed_query.startswith("CREATE SCHEMA IF NOT EXISTS")

    @mock.patch("luigi.contrib.redshift.S3CopyToTable.does_table_exist",
                return_value=False)
    @mock.patch("luigi.contrib.redshift.RedshiftTarget")
    def test_s3_copy_to_missing_table_with_compression_encodings(self,
                                                                 mock_redshift_target,
                                                                 mock_does_exist):
        """
        Test missing table creation with compression encodings
        """
        # Ensure `S3CopyToTable.create_table` does not throw an error.
        task = DummyS3CopyToTableWithCompressionEncodings()
        task.run()

        # Make sure the cursor was successfully used to create the table in
        # `create_table` as expected.
        mock_cursor = (mock_redshift_target.return_value
                                           .connect
                                           .return_value
                                           .cursor
                                           .return_value)
        encode_string = ','.join(
                '{name} {type} ENCODE {encoding}'.format(
                    name=name,
                    type=type,
                    encoding=encoding) for name, type, encoding in task.columns
            )

        assert mock_cursor.execute.call_args_list[0][0][0].startswith(
            "CREATE  TABLE %s (%s )" % (task.table, encode_string))

        return

    @mock.patch("luigi.contrib.redshift.S3CopyToTable.does_table_exist", return_value=False)
    @mock.patch("luigi.contrib.redshift.RedshiftTarget")
    def test_s3_copy_to_missing_table_with_table_constraints(self, mock_redshift_target, mock_does_exist):
        table_constraints = 'PRIMARY KEY (COL1, COL2)'

        task = DummyS3CopyToTableKey(table_constraints=table_constraints)

        task.run()

        mock_cursor = (mock_redshift_target.return_value
                                           .connect
                                           .return_value
                                           .cursor
                                           .return_value)
        columns_string = ','.join(
                '{name} {type}'.format(
                    name=name,
                    type=type) for name, type in task.columns
            )

        executed_query = mock_cursor.execute.call_args_list[0][0][0]
        expectation = "CREATE  TABLE %s (%s , PRIMARY KEY (COL1, COL2))" % (task.table, columns_string)

        assert executed_query.startswith(expectation)

    @mock.patch("luigi.contrib.redshift.S3CopyToTable.copy")
    @mock.patch("luigi.contrib.redshift.RedshiftTarget")
    def test_s3_copy_to_temp_table(self, mock_redshift_target, mock_copy):
        task = DummyS3CopyToTempTable()
        task.run()

        # The mocked connection cursor passed to
        # S3CopyToTable.copy(self, cursor, f).
        mock_cursor = (mock_redshift_target.return_value
                                           .connect
                                           .return_value
                                           .cursor
                                           .return_value)

        # `mock_redshift_target` is the mocked `RedshiftTarget` object
        # returned by S3CopyToTable.output(self).
        mock_redshift_target.assert_called_once_with(
            database=task.database,
            host=task.host,
            update_id=task.task_id,
            user=task.user,
            table=task.table,
            password=task.password,
        )

        # Check if the `S3CopyToTable.s3_load_path` class attribute was
        # successfully referenced in the `S3CopyToTable.run` method, which is
        # in-turn passed to `S3CopyToTable.copy` and other functions in `run`
        # (see issue #995).
        mock_copy.assert_called_once_with(mock_cursor, task.s3_load_path())

        # Check the SQL query in `S3CopyToTable.does_table_exist`. # temp table
        mock_cursor.execute.assert_any_call(
            "select 1 as table_exists "
            "from pg_table_def "
            "where tablename = lower(%s) limit 1",
            (task.table,),
        )

    @mock.patch("luigi.contrib.redshift.RedshiftTarget")
    def test_s3_copy_with_valid_columns(self, mock_redshift_target):
        task = DummyS3CopyToTableKey()
        task.run()

        # The mocked connection cursor passed to
        # S3CopyToTable.copy(self, cursor, f).
        mock_cursor = (mock_redshift_target.return_value
                                           .connect
                                           .return_value
                                           .cursor
                                           .return_value)

        # `mock_redshift_target` is the mocked `RedshiftTarget` object
        # returned by S3CopyToTable.output(self).
        mock_redshift_target.assert_called_once_with(
            database=task.database,
            host=task.host,
            update_id=task.task_id,
            user=task.user,
            table=task.table,
            password=task.password,
        )

        # To get the proper intendation in the multiline `COPY` statement the
        # SQL string was copied from redshift.py.
        mock_cursor.execute.assert_called_with("""
         COPY {table} {colnames} from '{source}'
         CREDENTIALS '{creds}'
         {options}
         ;""".format(
            table='dummy_table',
            colnames='(some_text,some_int)',
            source='s3://bucket/key',
            creds='aws_access_key_id=key;aws_secret_access_key=secret',
            options='')
        )

    @mock.patch("luigi.contrib.redshift.RedshiftTarget")
    def test_s3_copy_with_default_columns(self, mock_redshift_target):
        task = DummyS3CopyToTableKey(columns=[])
        task.run()

        # The mocked connection cursor passed to
        # S3CopyToTable.copy(self, cursor, f).
        mock_cursor = (mock_redshift_target.return_value
                                           .connect
                                           .return_value
                                           .cursor
                                           .return_value)

        # `mock_redshift_target` is the mocked `RedshiftTarget` object
        # returned by S3CopyToTable.output(self).
        mock_redshift_target.assert_called_once_with(
            database=task.database,
            host=task.host,
            update_id=task.task_id,
            user=task.user,
            table=task.table,
            password=task.password,
        )

        # To get the proper intendation in the multiline `COPY` statement the
        # SQL string was copied from redshift.py.
        mock_cursor.execute.assert_called_with("""
         COPY {table} {colnames} from '{source}'
         CREDENTIALS '{creds}'
         {options}
         ;""".format(
            table='dummy_table',
            colnames='',
            source='s3://bucket/key',
            creds='aws_access_key_id=key;aws_secret_access_key=secret',
            options='')
        )

    @mock.patch("luigi.contrib.redshift.RedshiftTarget")
    def test_s3_copy_with_nonetype_columns(self, mock_redshift_target):
        task = DummyS3CopyToTableKey(columns=None)
        task.run()

        # The mocked connection cursor passed to
        # S3CopyToTable.copy(self, cursor, f).
        mock_cursor = (mock_redshift_target.return_value
                                           .connect
                                           .return_value
                                           .cursor
                                           .return_value)

        # `mock_redshift_target` is the mocked `RedshiftTarget` object
        # returned by S3CopyToTable.output(self).
        mock_redshift_target.assert_called_once_with(
            database=task.database,
            host=task.host,
            update_id=task.task_id,
            user=task.user,
            table=task.table,
            password=task.password,
        )

        # To get the proper intendation in the multiline `COPY` statement the
        # SQL string was copied from redshift.py.
        mock_cursor.execute.assert_called_with("""
         COPY {table} {colnames} from '{source}'
         CREDENTIALS '{creds}'
         {options}
         ;""".format(
            table='dummy_table',
            colnames='',
            source='s3://bucket/key',
            creds='aws_access_key_id=key;aws_secret_access_key=secret',
            options='')
        )


@pytest.mark.aws
class TestS3CopyToSchemaTable(unittest.TestCase):
    @mock.patch("luigi.contrib.redshift.S3CopyToTable.copy")
    @mock.patch("luigi.contrib.redshift.RedshiftTarget")
    def test_s3_copy_to_table(self, mock_redshift_target, mock_copy):
        task = DummyS3CopyToTableKey(table='dummy_schema.dummy_table')
        task.run()

        # The mocked connection cursor passed to
        # S3CopyToTable.copy(self, cursor, f).
        mock_cursor = (mock_redshift_target.return_value
                                           .connect
                                           .return_value
                                           .cursor
                                           .return_value)

        # Check the SQL query in `S3CopyToTable.does_table_exist`.
        mock_cursor.execute.assert_called_with(
            "select 1 as table_exists "
            "from information_schema.tables "
            "where table_schema = lower(%s) and "
            "table_name = lower(%s) limit 1",
            tuple(task.table.split('.')),
        )


class DummyRedshiftUnloadTask(luigi.contrib.redshift.RedshiftUnloadTask):
    # Class attributes taken from `DummyPostgresImporter` in
    # `../postgres_test.py`.
    host = 'dummy_host'
    database = 'dummy_database'
    user = 'dummy_user'
    password = 'dummy_password'
    table = luigi.Parameter(default='dummy_table')
    columns = (
        ('some_text', 'varchar(255)'),
        ('some_int', 'int'),
    )

    aws_access_key_id = 'AWS_ACCESS_KEY'
    aws_secret_access_key = 'AWS_SECRET_KEY'

    s3_unload_path = 's3://%s/%s' % (BUCKET, KEY)
    unload_options = "DELIMITER ',' ADDQUOTES GZIP ALLOWOVERWRITE PARALLEL OFF"

    def query(self):
        return "SELECT 'a' as col_a, current_date as col_b"


@pytest.mark.aws
class TestRedshiftUnloadTask(unittest.TestCase):
    @mock.patch("luigi.contrib.redshift.RedshiftTarget")
    def test_redshift_unload_command(self, mock_redshift_target):

        task = DummyRedshiftUnloadTask()
        task.run()

        # The mocked connection cursor passed to
        # RedshiftUnloadTask.
        mock_cursor = (mock_redshift_target.return_value
                                           .connect
                                           .return_value
                                           .cursor
                                           .return_value)

        # Check the Unload query.
        mock_cursor.execute.assert_called_with(
            "UNLOAD ( 'SELECT \\'a\\' as col_a, current_date as col_b' ) TO 's3://bucket/key' "
            "credentials 'aws_access_key_id=AWS_ACCESS_KEY;aws_secret_access_key=AWS_SECRET_KEY' "
            "DELIMITER ',' ADDQUOTES GZIP ALLOWOVERWRITE PARALLEL OFF;"
        )


class DummyRedshiftAutocommitQuery(luigi.contrib.redshift.RedshiftQuery):
    # Class attributes taken from `DummyPostgresImporter` in
    # `../postgres_test.py`.
    host = 'dummy_host'
    database = 'dummy_database'
    user = 'dummy_user'
    password = 'dummy_password'
    table = luigi.Parameter(default='dummy_table')
    autocommit = True

    def query(self):
        return "SELECT 'a' as col_a, current_date as col_b"


@pytest.mark.aws
class TestRedshiftAutocommitQuery(unittest.TestCase):
    @mock.patch("luigi.contrib.redshift.RedshiftTarget")
    def test_redshift_autocommit_query(self, mock_redshift_target):

        task = DummyRedshiftAutocommitQuery()
        task.run()

        # The mocked connection cursor passed to
        # RedshiftUnloadTask.
        mock_connect = (mock_redshift_target.return_value
                                            .connect
                                            .return_value)

        # Check the Unload query.
        self.assertTrue(mock_connect.autocommit)


@pytest.mark.aws
class TestRedshiftManifestTask(unittest.TestCase):
    def test_run(self):
        with mock_s3():
            client = S3Client()
            client.s3.meta.client.create_bucket(Bucket=BUCKET)
            for key in FILES:
                k = '%s/%s' % (KEY, key)
                client.put_string('', 's3://%s/%s' % (BUCKET, k))
            folder_path = 's3://%s/%s' % (BUCKET, KEY)
            path = 's3://%s/%s/%s' % (BUCKET, 'manifest', 'test.manifest')
            folder_paths = [folder_path]

            m = mock.mock_open()
            with mock.patch('luigi.contrib.s3.S3Target.open', m, create=True):
                t = redshift.RedshiftManifestTask(path, folder_paths)
                luigi.build([t], local_scheduler=True)

            expected_manifest_output = json.dumps(
                generate_manifest_json(folder_paths, FILES))

            handle = m()
            handle.write.assert_called_with(expected_manifest_output)

    def test_run_multiple_paths(self):
        with mock_s3():
            client = S3Client()
            client.s3.meta.client.create_bucket(Bucket=BUCKET)
            for parent in [KEY, KEY_2]:
                for key in FILES:
                    k = '%s/%s' % (parent, key)
                    client.put_string('', 's3://%s/%s' % (BUCKET, k))
            folder_path_1 = 's3://%s/%s' % (BUCKET, KEY)
            folder_path_2 = 's3://%s/%s' % (BUCKET, KEY_2)
            folder_paths = [folder_path_1, folder_path_2]
            path = 's3://%s/%s/%s' % (BUCKET, 'manifest', 'test.manifest')

            m = mock.mock_open()
            with mock.patch('luigi.contrib.s3.S3Target.open', m, create=True):
                t = redshift.RedshiftManifestTask(path, folder_paths)
                luigi.build([t], local_scheduler=True)

            expected_manifest_output = json.dumps(
                generate_manifest_json(folder_paths, FILES))
            handle = m()
            handle.write.assert_called_with(expected_manifest_output)
