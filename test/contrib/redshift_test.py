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

import luigi
import luigi.contrib.redshift
import mock

import unittest


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
    columns = (
        ('some_text', 'varchar(255)'),
        ('some_int', 'int'),
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


class TestS3CopyToTable(unittest.TestCase):
    @mock.patch("luigi.contrib.redshift.RedshiftTarget")
    def test_copy_missing_creds(self, mock_redshift_target):
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
                                               "where tablename = %s limit 1",
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
            "where tablename = %s limit 1",
            (task.table,),
        )


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
            "where table_schema = %s and "
            "table_name = %s limit 1",
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
