import os
import sys

import mock
from moto import mock_s3

import luigi
import luigi.contrib.snowflake
import luigi.notifications

from helpers import unittest, with_config

from nose.plugins.attrib import attr

if (3, 4, 0) <= sys.version_info[:3] < (3, 4, 3):
    # spulec/moto#308
    mock_s3 = unittest.skip('moto mock doesn\'t work with python3.4')  # NOQA


BUCKET = 'bucket'
KEY = 'key'


class DummyTarget(luigi.contrib.snowflake.SnowflakeTarget):
    pass


class DummyQueryTask(luigi.contrib.snowflake.SnowflakeQuery):
    host = 'dummy_host'
    database = 'dummy_database'
    user = 'dummy_user'
    password = 'dummy_password'
    table = luigi.Parameter(default='dummy_table')

    @property
    def query(self):
        return "SELECT 1 FROM table_1; SELECT 2 FROM table_2;"


class DummyS3CopyToTableBase(luigi.contrib.snowflake.S3CopyToTable):
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

    def s3_load_path(self):
        return 's3://%s/%s' % (BUCKET, KEY)


class DummyS3CopyToTableWithKeys(DummyS3CopyToTableBase):
    aws_access_key_id = 'property_key'
    aws_secret_access_key = 'property_secret'


@attr('contrib')
class TestInternalCredentials(unittest.TestCase, DummyS3CopyToTableWithKeys):
    def test_from_property(self):
        self.assertEqual(self.aws_access_key_id, 'property_key')
        self.assertEqual(self.aws_secret_access_key, 'property_secret')


@attr('contrib')
class TestExternalCredentials(unittest.TestCase, DummyS3CopyToTableBase):
    @mock.patch.dict(os.environ, {"AWS_ACCESS_KEY_ID": "env_key",
                                  "AWS_SECRET_ACCESS_KEY": "env_secret"})
    def test_from_env(self):
        self.assertEqual(self.aws_access_key_id, "env_key")
        self.assertEqual(self.aws_secret_access_key, "env_secret")

    @with_config({"snowflake": {"aws_access_key_id": "config_key",
                                "aws_secret_access_key": "config_secret",
                                "warehouse": "config_warehouse",
                                "role": "config_role"}})
    def test_from_config(self):
        self.assertEqual(self.aws_access_key_id, "config_key")
        self.assertEqual(self.aws_secret_access_key, "config_secret")
        self.assertEqual(self.warehouse, "config_warehouse")
        self.assertEqual(self.role, "config_role")


@attr('contrib')
class TestS3CopyToTable(unittest.TestCase):
    @mock.patch("luigi.contrib.snowflake.SnowflakeTarget")
    def test_copy_missing_creds(self, mock_snowflake_target):

        # Make sure credentials are not set as env vars
        try:
            del os.environ['AWS_ACCESS_KEY_ID']
            del os.environ['AWS_SECRET_ACCESS_KEY']
        except KeyError:
            pass

        task = DummyS3CopyToTableBase()

        # The mocked connection cursor passed to
        # S3CopyToTable.copy(self, cursor, f).
        mock_cursor = (mock_snowflake_target.return_value
                       .connect
                       .return_value
                       .cursor
                       .return_value)

        with self.assertRaises(NotImplementedError):
            task.copy(mock_cursor, task.s3_load_path())

    @mock.patch("luigi.contrib.snowflake.S3CopyToTable.copy")
    @mock.patch("luigi.contrib.snowflake.SnowflakeTarget")
    def test_s3_copy_to_table(self, mock_snowflake_target, mock_copy):
        task = DummyS3CopyToTableWithKeys()
        task.run()

        # The mocked connection cursor passed to
        # S3CopyToTable.copy(self, cursor, f).
        mock_cursor = (mock_snowflake_target.return_value
                                            .connect
                                            .return_value
                                            .cursor
                                            .return_value)

        # `mock_snowflake_target` is the mocked `SnowflakeTarget` object
        # returned by S3CopyToTable.output(self).
        mock_snowflake_target.assert_called_with(database=task.database,
                                                 warehouse=task.warehouse,
                                                 host=task.host,
                                                 update_id=task.task_id,
                                                 user=task.user,
                                                 table=task.table,
                                                 password=task.password,
                                                 role=task.role)

        # Check if the `S3CopyToTable.s3_load_path` class attribute was
        # successfully referenced in the `S3CopyToTable.run` method, which is
        # in-turn passed to `S3CopyToTable.copy` and other functions in `run`
        # (see issue #995).
        mock_copy.assert_called_with(mock_cursor, task.s3_load_path())

    @mock.patch("luigi.contrib.snowflake.S3CopyToTable.does_table_exist",
                return_value=False)
    @mock.patch("luigi.contrib.snowflake.SnowflakeTarget")
    def test_s3_copy_to_missing_table(self,
                                      mock_snowflake_target,
                                      mock_does_exist):
        """
        Test missing table creation
        """
        # Ensure `S3CopyToTable.create_table` does not throw an error.
        task = DummyS3CopyToTableWithKeys()
        task.run()

        # Make sure the cursor was successfully used to create the table in
        # `create_table` as expected.
        mock_cursor = (mock_snowflake_target.return_value
                                            .connect
                                            .return_value
                                            .cursor
                                            .return_value)
        assert mock_cursor.execute.call_args_list[0][0][0].startswith(
            "CREATE TABLE %s" % task.table)

    @mock.patch("luigi.contrib.snowflake.S3CopyToTable.does_schema_exist", return_value=False)
    @mock.patch("luigi.contrib.snowflake.SnowflakeTarget")
    def test_s3_copy_to_missing_schema(self, mock_snowflake_target, mock_does_exist):
        task = DummyS3CopyToTableWithKeys(table='schema.table_with_schema')
        task.run()

        mock_cursor = (mock_snowflake_target.return_value
                                            .connect
                                            .return_value
                                            .cursor
                                            .return_value)
        executed_query = mock_cursor.execute.call_args_list[0][0][0]
        assert executed_query.startswith("CREATE SCHEMA IF NOT EXISTS schema")

    @mock.patch("luigi.contrib.snowflake.S3CopyToTable.does_schema_exist", return_value=False)
    @mock.patch("luigi.contrib.snowflake.SnowflakeTarget")
    def test_s3_copy_to_missing_schema_with_no_schema(self, mock_snowflake_target, mock_does_exist):
        task = DummyS3CopyToTableWithKeys(table='table_with_no_schema')
        task.run()

        mock_cursor = (mock_snowflake_target.return_value
                                            .connect
                                            .return_value
                                            .cursor
                                            .return_value)
        executed_query = mock_cursor.execute.call_args_list[0][0][0]
        assert not executed_query.startswith("CREATE SCHEMA IF NOT EXISTS")

    @mock.patch("luigi.contrib.snowflake.S3CopyToTable.does_schema_exist", return_value=True)
    @mock.patch("luigi.contrib.snowflake.SnowflakeTarget")
    def test_s3_copy_to_existing_schema_with_schema(self, mock_snowflake_target, mock_does_exist):
        task = DummyS3CopyToTableWithKeys(table='schema.table_with_schema')
        task.run()

        mock_cursor = (mock_snowflake_target.return_value
                                            .connect
                                            .return_value
                                            .cursor
                                            .return_value)
        executed_query = mock_cursor.execute.call_args_list[0][0][0]
        assert not executed_query.startswith("CREATE SCHEMA IF NOT EXISTS")

    @mock.patch("luigi.contrib.snowflake.SnowflakeTarget")
    def test_s3_copy_with_valid_columns(self, mock_snowflake_target):
        task = DummyS3CopyToTableWithKeys()
        task.run()

        # The mocked connection cursor passed to
        # S3CopyToTable.copy(self, cursor, f).
        mock_cursor = (mock_snowflake_target.return_value
                                            .connect
                                            .return_value
                                            .cursor
                                            .return_value)

        # `mock_snowflake_target` is the mocked `SnowflakeTarget` object
        # returned by S3CopyToTable.output(self).
        mock_snowflake_target.assert_called_once_with(
            database=task.database,
            warehouse=task.warehouse,
            host=task.host,
            update_id=task.task_id,
            user=task.user,
            table=task.table,
            password=task.password,
            role=task.role,
        )

        # To get the proper intendation in the multiline `COPY` statement the
        # SQL string was copied from snowflake.py.
        mock_cursor.execute.assert_called_with("COPY INTO {table} from {source} CREDENTIALS=({creds}) {options};".format(
                table='dummy_table',
                colnames='(some_text,some_int)',
                source='s3://bucket/key',
                creds="AWS_KEY_ID='property_key' AWS_SECRET_KEY='property_secret'",
                options='')
            )

    @mock.patch("luigi.contrib.snowflake.SnowflakeTarget")
    def test_s3_copy_with_default_columns(self, mock_snowflake_target):
        task = DummyS3CopyToTableWithKeys(columns=[])
        task.run()

        # The mocked connection cursor passed to
        # S3CopyToTable.copy(self, cursor, f).
        mock_cursor = (mock_snowflake_target.return_value
                                            .connect
                                            .return_value
                                            .cursor
                                            .return_value)

        # `mock_snowflake_target` is the mocked `SnowflakeTarget` object
        # returned by S3CopyToTable.output(self).
        mock_snowflake_target.assert_called_once_with(
            database=task.database,
            warehouse=task.warehouse,
            host=task.host,
            update_id=task.task_id,
            user=task.user,
            table=task.table,
            password=task.password,
            role=task.role,
        )

        # To get the proper intendation in the multiline `COPY` statement the
        # SQL string was copied from snowflake.py.
        mock_cursor.execute.assert_called_with("COPY INTO {table} from {source} CREDENTIALS=({creds}) {options};".format(
                table='dummy_table',
                source='s3://bucket/key',
                creds="AWS_KEY_ID='property_key' AWS_SECRET_KEY='property_secret'",
                options='')
            )

    @mock.patch("luigi.contrib.snowflake.SnowflakeTarget")
    def test_s3_copy_with_nonetype_columns(self, mock_snowflake_target):
        task = DummyS3CopyToTableWithKeys(columns=None)
        task.run()

        # The mocked connection cursor passed to
        # S3CopyToTable.copy(self, cursor, f).
        mock_cursor = (mock_snowflake_target.return_value
                                            .connect
                                            .return_value
                                            .cursor
                                            .return_value)

        # `mock_snowflake_target` is the mocked `SnowflakeTarget` object
        # returned by S3CopyToTable.output(self).
        mock_snowflake_target.assert_called_once_with(
            database=task.database,
            warehouse=task.warehouse,
            host=task.host,
            update_id=task.task_id,
            user=task.user,
            table=task.table,
            password=task.password,
            role=task.role,
        )

        mock_cursor.execute.assert_called_with("COPY INTO {table} from {source} CREDENTIALS=({creds}) {options};".format(
                table='dummy_table',
                colnames='',
                source='s3://bucket/key',
                creds="AWS_KEY_ID='property_key' AWS_SECRET_KEY='property_secret'",
                options='')
            )

    @mock.patch("luigi.contrib.snowflake.S3CopyToTable.do_truncate_table",
                return_value=True)
    @mock.patch("luigi.contrib.snowflake.SnowflakeTarget")
    def test_s3_copy_to_truncate_table(self,
                                       mock_snowflake_target,
                                       mock_does_exist):
        """
        Test truncate table
        """
        # Ensure `S3CopyToTable.create_table` does not throw an error.
        task = DummyS3CopyToTableWithKeys()
        task.run()

        # Make sure the cursor was successfully used to create the table in
        # `create_table` as expected.
        mock_cursor = (mock_snowflake_target.return_value
                                            .connect
                                            .return_value
                                            .cursor
                                            .return_value)
        assert mock_cursor.execute.call_args_list[1][0][0].startswith(
            "truncate %s" % task.table)


@attr('contrib')
class TestSnowflakeTarget(unittest.TestCase):
    target_settings = {'host': 'dummy_host',
                       'warehouse': 'dummy_warehouse',
                       'database': 'dummy_database',
                       'user': 'dummy_user',
                       'password': 'dummy_password',
                       'table': 'dummy_table',
                       'update_id': 'dummy_update_id',
                       'role': 'dummy_role'}

    @mock.patch("snowflake.connector")
    def test_target_execute_role(self, mock_snowflake):
        target = DummyTarget(**self.target_settings)
        target.connect()

        mock_cursor = (mock_snowflake.connect.return_value.cursor.return_value)

        assert mock_cursor.execute.call_args_list[0][0][0].startswith("USE ROLE %s" % target.role)

    @mock.patch("snowflake.connector")
    def test_target_execute_warehouse(self, mock_snowflake):
        target = DummyTarget(**self.target_settings)
        target.connect()

        mock_cursor = (mock_snowflake.connect.return_value.cursor.return_value)

        assert mock_cursor.execute.call_args_list[1][0][0].startswith("USE WAREHOUSE %s" % target.warehouse)

    @mock.patch("snowflake.connector")
    def test_target_execute_database(self, mock_snowflake):
        target = DummyTarget(**self.target_settings)
        target.connect()

        mock_cursor = (mock_snowflake.connect.return_value.cursor.return_value)

        assert mock_cursor.execute.call_args_list[2][0][0].startswith("USE DATABASE %s" % target.database)

    @mock.patch("snowflake.connector")
    def test_target_exists(self, mock_snowflake):
        target = DummyTarget(**self.target_settings)
        connection = target.connect()

        target.exists(connection=connection)

        mock_cursor = (mock_snowflake.connect.return_value.cursor.return_value)

        assert mock_cursor.execute.call_args_list[3][0][0] == "SELECT 1 FROM table_updates WHERE update_id = '%s' LIMIT 1" % target.update_id

    @mock.patch("snowflake.connector")
    def test_target_create_marker_table(self, mock_snowflake):
        target = DummyTarget(**self.target_settings)
        target.create_marker_table()

        mock_cursor = (mock_snowflake.connect.return_value.cursor.return_value)

        sql = """CREATE TABLE table_updates (
                     update_id TEXT PRIMARY KEY,
                     target_table TEXT,
                     inserted TIMESTAMP);"""

        assert mock_cursor.execute.call_args_list[3][0][0] == sql


@attr('contrib')
class TestSnowflakeQuery(unittest.TestCase):
    @mock.patch("luigi.contrib.snowflake.SnowflakeTarget")
    def test_execute_both_queries(self, mock_snowflake_target):
        task = DummyQueryTask()
        task.run()

        mock_connect = (mock_snowflake_target.return_value
                                             .connect
                                             .return_value)

        mock_connect.execute_string.assert_called_with("SELECT 1 FROM table_1; SELECT 2 FROM table_2;", return_cursors=False)
