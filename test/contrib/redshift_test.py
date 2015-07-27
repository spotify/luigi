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

from helpers import unittest


# Fake AWS and S3 credentials taken from `../redshift_test.py`.
AWS_ACCESS_KEY = 'key'
AWS_SECRET_KEY = 'secret'

BUCKET = 'bucket'
KEY = 'key'


class DummyS3CopyToTable(luigi.contrib.redshift.S3CopyToTable):

    # Class attributes taken from `DummyPostgresImporter` in
    # `../postgres_test.py`.
    host = 'dummy_host'
    database = 'dummy_database'
    user = 'dummy_user'
    password = 'dummy_password'
    table = luigi.Parameter(default='dummy_table')
    columns = (
        ('some_text', 'text'),
        ('some_int', 'int'),
    )

    aws_access_key_id = AWS_ACCESS_KEY
    aws_secret_access_key = AWS_SECRET_KEY
    copy_options = ''

    def s3_load_path(self):
        return 's3://%s/%s' % (BUCKET, KEY)


class TestS3CopyToTable(unittest.TestCase):
    @mock.patch("luigi.contrib.redshift.S3CopyToTable.copy")
    @mock.patch("luigi.contrib.redshift.RedshiftTarget")
    def test_s3_copy_to_table(self, mock_redshift_target, mock_copy):
        task = DummyS3CopyToTable()
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
                                                update_id='DummyS3CopyToTable(table=dummy_table)',
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


class TestS3CopyToSchemaTable(unittest.TestCase):
    @mock.patch("luigi.contrib.redshift.S3CopyToTable.copy")
    @mock.patch("luigi.contrib.redshift.RedshiftTarget")
    def test_s3_copy_to_table(self, mock_redshift_target, mock_copy):
        task = DummyS3CopyToTable(table='dummy_schema.dummy_table')
        task.run()

        # The mocked connection cursor passed to
        # S3CopyToTable.copy(self, cursor, f).
        mock_cursor = (mock_redshift_target.return_value
                                           .connect
                                           .return_value
                                           .cursor
                                           .return_value)

        # Check the SQL query in `S3CopyToTable.does_table_exist`.
        mock_cursor.execute.assert_called_with("select 1 as table_exists "
                                               "from information_schema.tables "
                                               "where table_schema = %s and "
                                               "table_name = %s limit 1",
                                               tuple(task.table.split('.')))

        return
