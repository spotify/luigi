# Copyright (c) 2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import datetime
import luigi
import luigi.contrib.postgres
from luigi.tools.range import RangeDaily
from helpers import unittest
import mock
import pytest


def datetime_to_epoch(dt):
    td = dt - datetime.datetime(1970, 1, 1)
    return td.days * 86400 + td.seconds + td.microseconds / 1E6


class MockContextManager(mock.Mock):

    def __init__(self, *args, **kwargs):
        super(MockContextManager, self).__init__(*args, **kwargs)
        self.context_counter = 0
        self.all_context_counter = 0

    def __enter__(self):
        self.context_counter += 1
        self.all_context_counter += 1

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.context_counter -= 1

    def _get_child_mock(self, /, **kwargs):
        """Child mocks will be instances of super."""
        return mock.Mock(**kwargs)


class MockPostgresConnection(MockContextManager):
    def __init__(self, existing_update_ids, *args, **kwargs):
        super(MockPostgresConnection, self).__init__(*args, **kwargs)
        self.existing = existing_update_ids
        self.is_open = False
        self.was_open = 0

    def cursor(self):
        self.is_open = True
        self.was_open = True
        return MockPostgresCursor(existing_update_ids=self.existing)

    def close(self):
        self.is_open = False


class MockPostgresCursor(MockContextManager):
    """
    Keeps state to simulate executing SELECT queries and fetching results.
    """
    def __init__(self, existing_update_ids, *args, **kwargs):
        super(MockPostgresCursor, self).__init__(*args, **kwargs)
        self.existing = existing_update_ids

    def execute(self, query, params):
        if query.startswith('SELECT 1 FROM table_updates'):
            self.fetchone_result = (1, ) if params[0] in self.existing else None
        else:
            self.fetchone_result = None

    def fetchone(self):
        return self.fetchone_result


class DummyPostgresImporter(luigi.contrib.postgres.CopyToTable):
    date = luigi.DateParameter()

    host = 'dummy_host'
    database = 'dummy_database'
    user = 'dummy_user'
    password = 'dummy_password'
    table = 'dummy_table'
    columns = (
        ('some_text', 'text'),
        ('some_int', 'int'),
    )


@pytest.mark.postgres
class DailyCopyToTableTest(unittest.TestCase):
    maxDiff = None

    @mock.patch('psycopg2.connect')
    def test_bulk_complete(self, mock_connect):
        mock_cursor = MockPostgresCursor([
            DummyPostgresImporter(date=datetime.datetime(2015, 1, 3)).task_id
        ])
        mock_connect.return_value.cursor.return_value = mock_cursor

        task = RangeDaily(of=DummyPostgresImporter,
                          start=datetime.date(2015, 1, 2),
                          now=datetime_to_epoch(datetime.datetime(2015, 1, 7)))
        actual = sorted([t.task_id for t in task.requires()])

        self.assertEqual(actual, sorted([
            DummyPostgresImporter(date=datetime.datetime(2015, 1, 2)).task_id,
            DummyPostgresImporter(date=datetime.datetime(2015, 1, 4)).task_id,
            DummyPostgresImporter(date=datetime.datetime(2015, 1, 5)).task_id,
            DummyPostgresImporter(date=datetime.datetime(2015, 1, 6)).task_id,
        ]))
        self.assertFalse(task.complete())

    @mock.patch('psycopg2.connect')
    @mock.patch("luigi.contrib.postgres.CopyToTable.rows", return_value=['row1', 'row2'])
    def test_cleanup_on_error(self, mock_rows, mock_connect):
        """
        Test cleanup behavior of CopyToTable in case of an error.

        When an error occured while the connection is open, it should be
        closed again so that subsequent tasks do not fail due to the unclosed
        connection.
        """
        task = DummyPostgresImporter(date=datetime.datetime(2021, 4, 15))

        mock_connection = MockPostgresConnection([task.task_id])
        mock_connect.return_value = mock_connection
        mock_cursor = MockPostgresCursor([task.task_id])

        original_cursor = mock_connection.cursor
        def get_mock_cursor():
            original_cursor()
            return mock_cursor

        mock_connection.cursor = mock.MagicMock(side_effect=get_mock_cursor)

        task = DummyPostgresImporter(date=datetime.datetime(2021, 4, 15))
        task.columns = [(42,)]  # inject defect

        with self.assertRaisesRegex(ValueError, "columns"):
            task.run()

        self.assertEqual(mock_connection.context_counter, 0)
        self.assertTrue(mock_connection.all_context_counter)
        self.assertFalse(mock_connection.is_open)
        self.assertTrue(mock_connection.was_open)

        self.assertEqual(mock_cursor.context_counter, 0)
        self.assertTrue(mock_cursor.all_context_counter)


class DummyPostgresQuery(luigi.contrib.postgres.PostgresQuery):
    date = luigi.DateParameter()

    host = 'dummy_host'
    database = 'dummy_database'
    user = 'dummy_user'
    password = 'dummy_password'
    table = 'dummy_table'
    columns = (
        ('some_text', 'text'),
        ('some_int', 'int'),
    )
    query = 'SELECT * FROM foo'


class DummyPostgresQueryWithPort(DummyPostgresQuery):
    port = 1234


class DummyPostgresQueryWithPortEncodedInHost(DummyPostgresQuery):
    host = 'dummy_host:1234'


@pytest.mark.postgres
class PostgresQueryTest(unittest.TestCase):
    maxDiff = None

    @mock.patch('psycopg2.connect')
    def test_bulk_complete(self, mock_connect):
        mock_cursor = MockPostgresCursor([
            'DummyPostgresQuery_2015_01_03_838e32a989'
        ])
        mock_connect.return_value.cursor.return_value = mock_cursor

        task = RangeDaily(of=DummyPostgresQuery,
                          start=datetime.date(2015, 1, 2),
                          now=datetime_to_epoch(datetime.datetime(2015, 1, 7)))
        actual = [t.task_id for t in task.requires()]

        self.assertEqual(actual, [
            'DummyPostgresQuery_2015_01_02_3a0ec498ed',
            'DummyPostgresQuery_2015_01_04_9c1d42ff62',
            'DummyPostgresQuery_2015_01_05_0f90e52357',
            'DummyPostgresQuery_2015_01_06_f91a47ec40',
        ])
        self.assertFalse(task.complete())

    def test_override_port(self):
        output = DummyPostgresQueryWithPort(date=datetime.datetime(1991, 3, 24)).output()
        self.assertEquals(output.port, 1234)

    def test_port_encoded_in_host(self):
        output = DummyPostgresQueryWithPortEncodedInHost(date=datetime.datetime(1991, 3, 24)).output()
        self.assertEquals(output.port, '1234')


@pytest.mark.postgres
class TestCopyToTableWithMetaColumns(unittest.TestCase):
    @mock.patch("luigi.contrib.postgres.CopyToTable.enable_metadata_columns", new_callable=mock.PropertyMock, return_value=True)
    @mock.patch("luigi.contrib.postgres.CopyToTable._add_metadata_columns")
    @mock.patch("luigi.contrib.postgres.CopyToTable.post_copy_metacolumns")
    @mock.patch("luigi.contrib.postgres.CopyToTable.rows", return_value=['row1', 'row2'])
    @mock.patch("luigi.contrib.postgres.PostgresTarget")
    @mock.patch('psycopg2.connect')
    def test_copy_with_metadata_columns_enabled(self,
                                                mock_connect,
                                                mock_redshift_target,
                                                mock_rows,
                                                mock_add_columns,
                                                mock_update_columns,
                                                mock_metadata_columns_enabled):

        task = DummyPostgresImporter(date=datetime.datetime(1991, 3, 24))

        mock_cursor = MockPostgresCursor([task.task_id])
        mock_connect.return_value.cursor.return_value = mock_cursor

        task = DummyPostgresImporter(date=datetime.datetime(1991, 3, 24))
        task.run()

        self.assertTrue(mock_add_columns.called)
        self.assertTrue(mock_update_columns.called)

    @mock.patch("luigi.contrib.postgres.CopyToTable.enable_metadata_columns", new_callable=mock.PropertyMock, return_value=False)
    @mock.patch("luigi.contrib.postgres.CopyToTable._add_metadata_columns")
    @mock.patch("luigi.contrib.postgres.CopyToTable.post_copy_metacolumns")
    @mock.patch("luigi.contrib.postgres.CopyToTable.rows", return_value=['row1', 'row2'])
    @mock.patch("luigi.contrib.postgres.PostgresTarget")
    @mock.patch('psycopg2.connect')
    def test_copy_with_metadata_columns_disabled(self,
                                                 mock_connect,
                                                 mock_redshift_target,
                                                 mock_rows,
                                                 mock_add_columns,
                                                 mock_update_columns,
                                                 mock_metadata_columns_enabled):

        task = DummyPostgresImporter(date=datetime.datetime(1991, 3, 24))

        mock_cursor = MockPostgresCursor([task.task_id])
        mock_connect.return_value.cursor.return_value = mock_cursor

        task.run()

        self.assertFalse(mock_add_columns.called)
        self.assertFalse(mock_update_columns.called)
