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
import tempfile
import os
import luigi
import luigi.contrib.duckdb
from luigi.tools.range import RangeDaily
from helpers import unittest
import mock
import pytest


def datetime_to_epoch(dt):
    td = dt - datetime.datetime(1970, 1, 1)
    return td.days * 86400 + td.seconds + td.microseconds / 1E6


class MockDuckDBCursor:
    """
    Mock cursor to simulate DuckDB query execution.
    """
    def __init__(self, existing_update_ids):
        self.existing = existing_update_ids
        self.fetchone_result = None

    def execute(self, query, params=None):
        if params and 'SELECT 1 FROM table_updates' in query:
            self.fetchone_result = (1,) if params[0] in self.existing else None
        else:
            self.fetchone_result = None
        return self

    def fetchone(self):
        return self.fetchone_result


class DummyDuckDBImporter(luigi.contrib.duckdb.CopyToTable):
    date = luigi.DateParameter()

    database = ':memory:'
    table = 'dummy_table'
    columns = (
        ('some_text', 'VARCHAR'),
        ('some_int', 'INTEGER'),
    )

    def rows(self):
        yield ('test_text', 123)


@pytest.mark.duckdb
class DuckDBTargetTest(unittest.TestCase):
    """Test DuckDBTarget functionality."""

    def test_target_creation(self):
        """Test that a DuckDBTarget can be created."""
        target = luigi.contrib.duckdb.DuckDBTarget(
            database=':memory:',
            table='test_table',
            update_id='test_update_id'
        )
        self.assertEqual(target.database, ':memory:')
        self.assertEqual(target.table, 'test_table')
        self.assertEqual(target.update_id, 'test_update_id')

    def test_target_str(self):
        """Test string representation of target."""
        target = luigi.contrib.duckdb.DuckDBTarget(
            database='/path/to/db.duckdb',
            table='test_table',
            update_id='test_update_id'
        )
        self.assertEqual(str(target), '/path/to/db.duckdb:test_table')

    def test_marker_table_creation(self):
        """Test that marker table is created correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, 'test.duckdb')
            target = luigi.contrib.duckdb.DuckDBTarget(
                database=db_path,
                table='test_table',
                update_id='test_id'
            )

            target.create_marker_table()

            # Verify table exists
            conn = target.connect()
            result = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='table_updates'"
            ).fetchone()
            # DuckDB uses information_schema instead of sqlite_master
            result = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name='table_updates'"
            ).fetchone()
            conn.close()

            self.assertIsNotNone(result)

    def test_touch_and_exists(self):
        """Test touching a target and checking existence."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, 'test.duckdb')
            target = luigi.contrib.duckdb.DuckDBTarget(
                database=db_path,
                table='test_table',
                update_id='test_id'
            )

            # Should not exist initially
            self.assertFalse(target.exists())

            # Touch the target
            target.touch()

            # Should exist now
            self.assertTrue(target.exists())


@pytest.mark.duckdb
class DuckDBCopyToTableTest(unittest.TestCase):
    """Test CopyToTable functionality."""

    def test_copy_to_table_attributes(self):
        """Test that task has correct attributes."""
        task = DummyDuckDBImporter(date=datetime.datetime(2015, 1, 1))
        self.assertEqual(task.database, ':memory:')
        self.assertEqual(task.table, 'dummy_table')
        self.assertEqual(len(task.columns), 2)

    def test_output_returns_target(self):
        """Test that output returns a DuckDBTarget."""
        task = DummyDuckDBImporter(date=datetime.datetime(2015, 1, 1))
        output = task.output()
        self.assertIsInstance(output, luigi.contrib.duckdb.DuckDBTarget)
        self.assertEqual(output.table, 'dummy_table')

    @mock.patch('duckdb.connect')
    def test_bulk_complete(self, mock_connect):
        """Test bulk completion checking."""
        mock_cursor = MockDuckDBCursor([
            DummyDuckDBImporter(date=datetime.datetime(2015, 1, 3)).task_id
        ])
        mock_connect.return_value.execute = mock_cursor.execute
        mock_connect.return_value.close = mock.Mock()

        task = RangeDaily(
            of=DummyDuckDBImporter,
            start=datetime.date(2015, 1, 2),
            now=datetime_to_epoch(datetime.datetime(2015, 1, 7))
        )
        actual = sorted([t.task_id for t in task.requires()])

        self.assertEqual(actual, sorted([
            DummyDuckDBImporter(date=datetime.datetime(2015, 1, 2)).task_id,
            DummyDuckDBImporter(date=datetime.datetime(2015, 1, 4)).task_id,
            DummyDuckDBImporter(date=datetime.datetime(2015, 1, 5)).task_id,
            DummyDuckDBImporter(date=datetime.datetime(2015, 1, 6)).task_id,
        ]))
        self.assertFalse(task.complete())


class DummyDuckDBQuery(luigi.contrib.duckdb.DuckDBQuery):
    date = luigi.DateParameter()

    database = ':memory:'
    table = 'dummy_table'
    query = 'SELECT * FROM dummy_table'


@pytest.mark.duckdb
class DuckDBQueryTest(unittest.TestCase):
    """Test DuckDBQuery functionality."""

    def test_query_attributes(self):
        """Test that query task has correct attributes."""
        task = DummyDuckDBQuery(date=datetime.datetime(2015, 1, 1))
        self.assertEqual(task.database, ':memory:')
        self.assertEqual(task.table, 'dummy_table')
        self.assertEqual(task.query, 'SELECT * FROM dummy_table')

    def test_output_returns_target(self):
        """Test that output returns a DuckDBTarget."""
        task = DummyDuckDBQuery(date=datetime.datetime(2015, 1, 1))
        output = task.output()
        self.assertIsInstance(output, luigi.contrib.duckdb.DuckDBTarget)

    @mock.patch('duckdb.connect')
    def test_bulk_complete(self, mock_connect):
        """Test bulk completion for queries."""
        mock_cursor = MockDuckDBCursor([
            'DummyDuckDBQuery_2015_01_03_838e32a989'
        ])
        mock_connect.return_value.execute = mock_cursor.execute
        mock_connect.return_value.close = mock.Mock()

        task = RangeDaily(
            of=DummyDuckDBQuery,
            start=datetime.date(2015, 1, 2),
            now=datetime_to_epoch(datetime.datetime(2015, 1, 7))
        )
        actual = [t.task_id for t in task.requires()]

        self.assertEqual(actual, [
            'DummyDuckDBQuery_2015_01_02_3a0ec498ed',
            'DummyDuckDBQuery_2015_01_04_9c1d42ff62',
            'DummyDuckDBQuery_2015_01_05_0f90e52357',
            'DummyDuckDBQuery_2015_01_06_f91a47ec40',
        ])
        self.assertFalse(task.complete())


@pytest.mark.duckdb
class TestCopyToTableWithMetaColumns(unittest.TestCase):
    """Test metadata columns functionality."""

    @mock.patch("luigi.contrib.duckdb.CopyToTable.enable_metadata_columns",
                new_callable=mock.PropertyMock, return_value=True)
    @mock.patch("luigi.contrib.duckdb.CopyToTable._add_metadata_columns")
    @mock.patch("luigi.contrib.duckdb.CopyToTable.post_copy_metacolumns")
    @mock.patch("luigi.contrib.duckdb.CopyToTable.rows", return_value=[('row1', 1), ('row2', 2)])
    @mock.patch("luigi.contrib.duckdb.DuckDBTarget")
    @mock.patch('duckdb.connect')
    def test_copy_with_metadata_columns_enabled(self,
                                                mock_connect,
                                                mock_target,
                                                mock_rows,
                                                mock_post_copy,
                                                mock_add_columns,
                                                mock_metadata_enabled):
        """Test that metadata columns are added when enabled."""
        task = DummyDuckDBImporter(date=datetime.datetime(1991, 3, 24))

        mock_cursor = MockDuckDBCursor([task.task_id])
        mock_conn = mock.Mock()
        mock_conn.execute = mock_cursor.execute
        mock_conn.executemany = mock.Mock()
        mock_conn.close = mock.Mock()
        mock_connect.return_value = mock_conn

        task.run()

        self.assertTrue(mock_add_columns.called)
        self.assertTrue(mock_post_copy.called)

    @mock.patch("luigi.contrib.duckdb.CopyToTable.enable_metadata_columns",
                new_callable=mock.PropertyMock, return_value=False)
    @mock.patch("luigi.contrib.duckdb.CopyToTable._add_metadata_columns")
    @mock.patch("luigi.contrib.duckdb.CopyToTable.post_copy_metacolumns")
    @mock.patch("luigi.contrib.duckdb.CopyToTable.rows", return_value=[('row1', 1), ('row2', 2)])
    @mock.patch("luigi.contrib.duckdb.DuckDBTarget")
    @mock.patch('duckdb.connect')
    def test_copy_with_metadata_columns_disabled(self,
                                                 mock_connect,
                                                 mock_target,
                                                 mock_rows,
                                                 mock_post_copy,
                                                 mock_add_columns,
                                                 mock_metadata_enabled):
        """Test that metadata columns are not added when disabled."""
        task = DummyDuckDBImporter(date=datetime.datetime(1991, 3, 24))

        mock_cursor = MockDuckDBCursor([task.task_id])
        mock_conn = mock.Mock()
        mock_conn.execute = mock_cursor.execute
        mock_conn.executemany = mock.Mock()
        mock_conn.close = mock.Mock()
        mock_connect.return_value = mock_conn

        task.run()

        self.assertFalse(mock_add_columns.called)
        self.assertFalse(mock_post_copy.called)
