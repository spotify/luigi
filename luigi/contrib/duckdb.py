# -*- coding: utf-8 -*-
#
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
#
"""
Implements a Target for DuckDB and tasks for interacting with DuckDB databases.

DuckDB is an in-process SQL OLAP database management system. It is designed to support
analytical query workloads, also known as online analytical processing (OLAP).

This module provides:
- DuckDBTarget: Target for tracking task completion in a DuckDB database
- CopyToTable: Task template for bulk loading data into DuckDB tables
- DuckDBQuery: Task template for executing queries against DuckDB

Usage:

.. code-block:: python

    import luigi
    from luigi.contrib import duckdb

    class MyDuckDBTask(duckdb.CopyToTable):
        database = '/path/to/database.duckdb'
        table = 'my_table'
        columns = [
            ('id', 'INTEGER'),
            ('name', 'VARCHAR'),
            ('value', 'DOUBLE')
        ]

        def rows(self):
            # Yield rows to insert
            yield (1, 'Alice', 10.5)
            yield (2, 'Bob', 20.3)
"""

import datetime
import logging
import tempfile

import luigi
from luigi.contrib import rdbms

logger = logging.getLogger('luigi-interface')

try:
    import duckdb as duckdb_module
except ImportError:
    logger.warning("Loading DuckDB module without the python package duckdb. "
                   "This will crash at runtime if DuckDB functionality is used.")


class DuckDBTarget(luigi.Target):
    """
    Target for a resource in DuckDB.

    DuckDB is an embedded database, so it uses a file path instead of host/port.
    This target tracks task completion using a marker table in the database.
    """

    marker_table = luigi.configuration.get_config().get('duckdb', 'marker-table', 'table_updates')

    def __init__(self, database, table, update_id, **connect_kwargs):
        """
        Initializes a DuckDBTarget instance.

        Args:
            database (str): Path to the DuckDB database file. Use ':memory:' for in-memory database.
            table (str): Name of the table this target represents.
            update_id (str): An identifier for this data set (usually task_id).
            connect_kwargs: Additional keyword arguments passed to duckdb.connect().
        """
        self.database = database
        self.table = table
        self.update_id = update_id
        self.connect_kwargs = connect_kwargs

    def __str__(self):
        return f"{self.database}:{self.table}"

    def touch(self, connection=None):
        """
        Mark this update as complete by inserting a record into the marker table.

        If the marker table doesn't exist, it will be created automatically.

        Args:
            connection: Optional DuckDB connection. If None, a new connection is created.
        """
        self.create_marker_table()

        if connection is None:
            connection = self.connect()
            should_close = True
        else:
            should_close = False

        try:
            connection.execute(
                f"""INSERT INTO {self.marker_table} (update_id, target_table, inserted)
                   VALUES (?, ?, ?)
                   ON CONFLICT (update_id) DO UPDATE SET
                   target_table = EXCLUDED.target_table,
                   inserted = EXCLUDED.inserted
                """,
                (self.update_id, self.table, datetime.datetime.now())
            )
            # Verify the update was recorded
            assert self.exists(connection)
        finally:
            if should_close:
                connection.close()

    def exists(self, connection=None):
        """
        Check if this target has been marked as complete.

        Args:
            connection: Optional DuckDB connection. If None, a new connection is created.

        Returns:
            bool: True if the target exists (task has been completed), False otherwise.
        """
        if connection is None:
            connection = self.connect()
            should_close = True
        else:
            should_close = False

        try:
            cursor = connection.execute(
                f"""SELECT 1 FROM {self.marker_table}
                   WHERE update_id = ?
                   LIMIT 1
                """,
                (self.update_id,)
            )
            row = cursor.fetchone()
            return row is not None
        except duckdb_module.CatalogException:
            # Table doesn't exist yet
            return False
        finally:
            if should_close:
                connection.close()

    def connect(self):
        """
        Get a connection to the DuckDB database.

        Returns:
            duckdb.DuckDBPyConnection: Connection to the database.
        """
        connection = duckdb_module.connect(database=self.database, **self.connect_kwargs)
        return connection

    def create_marker_table(self):
        """
        Create the marker table if it doesn't exist.

        The marker table tracks which tasks have been completed.
        """
        connection = self.connect()
        try:
            connection.execute(
                f"""CREATE TABLE IF NOT EXISTS {self.marker_table} (
                       update_id VARCHAR PRIMARY KEY,
                       target_table VARCHAR,
                       inserted TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                   )
                """
            )
        finally:
            connection.close()


class CopyToTable(rdbms.CopyToTable):
    """
    Template task for inserting a data set into DuckDB.

    Usage:
        Subclass and override the required `database`, `table` and `columns` attributes.

        To customize how to access data from an input task, override the `rows` method
        with a generator that yields each row as a tuple with fields ordered according to `columns`.

    Example:

    .. code-block:: python

        class LoadData(duckdb.CopyToTable):
            database = '/path/to/data.duckdb'
            table = 'events'
            columns = [
                ('event_id', 'INTEGER'),
                ('event_name', 'VARCHAR'),
                ('timestamp', 'TIMESTAMP')
            ]

            def rows(self):
                with self.input().open('r') as f:
                    for line in f:
                        parts = line.strip().split('\\t')
                        yield parts
    """

    @property
    def database(self):
        """
        Path to the DuckDB database file.

        Use ':memory:' for an in-memory database.
        Override this in your task.
        """
        raise NotImplementedError("database path must be specified")

    # Override host, user, password from parent to make them optional for DuckDB
    @property
    def host(self):
        """Not applicable for DuckDB (embedded database)."""
        return None

    @property
    def user(self):
        """Not applicable for DuckDB (embedded database)."""
        return None

    @property
    def password(self):
        """Not applicable for DuckDB (embedded database)."""
        return None

    def rows(self):
        """
        Return/yield tuples or lists corresponding to each row to be inserted.

        Override this method to customize how data is read from input.

        Yields:
            tuple: A tuple of values for each row to insert.
        """
        with self.input().open('r') as fobj:
            for line in fobj:
                yield line.strip('\n').split('\t')

    def output(self):
        """
        Returns a DuckDBTarget representing the inserted dataset.

        Normally you don't override this.

        Returns:
            DuckDBTarget: Target representing this task's output.
        """
        return DuckDBTarget(
            database=self.database,
            table=self.table,
            update_id=self.update_id
        )

    def copy(self, cursor, file):
        """
        Copy data from file to DuckDB table using bulk insert.

        Args:
            cursor: DuckDB connection (DuckDB doesn't use cursors like other databases).
            file: File-like object containing the data to copy.
        """
        # Get column names
        if isinstance(self.columns[0], str):
            column_names = self.columns
        elif len(self.columns[0]) == 2:
            column_names = [c[0] for c in self.columns]
        else:
            raise Exception('columns must consist of column strings or (column string, type string) tuples')

        # DuckDB supports reading from CSV/TSV files directly
        # We'll use the COPY command for efficient bulk loading
        file.seek(0)

        # Create a temporary CSV/TSV for DuckDB to read
        # DuckDB's read_csv is very efficient
        try:
            # Try using INSERT with VALUES for each row
            # This is less efficient but works reliably
            placeholders = ','.join(['?' for _ in column_names])
            insert_sql = f"INSERT INTO {self.table} ({','.join(column_names)}) VALUES ({placeholders})"

            batch = []
            batch_size = 10000

            for row in self.rows():
                batch.append(row)
                if len(batch) >= batch_size:
                    cursor.executemany(insert_sql, batch)
                    batch = []

            # Insert remaining rows
            if batch:
                cursor.executemany(insert_sql, batch)

        except Exception as e:
            logger.error(f"Error during copy: {e}")
            raise

    def run(self):
        """
        Inserts data generated by rows() into target table.

        If the target table doesn't exist, self.create_table will be called
        to attempt to create the table.

        Normally you don't want to override this.
        """
        if not (self.table and self.columns):
            raise Exception("table and columns need to be specified")

        connection = self.output().connect()

        try:
            # Attempt to copy the data into DuckDB
            # If it fails because the target table doesn't exist,
            # try to create it by running self.create_table
            for attempt in range(2):
                try:
                    self.init_copy(connection)
                    self.copy(connection, None)
                    self.post_copy(connection)
                    if self.enable_metadata_columns:
                        self.post_copy_metacolumns(connection)
                except duckdb_module.CatalogException as e:
                    if 'does not exist' in str(e).lower() and attempt == 0:
                        # If first attempt fails with table not found, try creating table
                        logger.info("Creating table %s", self.table)
                        self.create_table(connection)
                    else:
                        raise
                else:
                    break

            # Mark as complete
            self.output().touch(connection)

        finally:
            connection.close()


class DuckDBQuery(rdbms.Query):
    """
    Template task for querying a DuckDB database.

    Usage:
        Subclass and override the required `database`, `table`, and `query` attributes.

        Override the `run` method if your use case requires some action with the query result.

    Task instances require a dynamic `update_id`, e.g. via parameter(s), otherwise the query
    will only execute once.

    To customize the query signature as recorded in the database marker table, override
    the `update_id` property.

    Example:

    .. code-block:: python

        class AggregateData(duckdb.DuckDBQuery):
            date = luigi.DateParameter()

            database = '/path/to/data.duckdb'
            table = 'daily_aggregates'

            @property
            def query(self):
                return f'''
                    INSERT INTO daily_aggregates
                    SELECT date, COUNT(*), SUM(value)
                    FROM events
                    WHERE date = '{self.date}'
                    GROUP BY date
                '''
    """

    @property
    def database(self):
        """
        Path to the DuckDB database file.

        Use ':memory:' for an in-memory database.
        Override this in your task.
        """
        raise NotImplementedError("database path must be specified")

    # Override host, user, password from parent to make them optional for DuckDB
    @property
    def host(self):
        """Not applicable for DuckDB (embedded database)."""
        return None

    @property
    def user(self):
        """Not applicable for DuckDB (embedded database)."""
        return None

    @property
    def password(self):
        """Not applicable for DuckDB (embedded database)."""
        return None

    def run(self):
        """
        Execute the query and mark the task as complete.

        Override this method if you need to do something with the query results.
        """
        connection = self.output().connect()
        try:
            sql = self.query

            logger.info('Executing query from task: {name}'.format(name=self.__class__.__name__))
            connection.execute(sql)

            # Update marker table
            self.output().touch(connection)
        finally:
            connection.close()

    def output(self):
        """
        Returns a DuckDBTarget representing the executed query.

        Normally you don't override this.

        Returns:
            DuckDBTarget: Target representing this query execution.
        """
        return DuckDBTarget(
            database=self.database,
            table=self.table,
            update_id=self.update_id
        )
