# -*- coding: utf-8 -*-
#
# Copyright 2015 Artiya T(artiya4u@gmail.com)
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
import abc
import logging
import datetime
import tempfile

import luigi
from luigi.contrib import rdbms


logger = logging.getLogger('luigi-interface')
try:
    import pyodbc
except ImportError:
    logger.warning("Loading pyodbc module without pyodbc installed. "
                   "Will crash at runtime if ODBC functionality is used.")


class ODBCTarget(luigi.Target):
    """
    Target for a resource in ODBC database.

    This will rarely have to be directly instantiated by the user.
    """
    marker_table = luigi.configuration.get_config().get('odbc', 'marker-table', 'table_updates')

    # Non specific database we use db timestamp
    use_db_timestamps = True

    def __init__(self, conn_str, table, update_id):
        """
        Args:
            conn_str: The connection string to connect ODBC.
            update_id (str): An identifier for this data set

        """
        self.conn_str = conn_str
        self.table = table
        self.update_id = update_id

    def touch(self, connection=None):
        """
        Mark this update as complete.

        Important: If the marker table doesn't exist, the connection transaction will be aborted
        and the connection reset.
        Then the marker table will be created.
        """
        self.create_marker_table()

        if connection is None:
            # TODO: test this
            connection = self.connect(self.conn_str, autocommit=True)

        if self.use_db_timestamps:
            connection.cursor().execute(
                """INSERT INTO {marker_table} (update_id, target_table)
                   VALUES (%s, %s)
                """.format(marker_table=self.marker_table),
                (self.update_id, self.table))
        else:
            connection.cursor().execute(
                """INSERT INTO {marker_table} (update_id, target_table, inserted)
                         VALUES (%s, %s, %s);
                    """.format(marker_table=self.marker_table),
                (self.update_id, self.table,
                 datetime.datetime.now()))

        # make sure update is properly marked
        assert self.exists(connection)

    def exists(self, connection=None):
        if connection is None:
            connection = self.connect()
            connection.autocommit = True
        cursor = connection.cursor()
        try:
            cursor.execute("""SELECT 1 FROM {marker_table}
                WHERE update_id = %s
                LIMIT 1""".format(marker_table=self.marker_table),
                           (self.update_id,)
                           )
            row = cursor.fetchone()
        except pyodbc.ProgrammingError:
            raise
        return row is not None

    def connect(self):
        """
        Get a pyodbc connection object to the database where the table is.
        """
        connection = pyodbc.connect(self.conn_str, autocommit=True)
        return connection

    def create_marker_table(self):
        """
        Create marker table if it doesn't exist.

        Using a separate connection since the transaction might have to be reset.
        """
        connection = self.connect()
        cursor = connection.cursor()
        if cursor.tables(table=self.marker_table).fetchone():
            # Table already exits.
            pass
        else:
            if self.use_db_timestamps:
                sql = """ CREATE TABLE {marker_table} (
                          update_id TEXT PRIMARY KEY,
                          target_table TEXT,
                          inserted TIMESTAMP DEFAULT NOW())
                    """.format(marker_table=self.marker_table)
            else:
                sql = """ CREATE TABLE {marker_table} (
                          update_id TEXT PRIMARY KEY,
                          target_table TEXT,
                          inserted TIMESTAMP);
                      """.format(marker_table=self.marker_table)
            try:
                cursor.execute(sql)
            except pyodbc.ProgrammingError:
                # We cannot know the error code from ODBC implementation, just keep quiet if table already create.
                pass
        connection.close()

    def open(self, mode):
        raise NotImplementedError("Cannot open() ODBCTarget")


class CopyToTable(rdbms.CopyToTable):
    """
    Template task for inserting a data set into ODBC database

    Usage:
    Subclass and override the required `conn_str`, `table` and `columns` attributes.

    To customize how to access data from an input task, override the `rows` method
    with a generator that yields each row as a tuple with fields ordered according to `columns`.
    """

    @abc.abstractproperty
    def conn_str(self):
        return None

    def rows(self):
        """
        Return/yield tuples or lists corresponding to each row to be inserted.
        """
        with self.input().open('r') as fobj:
            for line in fobj:
                yield line.strip('\n').split('\t')

    def map_column(self, value):
        """
        Applied to each column of every row returned by `rows`.

        Default behaviour is to escape special characters and identify any self.null_values.
        """
        if value in self.null_values:
            return '\N'
        elif isinstance(value, unicode):
            return value.encode('utf8')
        else:
            return str(value)

            # everything below will rarely have to be overridden

    def output(self):
        """
        Returns a ODBC representing the inserted dataset.

        Normally you don't override this.
        """
        return ODBCTarget(host=self.conn_str, table=self.table, update_id=self.update_id())

    def copy(self, cursor, file):
        if isinstance(self.columns[0], basestring):
            column_names = self.columns
        elif len(self.columns[0]) == 2:
            column_names = zip(*self.columns)[0]
        else:
            raise Exception(
                'columns must consist of column strings or (column string, type string) tuples (was %r ...)' % (
                    self.columns[0],))
        cursor.copy_from(file, self.table, null='\N', sep=self.column_separator, columns=column_names)

    def run(self):
        """
        Inserts data generated by rows() into target table.

        If the target table doesn't exist, self.create_table will be called to attempt to create the table.

        Normally you don't want to override this.
        """
        if not (self.table and self.columns):
            raise Exception("table and columns need to be specified")

        connection = self.output().connect()
        # transform all data generated by rows() using map_column and write data
        # to a temporary file for import using odbc COPY
        tmp_dir = luigi.configuration.get_config().get('odbc', 'local-tmp-dir', None)
        tmp_file = tempfile.TemporaryFile(dir=tmp_dir)
        n = 0
        for row in self.rows():
            n += 1
            if n % 100000 == 0:
                logger.info("Wrote %d lines", n)
            rowstr = self.column_separator.join(self.map_column(val) for val in row)
            tmp_file.write(rowstr + '\n')

        logger.info("Done writing, importing at %s", datetime.datetime.now())
        tmp_file.seek(0)

        # attempt to copy the data into odbc
        # if it fails because the target table doesn't exist
        # try to create it by running self.create_table
        for attempt in xrange(2):
            try:
                cursor = connection.cursor()
                self.init_copy(connection)
                self.copy(cursor, tmp_file)
            except pyodbc.ProgrammingError as e:
                if not cursor.tables(table=self.table).fetchone() and attempt == 0:
                    # if first attempt fails with "relation not found", try creating table
                    logger.info("Creating table %s", self.table)
                    connection.reset()
                    self.create_table(connection)
                else:
                    raise
            else:
                break

        # mark as complete in same transaction
        self.output().touch(connection)

        # commit and clean up
        connection.commit()
        connection.close()
        tmp_file.close()