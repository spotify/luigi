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
Implements a subclass of :py:class:`~luigi.target.Target` that writes data to Postgres.
Also provides a helper task to copy data into a Postgres table.
"""

import os
import datetime
import logging
import re
import tempfile

import luigi
from luigi.contrib import rdbms

logger = logging.getLogger('luigi-interface')

DB_DRIVER = os.environ.get('LUIGI_PGSQL_DRIVER', 'psycopg2')

DB_ERROR_CODES = {}
ERROR_DUPLICATE_TABLE = 'duplicate_table'
ERROR_UNDEFINED_TABLE = 'undefined_table'

dbapi = None

if DB_DRIVER == 'psycopg2':
    try:
        import psycopg2 as dbapi

        def update_error_codes():
            import psycopg2.errorcodes

            DB_ERROR_CODES.update({
                psycopg2.errorcodes.DUPLICATE_TABLE: ERROR_DUPLICATE_TABLE,
                psycopg2.errorcodes.UNDEFINED_TABLE: ERROR_UNDEFINED_TABLE,
            })
        update_error_codes()
    except ImportError:
        pass

if dbapi is None or DB_DRIVER == 'pg8000':
    try:
        import pg8000.dbapi as dbapi  # noqa: F811
        import pg8000.core
        # pg8000 doesn't have an error code catalog so we need to make our own
        # from https://www.postgresql.org/docs/8.2/errcodes-appendix.html
        DB_ERROR_CODES.update({'42P07': ERROR_DUPLICATE_TABLE, '42P01': ERROR_UNDEFINED_TABLE})
    except ImportError:
        pass


if dbapi is None:
    logger.warning("Loading postgres module without psycopg2 nor pg8000 installed. "
                   "Will crash at runtime if postgres functionality is used.")


def _is_pg8000_error(exception):
    try:
        return isinstance(exception, dbapi.DatabaseError) and \
            isinstance(exception.args, tuple) and \
            isinstance(exception.args[0], dict) and \
            pg8000.core.RESPONSE_CODE in exception.args[0]
    except NameError:
        return False


def _pg8000_connection_reset(connection):
    cursor = connection.cursor()
    if connection.autocommit:
        cursor.execute("DISCARD ALL")
    else:
        cursor.execute("ABORT")
        cursor.execute("BEGIN TRANSACTION")
    cursor.close()


def db_error_code(exception):
    try:
        error_code = None
        if hasattr(exception, 'pgcode'):
            error_code = exception.pgcode
        elif _is_pg8000_error(exception):
            error_code = exception.args[0][pg8000.core.RESPONSE_CODE]

        return DB_ERROR_CODES.get(error_code)
    except TypeError as error:
        error.__cause__ = exception
        raise error


class MultiReplacer:
    """
    Object for one-pass replace of multiple words

    Substituted parts will not be matched against other replace patterns, as opposed to when using multipass replace.
    The order of the items in the replace_pairs input will dictate replacement precedence.

    Constructor arguments:
    replace_pairs -- list of 2-tuples which hold strings to be replaced and replace string

    Usage:

    .. code-block:: python

        >>> replace_pairs = [("a", "b"), ("b", "c")]
        >>> MultiReplacer(replace_pairs)("abcd")
        'bccd'
        >>> replace_pairs = [("ab", "x"), ("a", "x")]
        >>> MultiReplacer(replace_pairs)("ab")
        'x'
        >>> replace_pairs.reverse()
        >>> MultiReplacer(replace_pairs)("ab")
        'xb'
    """
    # TODO: move to misc/util module

    def __init__(self, replace_pairs):
        """
        Initializes a MultiReplacer instance.

        :param replace_pairs: list of 2-tuples which hold strings to be replaced and replace string.
        :type replace_pairs: tuple
        """
        replace_list = list(replace_pairs)  # make a copy in case input is iterable
        self._replace_dict = dict(replace_list)
        pattern = '|'.join(re.escape(x) for x, y in replace_list)
        self._search_re = re.compile(pattern)

    def _replacer(self, match_object):
        # this method is used as the replace function in the re.sub below
        return self._replace_dict[match_object.group()]

    def __call__(self, search_string):
        # using function replacing for a per-result replace
        return self._search_re.sub(self._replacer, search_string)


# these are the escape sequences recognized by postgres COPY
# according to http://www.postgresql.org/docs/8.1/static/sql-copy.html
default_escape = MultiReplacer([('\\', '\\\\'),
                                ('\t', '\\t'),
                                ('\n', '\\n'),
                                ('\r', '\\r'),
                                ('\v', '\\v'),
                                ('\b', '\\b'),
                                ('\f', '\\f')
                                ])


class PostgresTarget(luigi.Target):
    """
    Target for a resource in Postgres.

    This will rarely have to be directly instantiated by the user.
    """
    marker_table = luigi.configuration.get_config().get('postgres', 'marker-table', 'table_updates')

    # if not supplied, fall back to default Postgres port
    DEFAULT_DB_PORT = 5432

    # Use DB side timestamps or client side timestamps in the marker_table
    use_db_timestamps = True

    def __init__(
            self, host, database, user, password, table, update_id, port=None
    ):
        """
        Args:
            host (str): Postgres server address. Possibly a host:port string.
            database (str): Database name
            user (str): Database user
            password (str): Password for specified user
            update_id (str): An identifier for this data set
            port (int): Postgres server port.

        """
        if ':' in host:
            self.host, self.port = host.split(':')
        else:
            self.host = host
            self.port = port or self.DEFAULT_DB_PORT
        self.database = database
        self.user = user
        self.password = password
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
            connection = self.connect()
            connection.autocommit = True  # if connection created here, we commit it here

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
        except dbapi.DatabaseError as e:
            if db_error_code(e) == ERROR_UNDEFINED_TABLE:
                row = None
            else:
                raise
        return row is not None

    def connect(self):
        """
        Get a DBAPI 2.0 connection object to the database where the table is.
        """
        connection = dbapi.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password)
        connection.set_client_encoding('utf-8')
        return connection

    def create_marker_table(self):
        """
        Create marker table if it doesn't exist.

        Using a separate connection since the transaction might have to be reset.
        """
        connection = self.connect()
        connection.autocommit = True
        cursor = connection.cursor()
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
        except dbapi.DatabaseError as e:
            if db_error_code(e) == ERROR_DUPLICATE_TABLE:
                pass
            else:
                raise
        connection.close()

    def open(self, mode):
        raise NotImplementedError("Cannot open() PostgresTarget")


class CopyToTable(rdbms.CopyToTable):
    """
    Template task for inserting a data set into Postgres

    Usage:
    Subclass and override the required `host`, `database`, `user`,
    `password`, `table` and `columns` attributes.

    To customize how to access data from an input task, override the `rows` method
    with a generator that yields each row as a tuple with fields ordered according to `columns`.
    """

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
            return r'\\N'
        else:
            return default_escape(str(value))

    # everything below will rarely have to be overridden

    def output(self):
        """
        Returns a PostgresTarget representing the inserted dataset.

        Normally you don't override this.
        """
        return PostgresTarget(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            table=self.table,
            update_id=self.update_id,
            port=self.port
        )

    def copy(self, cursor, file):
        if isinstance(self.columns[0], str):
            column_names = self.columns
        elif len(self.columns[0]) == 2:
            column_names = [c[0] for c in self.columns]
        else:
            raise Exception('columns must consist of column strings or (column string, type string) tuples (was %r ...)' % (self.columns[0],))

        # cursor.copy_from is not available in pg8000
        if hasattr(cursor, 'copy_from'):
            cursor.copy_from(
                file, self.table, null=r'\\N', sep=self.column_separator, columns=column_names)
        else:
            copy_sql = (
                "COPY {table} ({column_list}) FROM STDIN "
                "WITH (FORMAT text, NULL '{null_string}', DELIMITER '{delimiter}')"
            ).format(table=self.table, delimiter=self.column_separator, null_string=r'\\N',
                     column_list=", ".join(column_names))
            cursor.execute(copy_sql, stream=file)

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
        # to a temporary file for import using postgres COPY
        tmp_dir = luigi.configuration.get_config().get('postgres', 'local-tmp-dir', None)
        tmp_file = tempfile.TemporaryFile(dir=tmp_dir)
        n = 0
        for row in self.rows():
            n += 1
            if n % 100000 == 0:
                logger.info("Wrote %d lines", n)
            rowstr = self.column_separator.join(self.map_column(val) for val in row)
            rowstr += "\n"
            tmp_file.write(rowstr.encode('utf-8'))

        logger.info("Done writing, importing at %s", datetime.datetime.now())
        tmp_file.seek(0)

        # attempt to copy the data into postgres
        # if it fails because the target table doesn't exist
        # try to create it by running self.create_table
        for attempt in range(2):
            try:
                cursor = connection.cursor()
                self.init_copy(connection)
                self.copy(cursor, tmp_file)
                self.post_copy(connection)
                if self.enable_metadata_columns:
                    self.post_copy_metacolumns(cursor)
            except dbapi.DatabaseError as e:
                if db_error_code(e) == ERROR_UNDEFINED_TABLE and attempt == 0:
                    # if first attempt fails with "relation not found", try creating table
                    logger.info("Creating table %s", self.table)
                    # reset() is a psycopg2-specific method
                    if hasattr(connection, 'reset'):
                        connection.reset()
                    else:
                        _pg8000_connection_reset(connection)
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


class PostgresQuery(rdbms.Query):
    """
    Template task for querying a Postgres compatible database

    Usage:
    Subclass and override the required `host`, `database`, `user`, `password`, `table`, and `query` attributes.
    Optionally one can override the `autocommit` attribute to put the connection for the query in autocommit mode.

    Override the `run` method if your use case requires some action with the query result.

    Task instances require a dynamic `update_id`, e.g. via parameter(s), otherwise the query will only execute once

    To customize the query signature as recorded in the database marker table, override the `update_id` property.
    """
    def run(self):
        connection = self.output().connect()
        connection.autocommit = self.autocommit
        cursor = connection.cursor()
        sql = self.query

        logger.info('Executing query from task: {name}'.format(name=self.__class__))
        cursor.execute(sql)

        # Update marker table
        self.output().touch(connection)

        # commit and close connection
        connection.commit()
        connection.close()

    def output(self):
        """
        Returns a PostgresTarget representing the executed query.

        Normally you don't override this.
        """
        return PostgresTarget(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            table=self.table,
            update_id=self.update_id,
            port=self.port
        )
