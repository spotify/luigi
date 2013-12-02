# Copyright (c) 2012 Spotify AB
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

import abc
import datetime
import logging
import tempfile
import re

import luigi

logger = logging.getLogger('luigi-interface')

try:
    import psycopg2
    import psycopg2.errorcodes
    import psycopg2.extensions
except ImportError:
    logger.warning("Loading postgres module without psycopg2 installed. Will crash at runtime if postgres functionality is used.")

class MultiReplacer(object):
    # TODO: move to misc/util module
    """Object for one-pass replace of multiple words

    Substituted parts will not be matched against other replace patterns, as opposed to when using multipass replace.
    The order of the items in the replace_pairs input will dictate replacement precedence.

    Constructor arguments:
    replace_pairs -- list of 2-tuples which hold strings to be replaced and replace string

    Usage:
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
    def __init__(self, replace_pairs):
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
    """Target for a resource in Postgres.

    This will rarely have to be directly instantiated by the user"""
    marker_table = luigi.configuration.get_config().get('postgres', 'marker-table', 'table_updates')

    def __init__(self, host, database, user, password, table, update_id):
        """
        Args:
            host (str): Postgres server address. Possibly a host:port string.
            database (str): Database name
            user (str): Database user
            password (str): Password for specified user
            update_id (str): An identifier for this data set

        """
        if ':' in host:
            self.host, self.port = host.split(':')
        else:
            self.host = host
            self.port = None
        self.database = database
        self.user = user
        self.password = password
        self.table = table
        self.update_id = update_id

    def touch(self, connection=None):
        """Mark this update as complete.

        Important: If the marker table doesn't exist, the connection transaction will be aborted
        and the connection reset. Then the marker table will be created.
        """
        self.create_marker_table()

        if connection is None:
            # TODO: test this
            connection = self.connect()
            connection.autocommit = True  # if connection created here, we commit it here

        connection.cursor().execute(
            """INSERT INTO {marker_table} (update_id, target_table)
               VALUES (%s, %s)
            """.format(marker_table=self.marker_table),
            (self.update_id, self.table)
        )
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
        except psycopg2.ProgrammingError, e:
            if e.pgcode == psycopg2.errorcodes.UNDEFINED_TABLE:
                row = None
            else:
                raise
        return row is not None

    def connect(self):
        "Get a psycopg2 connection object to the database where the table is"
        connection = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password)
        connection.set_client_encoding('utf-8')
        return connection

    def create_marker_table(self):
        """Create marker table if it doesn't exist.

        Using a separate connection since the transaction might have to be reset"""
        connection = self.connect()
        connection.autocommit = True
        cursor = connection.cursor()
        try:
            cursor.execute(
                """ CREATE TABLE {marker_table} (
                        update_id TEXT PRIMARY KEY,
                        target_table TEXT,
                        inserted TIMESTAMP DEFAULT NOW()
                    )
                """
                .format(marker_table=self.marker_table)
            )
        except psycopg2.ProgrammingError, e:
            if e.pgcode == psycopg2.errorcodes.DUPLICATE_TABLE:
                pass
            else:
                raise
        connection.close()

    def open(self, mode):
        raise NotImplementedError("Cannot open() PostgresTarget")


class CopyToTable(luigi.Task):
    """
    Template task for inserting a data set into Postgres

    Usage:
    Subclass and override the required `host`, `database`, `user`,
    `password`, `table` and `columns` attributes.

    To customize how to access data from an input task, override the `rows` method
    with a generator that yields each row as a tuple with fields ordered according to `columns`.

    """

    @abc.abstractproperty
    def host(self):
        return None

    @abc.abstractproperty
    def database(self):
        return None

    @abc.abstractproperty
    def user(self):
        return None

    @abc.abstractproperty
    def password(self):
        return None

    @abc.abstractproperty
    def table(self):
        return None

    # specify the columns that are to be inserted (same as are returned by columns)
    # overload this in subclasses with the either column names of columns to import:
    # e.g. ['id', 'username', 'inserted']
    # or tuples with column name, postgres column type strings:
    # e.g. [('id', 'SERIAL PRIMARY KEY'), ('username', 'VARCHAR(255)'), ('inserted', 'DATETIME')]
    columns = []

    # options
    null_values = (None,)  # container of values that should be inserted as NULL values

    column_separator = "\t"  # how columns are separated in the file copied into postgres

    def rows(self):
        """Return/yield tuples or lists corresponding to each row to be inserted """
        with self.input().open('r') as fobj:
            for line in fobj:
                yield line.strip('\n').split('\t')

    def create_table(self, connection):
        """ Override to provide code for creating the target table.

        By default it will be created using types (optionally) specified in columns.

        If overridden, use the provided connection object for setting up the table in order to
        create the table and insert data using the same transaction.
        """
        if len(self.columns[0]) == 1:
            # only names of columns specified, no types
            raise NotImplementedError("create_table() not implemented for %r and columns types not specified" % self.table)
        elif len(self.columns[0]) == 2:
            # if columns is specified as (name, type) tuples
            coldefs = ','.join(
                '{name} {type}'.format(name=name, type=type) for name, type in self.columns
            )
            query = "CREATE TABLE {table} ({coldefs})".format(table=self.table, coldefs=coldefs)
            connection.cursor().execute(query)

    def map_column(self, value):
        """Applied to each column of every row returned by `rows`

        Default behaviour is to escape special characters and identify any self.null_values
        """
        if value in self.null_values:
            return '\N'
        elif isinstance(value, unicode):
            return default_escape(value).encode('utf8')
        else:
            return default_escape(str(value))

    def update_id(self):
        """This update id will be a unique identifier for this insert on this table."""
        return self.task_id

# everything below will rarely have to be overridden

    def output(self):
        """Returns a PostgresTarget representing the inserted dataset.

        Normally you don't override this.
        """
        return PostgresTarget(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            table=self.table,
            update_id=self.update_id()
         )

    def init_copy(self, connection):
        """ Override to perform custom queries.

            Any code here will be formed in the same transaction as the main copy, just prior to copying data. Example use cases include truncating the table or removing all data older than X in the database to keep a rolling window of data available in the table.
        """

        # TODO: remove this after sufficient time so most people using the
        # clear_table attribtue will have noticed it doesn't work anymore
        if hasattr(self, "clear_table"):
            raise Exception("The clear_table attribute has been removed. Override init_copy instead!")

    def copy(self, cursor, file):
        if isinstance(self.columns[0], basestring):
            column_names = self.columns
        elif len(self.columns[0]) == 2:
            column_names = zip(*self.columns)[0]
        else:
            raise Exception('columns must consist of column strings or (column string, type string) tuples (was %r ...)' % (self.columns[0],))
        cursor.copy_from(file, self.table, null='\N', sep=self.column_separator, columns=column_names)

    def run(self):
        """Inserts data generated by rows() into target table.

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
            tmp_file.write(rowstr + '\n')

        logger.info("Done writing, importing at %s", datetime.datetime.now())
        tmp_file.seek(0)

        # attempt to copy the data into postgres
        # if it fails because the target table doesn't exist
        # try to create it by running self.create_table
        for attempt in xrange(2):
            try:
                cursor = connection.cursor()
                self.init_copy(connection)
                self.copy(cursor, tmp_file)
            except psycopg2.ProgrammingError, e:
                if e.pgcode == psycopg2.errorcodes.UNDEFINED_TABLE and attempt == 0:
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
