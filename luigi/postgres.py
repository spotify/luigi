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

import luigi
from luigi.contrib import postgreslike
from luigi.contrib.postgreslike import default_escape

logger = logging.getLogger('luigi-interface')

try:
    import psycopg2
    import psycopg2.errorcodes
    import psycopg2.extensions
except ImportError:
    logger.warning("Loading postgres module without psycopg2 installed. Will crash at runtime if postgres functionality is used.")


class PostgresTarget(postgreslike.PostgreslikeTarget):
    """Target for a resource in Postgres.

    This will rarely have to be directly instantiated by the user"""
    marker_table = luigi.configuration.get_config().get('postgres', 'marker-table', 'table_updates')

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


class CopyToTable(postgreslike.CopyToTable):
    """
    Template task for inserting a data set into Postgres

    Usage:
    Subclass and override the required `host`, `database`, `user`,
    `password`, `table` and `columns` attributes.

    To customize how to access data from an input task, override the `rows` method
    with a generator that yields each row as a tuple with fields ordered according to `columns`.

    """

    def rows(self):
        """Return/yield tuples or lists corresponding to each row to be inserted """
        with self.input().open('r') as fobj:
            for line in fobj:
                yield line.strip('\n').split('\t')

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
