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

import logging

import luigi

from luigi.contrib import rdbms

logger = logging.getLogger('luigi-interface')

try:
    import mysql.connector
    from mysql.connector import errorcode, Error
except ImportError:
    logger.warning("Loading MySQL module without the python package mysql-connector-python. \
       This will crash at runtime if MySQL functionality is used.")


class MySqlTarget(luigi.Target):
    """
    Target for a resource in MySql.
    """

    marker_table = luigi.configuration.get_config().get('mysql', 'marker-table', 'table_updates')

    def __init__(self, host, database, user, password, table, update_id, **cnx_kwargs):
        """
        Initializes a MySqlTarget instance.

        :param host: MySql server address. Possibly a host:port string.
        :type host: str
        :param database: database name.
        :type database: str
        :param user: database user
        :type user: str
        :param password: password for specified user.
        :type password: str
        :param update_id: an identifier for this data set.
        :type update_id: str
        :param cnx_kwargs: optional params for mysql connector constructor.
            See https://dev.mysql.com/doc/connector-python/en/connector-python-connectargs.html.
        """
        if ':' in host:
            self.host, self.port = host.split(':')
            self.port = int(self.port)
        else:
            self.host = host
            self.port = 3306
        self.database = database
        self.user = user
        self.password = password
        self.table = table
        self.update_id = update_id
        self.cnx_kwargs = cnx_kwargs

    def touch(self, connection=None):
        """
        Mark this update as complete.

        IMPORTANT, If the marker table doesn't exist,
        the connection transaction will be aborted and the connection reset.
        Then the marker table will be created.
        """
        self.create_marker_table()

        if connection is None:
            connection = self.connect()
            connection.autocommit = True  # if connection created here, we commit it here

        connection.cursor().execute(
            """INSERT INTO {marker_table} (update_id, target_table)
               VALUES (%s, %s)
               ON DUPLICATE KEY UPDATE
               update_id = VALUES(update_id)
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
        except mysql.connector.Error as e:
            if e.errno == errorcode.ER_NO_SUCH_TABLE:
                row = None
            else:
                raise
        return row is not None

    def connect(self, autocommit=False):
        connection = mysql.connector.connect(user=self.user,
                                             password=self.password,
                                             host=self.host,
                                             port=self.port,
                                             database=self.database,
                                             autocommit=autocommit,
                                             **self.cnx_kwargs)
        return connection

    def create_marker_table(self):
        """
        Create marker table if it doesn't exist.

        Using a separate connection since the transaction might have to be reset.
        """
        connection = self.connect(autocommit=True)
        cursor = connection.cursor()
        try:
            cursor.execute(
                """ CREATE TABLE {marker_table} (
                        id            BIGINT(20)    NOT NULL AUTO_INCREMENT,
                        update_id     VARCHAR(128)  NOT NULL,
                        target_table  VARCHAR(128),
                        inserted      TIMESTAMP DEFAULT NOW(),
                        PRIMARY KEY (update_id),
                        KEY id (id)
                    )
                """
                .format(marker_table=self.marker_table)
            )
        except mysql.connector.Error as e:
            if e.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                pass
            else:
                raise
        connection.close()


class CopyToTable(rdbms.CopyToTable):
    """
    Template task for inserting a data set into MySQL

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

# everything below will rarely have to be overridden

    def output(self):
        """
        Returns a MySqlTarget representing the inserted dataset.

        Normally you don't override this.
        """
        return MySqlTarget(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            table=self.table,
            update_id=self.update_id

        )

    def copy(self, cursor, file=None):
        values = '({})'.format(','.join(['%s' for i in range(len(self.columns))]))
        columns = '({})'.format(','.join([c[0] for c in self.columns]))
        query = 'INSERT INTO {} {} VALUES {}'.format(self.table, columns, values)
        rows = []

        for idx, row in enumerate(self.rows()):
            rows.append(row)

            if (idx + 1) % self.bulk_size == 0:
                cursor.executemany(query, rows)
                rows = []

        cursor.executemany(query, rows)

    def run(self):
        """
        Inserts data generated by rows() into target table.

        If the target table doesn't exist, self.create_table will be called to attempt to create the table.

        Normally you don't want to override this.
        """
        if not (self.table and self.columns):
            raise Exception("table and columns need to be specified")

        connection = self.output().connect()

        # attempt to copy the data into mysql
        # if it fails because the target table doesn't exist
        # try to create it by running self.create_table
        for attempt in range(2):
            try:
                cursor = connection.cursor()
                print("caling init copy...")
                self.init_copy(connection)
                self.copy(cursor)
                self.post_copy(connection)
                if self.enable_metadata_columns:
                    self.post_copy_metacolumns(cursor)
            except Error as err:
                if err.errno == errorcode.ER_NO_SUCH_TABLE and attempt == 0:
                    # if first attempt fails with "relation not found", try creating table
                    # logger.info("Creating table %s", self.table)
                    connection.reconnect()
                    self.create_table(connection)
                else:
                    raise
            else:
                break

        # mark as complete in same transaction
        self.output().touch(connection)
        connection.commit()
        connection.close()

    @property
    def bulk_size(self):
        return 10000
