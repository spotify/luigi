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

logger = logging.getLogger('luigi-interface')

try:
    import _mssql
except ImportError:
    logger.warning("Loading MSSQL module without the python package pymssql. \
        This will crash at runtime if SQL Server functionality is used.")


class MSSqlTarget(luigi.Target):
    """
    Target for a resource in Microsoft SQL Server.
    This module is primarily derived from mysqldb.py.  Much of MSSqlTarget,
    MySqlTarget and PostgresTarget are similar enough to potentially add a
    RDBMSTarget abstract base class to rdbms.py that these classes could be
    derived from.
    """

    marker_table = luigi.configuration.get_config().get('mssql',
                                                        'marker-table',
                                                        'table_updates')

    def __init__(self, host, database, user, password, table, update_id):
        """
        Initializes a MsSqlTarget instance.

        :param host: MsSql server address. Possibly a host:port string.
        :type host: str
        :param database: database name.
        :type database: str
        :param user: database user
        :type user: str
        :param password: password for specified user.
        :type password: str
        :param update_id: an identifier for this data set.
        :type update_id: str
        """
        if ':' in host:
            self.host, self.port = host.split(':')
            self.port = int(self.port)
        else:
            self.host = host
            self.port = 1433
        self.database = database
        self.user = user
        self.password = password
        self.table = table
        self.update_id = update_id

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

        connection.execute_non_query(
            """IF NOT EXISTS(SELECT 1
                            FROM {marker_table}
                            WHERE update_id = %(update_id)s)
                    INSERT INTO {marker_table} (update_id, target_table)
                        VALUES (%(update_id)s, %(table)s)
                ELSE
                    UPDATE t
                    SET target_table = %(table)s
                        , inserted = GETDATE()
                    FROM {marker_table} t
                    WHERE update_id = %(update_id)s
              """.format(marker_table=self.marker_table),
            {"update_id": self.update_id, "table": self.table})

        # make sure update is properly marked
        assert self.exists(connection)

    def exists(self, connection=None):
        if connection is None:
            connection = self.connect()
        try:
            row = connection.execute_row("""SELECT 1 FROM {marker_table}
                                            WHERE update_id = %s
                                    """.format(marker_table=self.marker_table),
                                         (self.update_id,))
        except _mssql.MSSQLDatabaseException as e:
            # Error number for table doesn't exist
            if e.number == 208:
                row = None
            else:
                raise

        return row is not None

    def connect(self):
        """
        Create a SQL Server connection and return a connection object
        """
        connection = _mssql.connect(user=self.user,
                                    password=self.password,
                                    server=self.host,
                                    port=self.port,
                                    database=self.database)
        return connection

    def create_marker_table(self):
        """
        Create marker table if it doesn't exist.
        Use a separate connection since the transaction might have to be reset.
        """
        connection = self.connect()
        try:
            connection.execute_non_query(
                """ CREATE TABLE {marker_table} (
                        id            BIGINT    NOT NULL IDENTITY(1,1),
                        update_id     VARCHAR(128)  NOT NULL,
                        target_table  VARCHAR(128),
                        inserted      DATETIME DEFAULT(GETDATE()),
                        PRIMARY KEY (update_id)
                    )
                """
                .format(marker_table=self.marker_table)
            )
        except _mssql.MSSQLDatabaseException as e:
            # Table already exists code
            if e.number == 2714:
                pass
            else:
                raise
        connection.close()
