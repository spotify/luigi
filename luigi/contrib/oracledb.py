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

# This module makes use of the f string formatting syntax, which requires Python 3.6 or higher

import logging
import luigi

logger = logging.getLogger('luigi-interface')

try:
    import cx_Oracle
except ImportError:
    logger.warning("Loading oracledb module without the python package cx_Oracle. \
        This will crash at runtime if Oracle Server functionality is used.")


class OracleTarget(luigi.Target):
    """
    Target for a resource in Oracle Server.
    This module is primarily derived from mysqldb.py.  Much of OracleTarget, MSSqlTarget,
    MySqlTarget, and PostgresTarget are similar enough to potentially add a
    RDBMSTarget abstract base class to rdbms.py that these classes could be
    derived from.
    """
    marker_table = luigi.configuration.get_config().get('oraclesql', 'marker_table', 'luigi_targets')

    def __init__(self, host, port, database, user, password, table, update_id, **cnx_kwargs):
        """
        Initializes an OracleTarget instance.

        :param host: Oracle server address. Possibly a host:port string.
        :type host: str
        :param port: Oracle server port number
        :type host: int
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
            self.port = '1521'  # Default Oracle Server Port
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

        with connection.cursor() as cursor:
            sql = f"MERGE INTO {self.marker_table} d " \
                  f"USING (SELECT '{self.update_id}' UPDATE_ID, '{self.table}' TARGET_TABLE, SYSDATE INSERTED from dual) s " \
                  f"ON (d.UPDATE_ID = s.UPDATE_ID) " \
                  f"WHEN MATCHED THEN UPDATE SET d.TARGET_TABLE = s.TARGET_TABLE, d.INSERTED = s.INSERTED " \
                  f"WHEN NOT MATCHED THEN INSERT (UPDATE_ID, TARGET_TABLE) VALUES (s.UPDATE_ID, s.TARGET_TABLE)"
            cursor.execute(sql)
            connection.commit()

        # make sure update is properly marked
        assert self.exists(connection)

    def exists(self, connection=None):
        if connection is None:
            connection = self.connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT NULL FROM {self.marker_table} WHERE UPPER(update_id) = UPPER('{self.update_id}')")
                cursor.fetchall()
                row_count = cursor.rowcount

        except cx_Oracle.DatabaseError as e:
            error, = e.args  # Returns cx_Oracle._Error object
            # Table does not exists code
            if error.code == 942:
                row_count = 0
            else:
                raise
        return row_count > 0

    def connect(self):
        """
        Create an Oracle Server connection and return a connection object
        """
        connection_string = f"{self.user}/{self.password}@{self.host}:{self.port}/{self.database}"
        connection = cx_Oracle.connect(connection_string)
        return connection

    def create_marker_table(self):
        """
        Create marker table if it doesn't exist.
        Use a separate connection since the transaction might have to be reset.
        """
        connection = self.connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"CREATE TABLE {self.marker_table}"
                               f"(ID NUMBER GENERATED ALWAYS AS IDENTITY,"
                               f"UPDATE_ID VARCHAR2(128) NOT NULL,"
                               f"TARGET_TABLE VARCHAR2(128),"
                               f"INSERTED  DATE DEFAULT SYSDATE NOT NULL,"
                               f"CONSTRAINT LUIGI_UPDATE_ID_PK PRIMARY KEY (UPDATE_ID))"
                               )
        except cx_Oracle.DatabaseError as e:
            error, = e.args  # Returns cx_Oracle._Error object
            # Table already exists code
            if error.code == 955:
                pass
            else:
                raise
        connection.close()