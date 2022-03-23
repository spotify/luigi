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

logger = logging.getLogger("luigi-interface")

try:
    import cx_Oracle as oracle
except ImportError:
    logger.warning(
        "Loading Oracle module without the python package cx_Oracle. \
            This will crash at runtime if Oracle functionality is used.\
            See https://cx-oracle.readthedocs.io/en/latest/index.html \
        "
    )


class OracleTarget(luigi.Target):
    """
    Target for a resource in Oracle.
    """

    marker_table = luigi.configuration.get_config().get(
        "oracle", "marker-table", "table_updates"
    )

    def __init__(
        self,
        host: str = None,
        port: int = 1521,
        database: str = None,
        dsn: str = None,
        user: str = None,
        password: str = None,
        table: str = None,
        update_id: str = None,
        **kwargs,
    ):
        """
        Initializes a OracleTarget instance.

        :param host: Oracle server address. Possibly a host:port string.
        :type host: str
        :param database: service name or sid.
        :type database: str
        :param dsn: Data Source Name. If not specified, then host, port and database are required.
        :type dsn: str
        :param user: database user.
        :type user: str
        :param password: database password.
        :type password: str
        :param table: table name.
        :type table: str
        :param update_id: update id.
        :type update_id: str
        :param kwargs: optional params for oracle connect constructor.
        """
        self.host = host
        self.port = port
        self.database = database
        self.dsn = dsn
        self.user = user
        self.password = password
        self.table = table
        self.update_id = update_id
        self.cnx_kwargs = kwargs

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

        sql = f"""
            merge into {self.marker_table} t
            using (
                select '{self.update_id}' as update_id, '{self.table}' as target_table
                from dual
            ) s
            on (t.update_id = s.update_id)
            when not matched then
                insert (update_id, target_table)
                values (s.update_id, s.update_id)
        """
        connection.cursor().execute(sql)
        connection.commit()
        # make sure update is properly marked
        assert self.exists(connection)

    def exists(self, connection=None):
        if connection is None:
            connection = self.connect()
        cursor = connection.cursor()
        try:
            sql = f"""
                SELECT 1
                FROM {self.marker_table}
                WHERE update_id = '{self.update_id}'
            """
            cursor.execute(sql)
            row = cursor.fetchone()
        except oracle.DatabaseError as e:
            if e.args[0].code == "ORA-00942":
                row = None
            else:
                raise
        return row is not None

    def connect(self):
        if self.dsn is None:
            self.dsn = oracle.makedsn(
                self.host,
                self.port,
                service_name=self.database,
            )

        connection = oracle.connect(
            user=self.user,
            password=self.password,
            dsn=self.dsn,
            **self.cnx_kwargs,
        )

        return connection

    def create_marker_table(self):
        """
        Create marker table if it doesn't exist.

        Using a separate connection since the transaction might have to be reset.
        """
        connection = self.connect()
        cursor = connection.cursor()
        queries = [
            f"""
                CREATE TABLE {self.marker_table} (
                    id            NUMBER  not null,
                    update_id     VARCHAR(128) NOT NULL UNIQUE,
                    target_table  VARCHAR(128),
                    inserted      TIMESTAMP DEFAULT sysdate
                )
            """,
            f"""
                create sequence {self.marker_table}_seq start with 1 increment by 1 nomaxvalue
            """,
            f"""
                create or replace trigger {self.marker_table}_id_trigger
                before insert on {self.marker_table}
                for each row
                begin
                    if :new.id is null then
                        select {self.marker_table}_seq.nextval into :new.id from dual;
                    end if;
                end;
            """,
        ]

        try:
            for q in queries:
                cursor.execute(q)
        except oracle.DatabaseError as e:
            if e.args[0].code == "ORA-00955":
                pass

        connection.close()


class CopyToTable(rdbms.CopyToTable):
    """
    Template task for inserting a data set into Oracle

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
        with self.input().open("r") as fobj:
            for line in fobj:
                yield line.strip("\n").split("\t")

    # everything below will rarely have to be overridden

    def output(self):
        """
        Returns a OracleTarget representing the inserted dataset.

        Normally you don't override this.
        """
        return OracleTarget(
            host=self.host,
            port=self.port,
            database=self.database,
            dsn=self.dsn,
            user=self.user,
            password=self.password,
            table=self.table,
            update_id=self.update_id,
        )

    def copy(self, cursor):
        query = f"""
            INSERT INTO {self.table} ({','.join(self.columns)})
            VALUES ({','.join([':'+c for c in self.columns])})
        """
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

        # attempt to copy the data into oracle
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
            except oracle.DatabaseError as e:
                if attempt == 0 and e.args[0].code == "ORA-00942":
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

    @property
    def dsn(self):
        return None

    @property
    def database(self):
        return None

    @property
    def host(self):
        return None
