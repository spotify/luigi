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

import abc
import json
import logging
import time
import os

import luigi
from luigi.contrib import postgres
from luigi.contrib import rdbms
from luigi.contrib.s3 import S3PathTask, S3Target

logger = logging.getLogger('luigi-interface')


try:
    import psycopg2
    import psycopg2.errorcodes
except ImportError:
    logger.warning("Loading postgres module without psycopg2 installed. "
                   "Will crash at runtime if postgres functionality is used.")


class _CredentialsMixin():
    """
    This mixin is used to provide the same credential properties
    for AWS to all Redshift tasks. It also provides a helper method
    to generate the credentials string for the task.
    """

    @property
    def configuration_section(self):
        """
        Override to change the configuration section used
        to obtain default credentials.
        """
        return 'redshift'

    @property
    def aws_access_key_id(self):
        """
        Override to return the key id.
        """
        return self._get_configuration_attribute('aws_access_key_id')

    @property
    def aws_secret_access_key(self):
        """
        Override to return the secret access key.
        """
        return self._get_configuration_attribute('aws_secret_access_key')

    @property
    def aws_account_id(self):
        """
        Override to return the account id.
        """
        return self._get_configuration_attribute('aws_account_id')

    @property
    def aws_arn_role_name(self):
        """
        Override to return the arn role name.
        """
        return self._get_configuration_attribute('aws_arn_role_name')

    @property
    def aws_session_token(self):
        """
        Override to return the session token.
        """
        return self._get_configuration_attribute('aws_session_token')

    def _get_configuration_attribute(self, attribute):
        config = luigi.configuration.get_config()

        value = config.get(self.configuration_section, attribute, default=None)

        if not value:
            value = os.environ.get(attribute.upper(), None)

        return value

    def _credentials(self):
        """
        Return a credential string for the provided task. If no valid
        credentials are set, raise a NotImplementedError.
        """

        if self.aws_account_id and self.aws_arn_role_name:
            return 'aws_iam_role=arn:aws:iam::{id}:role/{role}'.format(
                id=self.aws_account_id,
                role=self.aws_arn_role_name
            )
        elif self.aws_access_key_id and self.aws_secret_access_key:
            return 'aws_access_key_id={key};aws_secret_access_key={secret}{opt}'.format(
                key=self.aws_access_key_id,
                secret=self.aws_secret_access_key,
                opt=';token={}'.format(self.aws_session_token) if self.aws_session_token else ''
            )
        else:
            raise NotImplementedError("Missing Credentials. "
                                      "Ensure one of the pairs of auth args below are set "
                                      "in a configuration file, environment variables or by "
                                      "being overridden in the task: "
                                      "'aws_access_key_id' AND 'aws_secret_access_key' OR "
                                      "'aws_account_id' AND 'aws_arn_role_name'")


class RedshiftTarget(postgres.PostgresTarget):
    """
    Target for a resource in Redshift.

    Redshift is similar to postgres with a few adjustments
    required by redshift.
    """
    marker_table = luigi.configuration.get_config().get(
        'redshift',
        'marker-table',
        'table_updates')

    # if not supplied, fall back to default Redshift port
    DEFAULT_DB_PORT = 5439

    use_db_timestamps = False


class S3CopyToTable(rdbms.CopyToTable, _CredentialsMixin):
    """
    Template task for inserting a data set into Redshift from s3.

    Usage:

    * Subclass and override the required attributes:

      * `host`,
      * `database`,
      * `user`,
      * `password`,
      * `table`,
      * `columns`,
      * `s3_load_path`.

    * You can also override the attributes provided by the
      CredentialsMixin if they are not supplied by your
      configuration or environment variables.
    """

    @abc.abstractmethod
    def s3_load_path(self):
        """
        Override to return the load path.
        """
        return None

    @property
    @abc.abstractmethod
    def copy_options(self):
        """
        Add extra copy options, for example:

        * TIMEFORMAT 'auto'
        * IGNOREHEADER 1
        * TRUNCATECOLUMNS
        * IGNOREBLANKLINES
        * DELIMITER '\t'
        """
        return ''

    @property
    def prune_table(self):
        """
        Override to set equal to the name of the table which is to be pruned.
        Intended to be used in conjunction with prune_column and prune_date
        i.e. copy to temp table, prune production table to prune_column with a date greater than prune_date, then insert into production table from temp table
        """
        return None

    @property
    def prune_column(self):
        """
        Override to set equal to the column of the prune_table which is to be compared
        Intended to be used in conjunction with prune_table and prune_date
        i.e. copy to temp table, prune production table to prune_column with a date greater than prune_date, then insert into production table from temp table
        """
        return None

    @property
    def prune_date(self):
        """
        Override to set equal to the date by which prune_column is to be compared
        Intended to be used in conjunction with prune_table and prune_column
        i.e. copy to temp table, prune production table to prune_column with a date greater than prune_date, then insert into production table from temp table
        """
        return None

    @property
    def table_attributes(self):
        """
        Add extra table attributes, for example:

        DISTSTYLE KEY
        DISTKEY (MY_FIELD)
        SORTKEY (MY_FIELD_2, MY_FIELD_3)
        """
        return ''

    @property
    def table_constraints(self):
        """
        Add extra table constraints, for example:

        PRIMARY KEY (MY_FIELD, MY_FIELD_2)
        UNIQUE KEY (MY_FIELD_3)
        """
        return ''

    @property
    def do_truncate_table(self):
        """
        Return True if table should be truncated before copying new data in.
        """
        return False

    def do_prune(self):
        """
        Return True if prune_table, prune_column, and prune_date are implemented.
        If only a subset of prune variables are override, an exception is raised to remind the user to implement all or none.
        Prune (data newer than prune_date deleted) before copying new data in.
        """
        if self.prune_table and self.prune_column and self.prune_date:
            return True
        elif self.prune_table or self.prune_column or self.prune_date:
            raise Exception('override zero or all prune variables')
        else:
            return False

    @property
    def table_type(self):
        """
        Return table type (i.e. 'temp').
        """
        return ''

    @property
    def queries(self):
        """
        Override to return a list of queries to be executed in order.
        """
        return []

    def truncate_table(self, connection):
        query = "truncate %s" % self.table
        cursor = connection.cursor()
        try:
            cursor.execute(query)
        finally:
            cursor.close()

    def prune(self, connection):
        query = "delete from %s where %s >= %s" % (self.prune_table, self.prune_column, self.prune_date)
        cursor = connection.cursor()
        try:
            cursor.execute(query)
        finally:
            cursor.close()

    def create_schema(self, connection):
        """
        Will create the schema in the database
        """
        if '.' not in self.table:
            return

        query = 'CREATE SCHEMA IF NOT EXISTS {schema_name};'.format(schema_name=self.table.split('.')[0])
        connection.cursor().execute(query)

    def create_table(self, connection):
        """
        Override to provide code for creating the target table.

        By default it will be created using types (optionally)
        specified in columns.

        If overridden, use the provided connection object for
        setting up the table in order to create the table and
        insert data using the same transaction.
        """
        if len(self.columns[0]) == 1:
            # only names of columns specified, no types
            raise NotImplementedError("create_table() not implemented "
                                      "for %r and columns types not "
                                      "specified" % self.table)
        elif len(self.columns[0]) == 2:
            # if columns is specified as (name, type) tuples
            coldefs = ','.join(
                '{name} {type}'.format(
                    name=name,
                    type=type) for name, type in self.columns
            )

            table_constraints = ''
            if self.table_constraints != '':
                table_constraints = ', ' + self.table_constraints

            query = ("CREATE {type} TABLE "
                     "{table} ({coldefs} {table_constraints}) "
                     "{table_attributes}").format(
                type=self.table_type,
                table=self.table,
                coldefs=coldefs,
                table_constraints=table_constraints,
                table_attributes=self.table_attributes)

            connection.cursor().execute(query)
        elif len(self.columns[0]) == 3:
            # if columns is specified as (name, type, encoding) tuples
            # possible column encodings: https://docs.aws.amazon.com/redshift/latest/dg/c_Compression_encodings.html
            coldefs = ','.join(
                '{name} {type} ENCODE {encoding}'.format(
                    name=name,
                    type=type,
                    encoding=encoding) for name, type, encoding in self.columns
            )

            table_constraints = ''
            if self.table_constraints != '':
                table_constraints = ',' + self.table_constraints

            query = ("CREATE {type} TABLE "
                     "{table} ({coldefs} {table_constraints}) "
                     "{table_attributes}").format(
                type=self.table_type,
                table=self.table,
                coldefs=coldefs,
                table_constraints=table_constraints,
                table_attributes=self.table_attributes)

            connection.cursor().execute(query)
        else:
            raise ValueError("create_table() found no columns for %r"
                             % self.table)

    def run(self):
        """
        If the target table doesn't exist, self.create_table
        will be called to attempt to create the table.
        """
        if not (self.table):
            raise Exception("table need to be specified")

        path = self.s3_load_path()
        output = self.output()
        connection = output.connect()
        cursor = connection.cursor()

        self.init_copy(connection)
        self.copy(cursor, path)
        self.post_copy(cursor)

        if self.enable_metadata_columns:
            self.post_copy_metacolumns(cursor)

        # update marker table
        output.touch(connection)
        connection.commit()

        # commit and clean up
        connection.close()

    def copy(self, cursor, f):
        """
        Defines copying from s3 into redshift.

        If both key-based and role-based credentials are provided, role-based will be used.
        """
        logger.info("Inserting file: %s", f)
        colnames = ''
        if self.columns and len(self.columns) > 0:
            colnames = ",".join([x[0] for x in self.columns])
            colnames = '({})'.format(colnames)

        cursor.execute("""
         COPY {table} {colnames} from '{source}'
         CREDENTIALS '{creds}'
         {options}
         ;""".format(
            table=self.table,
            colnames=colnames,
            source=f,
            creds=self._credentials(),
            options=self.copy_options)
        )

    def output(self):
        """
        Returns a RedshiftTarget representing the inserted dataset.

        Normally you don't override this.
        """
        return RedshiftTarget(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            table=self.table,
            update_id=self.update_id)

    def does_schema_exist(self, connection):
        """
        Determine whether the schema already exists.
        """

        if '.' in self.table:
            query = ("select 1 as schema_exists "
                     "from pg_namespace "
                     "where nspname = lower(%s) limit 1")
        else:
            return True

        cursor = connection.cursor()
        try:
            schema = self.table.split('.')[0]
            cursor.execute(query, [schema])
            result = cursor.fetchone()
            return bool(result)
        finally:
            cursor.close()

    def does_table_exist(self, connection):
        """
        Determine whether the table already exists.
        """

        if '.' in self.table:
            query = ("select 1 as table_exists "
                     "from information_schema.tables "
                     "where table_schema = lower(%s) and table_name = lower(%s) limit 1")
        else:
            query = ("select 1 as table_exists "
                     "from pg_table_def "
                     "where tablename = lower(%s) limit 1")
        cursor = connection.cursor()
        try:
            cursor.execute(query, tuple(self.table.split('.')))
            result = cursor.fetchone()
            return bool(result)
        finally:
            cursor.close()

    def init_copy(self, connection):
        """
        Perform pre-copy sql - such as creating table, truncating, or removing data older than x.
        """
        if not self.does_schema_exist(connection):
            logger.info("Creating schema for %s", self.table)
            self.create_schema(connection)

        if not self.does_table_exist(connection):
            logger.info("Creating table %s", self.table)
            self.create_table(connection)

        if self.enable_metadata_columns:
            self._add_metadata_columns(connection)

        if self.do_truncate_table:
            logger.info("Truncating table %s", self.table)
            self.truncate_table(connection)

        if self.do_prune():
            logger.info("Removing %s older than %s from %s", self.prune_column, self.prune_date, self.prune_table)
            self.prune(connection)

    def post_copy(self, cursor):
        """
        Performs post-copy sql - such as cleansing data, inserting into production table (if copied to temp table), etc.
        """
        logger.info('Executing post copy queries')
        for query in self.queries:
            cursor.execute(query)

    def post_copy_metacolums(self, cursor):
        """
        Performs post-copy to fill metadata columns.
        """
        logger.info('Executing post copy metadata queries')
        for query in self.metadata_queries:
            cursor.execute(query)


class S3CopyJSONToTable(S3CopyToTable, _CredentialsMixin):
    """
    Template task for inserting a JSON data set into Redshift from s3.

    Usage:

        * Subclass and override the required attributes:

            * `host`,
            * `database`,
            * `user`,
            * `password`,
            * `table`,
            * `columns`,
            * `s3_load_path`,
            * `jsonpath`,
            * `copy_json_options`.

    * You can also override the attributes provided by the
      CredentialsMixin if they are not supplied by your
      configuration or environment variables.
    """

    @property
    @abc.abstractmethod
    def jsonpath(self):
        """
        Override the jsonpath schema location for the table.
        """
        return ''

    @property
    @abc.abstractmethod
    def copy_json_options(self):
        """
        Add extra copy options, for example:

        * GZIP
        * LZOP
        """
        return ''

    def copy(self, cursor, f):
        """
        Defines copying JSON from s3 into redshift.
        """

        logger.info("Inserting file: %s", f)
        cursor.execute("""
         COPY %s from '%s'
         CREDENTIALS '%s'
         JSON AS '%s' %s
         %s
         ;""" % (self.table, f, self._credentials(),
                 self.jsonpath, self.copy_json_options, self.copy_options))


class RedshiftManifestTask(S3PathTask):
    """
    Generic task to generate a manifest file that can be used
    in S3CopyToTable in order to copy multiple files from your
    s3 folder into a redshift table at once.

    For full description on how to use the manifest file see
    http://docs.aws.amazon.com/redshift/latest/dg/loading-data-files-using-manifest.html

    Usage:

        * requires parameters
            * path - s3 path to the generated manifest file, including the
                     name of the generated file
                     to be copied into a redshift table
            * folder_paths - s3 paths to the folders containing files you wish to be copied

    Output:

        * generated manifest file
    """

    # should be over ridden to point to a variety
    # of folders you wish to copy from
    folder_paths = luigi.Parameter()
    text_target = True

    def run(self):
        entries = []
        for folder_path in self.folder_paths:
            s3 = S3Target(folder_path)
            client = s3.fs
            for file_name in client.list(s3.path):
                entries.append({
                    'url': '%s/%s' % (folder_path, file_name),
                    'mandatory': True
                })
        manifest = {'entries': entries}
        target = self.output().open('w')
        dump = json.dumps(manifest)
        if not self.text_target:
            dump = dump.encode('utf8')
        target.write(dump)
        target.close()


class KillOpenRedshiftSessions(luigi.Task):
    """
    An task for killing any open Redshift sessions
    in a given database. This is necessary to prevent open user sessions
    with transactions against the table from blocking drop or truncate
    table commands.

    Usage:

    Subclass and override the required `host`, `database`,
    `user`, and `password` attributes.
    """

    # time in seconds to wait before
    # reconnecting to Redshift if our session is killed too.
    # 30 seconds is usually fine; 60 is conservative
    connection_reset_wait_seconds = luigi.IntParameter(default=60)

    @property
    @abc.abstractmethod
    def host(self):
        return None

    @property
    @abc.abstractmethod
    def database(self):
        return None

    @property
    @abc.abstractmethod
    def user(self):
        return None

    @property
    @abc.abstractmethod
    def password(self):
        return None

    @property
    def update_id(self):
        """
        This update id will be a unique identifier
        for this insert on this table.
        """
        return self.task_id

    def output(self):
        """
        Returns a RedshiftTarget representing the inserted dataset.

        Normally you don't override this.
        """
        # uses class name as a meta-table
        return RedshiftTarget(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            table=self.__class__.__name__,
            update_id=self.update_id)

    def run(self):
        """
        Kill any open Redshift sessions for the given database.
        """
        connection = self.output().connect()
        # kill any sessions other than ours and
        # internal Redshift sessions (rdsdb)
        query = ("select pg_terminate_backend(process) "
                 "from STV_SESSIONS "
                 "where db_name=%s "
                 "and user_name != 'rdsdb' "
                 "and process != pg_backend_pid()")
        cursor = connection.cursor()
        logger.info('Killing all open Redshift sessions for database: %s', self.database)
        try:
            cursor.execute(query, (self.database,))
            cursor.close()
            connection.commit()
        except psycopg2.DatabaseError as e:
            if e.message and 'EOF' in e.message:
                # sometimes this operation kills the current session.
                # rebuild the connection. Need to pause for 30-60 seconds
                # before Redshift will allow us back in.
                connection.close()
                logger.info('Pausing %s seconds for Redshift to reset connection', self.connection_reset_wait_seconds)
                time.sleep(self.connection_reset_wait_seconds)
                logger.info('Reconnecting to Redshift')
                connection = self.output().connect()
            else:
                raise

        try:
            self.output().touch(connection)
            connection.commit()
        finally:
            connection.close()

        logger.info('Done killing all open Redshift sessions for database: %s', self.database)


class RedshiftQuery(postgres.PostgresQuery):
    """
    Template task for querying an Amazon Redshift database

    Usage:
    Subclass and override the required `host`, `database`, `user`, `password`, `table`, and `query` attributes.

    Override the `run` method if your use case requires some action with the query result.

    Task instances require a dynamic `update_id`, e.g. via parameter(s), otherwise the query will only execute once

    To customize the query signature as recorded in the database marker table, override the `update_id` property.
    """

    def output(self):
        """
        Returns a RedshiftTarget representing the executed query.

        Normally you don't override this.
        """
        return RedshiftTarget(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            table=self.table,
            update_id=self.update_id
        )


class RedshiftUnloadTask(postgres.PostgresQuery, _CredentialsMixin):
    """
    Template task for running UNLOAD on an Amazon Redshift database

    Usage:
    Subclass and override the required `host`, `database`, `user`, `password`, `table`, and `query` attributes.
    Optionally, override the `autocommit` attribute to run the query in autocommit mode - this is necessary to run VACUUM for example.
    Override the `run` method if your use case requires some action with the query result.
    Task instances require a dynamic `update_id`, e.g. via parameter(s), otherwise the query will only execute once
    To customize the query signature as recorded in the database marker table, override the `update_id` property.
    You can also override the attributes provided by the CredentialsMixin if they are not supplied by your configuration or environment variables.
    """

    @property
    def s3_unload_path(self):
        """
        Override to return the load path.
        """
        return ''

    @property
    def unload_options(self):
        """
        Add extra or override default unload options:
        """
        return "DELIMITER '|' ADDQUOTES GZIP ALLOWOVERWRITE PARALLEL ON"

    @property
    def unload_query(self):
        """
        Default UNLOAD command
        """
        return ("UNLOAD ( '{query}' ) TO '{s3_unload_path}' "
                "credentials '{credentials}' "
                "{unload_options};")

    def run(self):
        connection = self.output().connect()
        cursor = connection.cursor()

        unload_query = self.unload_query.format(
            query=self.query().replace("'", r"\'"),
            s3_unload_path=self.s3_unload_path,
            unload_options=self.unload_options,
            credentials=self._credentials())

        logger.info('Executing unload query from task: {name}'.format(name=self.__class__))

        cursor = connection.cursor()
        cursor.execute(unload_query)
        logger.info(cursor.statusmessage)

        # Update marker table
        self.output().touch(connection)
        # commit and close connection
        connection.commit()
        connection.close()

    def output(self):
        """
        Returns a RedshiftTarget representing the executed query.

        Normally you don't override this.
        """
        return RedshiftTarget(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            table=self.table,
            update_id=self.update_id
        )
