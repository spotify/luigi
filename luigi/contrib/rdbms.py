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
A common module for postgres like databases, such as postgres or redshift
"""

import abc
import logging

import luigi
import luigi.task

logger = logging.getLogger('luigi-interface')


class _MetadataColumnsMixin:
    """Provide an additional behavior that adds columns and values to tables

    This mixin is used to provide an additional behavior that allow a task to
    add generic metadata columns to every table created for both PSQL and
    Redshift.

    Example:

        This is a use-case example of how this mixin could come handy and how
        to use it.

        .. code:: python

            class CommonMetaColumnsBehavior:
                def update_report_execution_date_query(self):
                    query = "UPDATE {0} " \
                            "SET date_param = DATE '{1}' " \
                            "WHERE date_param IS NULL".format(self.table, self.date)

                    return query

                @property
                def metadata_columns(self):
                    if self.date:
                        cols.append(('date_param', 'VARCHAR'))

                    return cols

                @property
                def metadata_queries(self):
                    queries = [self.update_created_tz_query()]
                    if self.date:
                        queries.append(self.update_report_execution_date_query())

                    return queries


            class RedshiftCopyCSVToTableFromS3(CommonMetaColumnsBehavior, redshift.S3CopyToTable):
                "We have some business override here that would only add noise to the
                example, so let's assume that this is only a shell."
                pass


            class UpdateTableA(RedshiftCopyCSVToTableFromS3):
                date = luigi.Parameter()
                table = 'tableA'

                def queries():
                    return [query_content_for('/queries/deduplicate_dupes.sql')]


            class UpdateTableB(RedshiftCopyCSVToTableFromS3):
                date = luigi.Parameter()
                table = 'tableB'
    """
    @property
    def metadata_columns(self):
        """Returns the default metadata columns.

        Those columns are columns that we want each tables to have by default.
        """
        return []

    @property
    def metadata_queries(self):
        return []

    @property
    def enable_metadata_columns(self):
        return False

    def _add_metadata_columns(self, connection):
        cursor = connection.cursor()

        for column in self.metadata_columns:
            if len(column) == 0:
                raise ValueError("_add_metadata_columns is unable to infer column information from column {column} for {table}".format(column=column,
                                                                                                                                       table=self.table))

            column_name = column[0]
            if not self._column_exists(cursor, column_name):
                logger.info('Adding missing metadata column {column} to {table}'.format(column=column, table=self.table))
                self._add_column_to_table(cursor, column)

    def _column_exists(self, cursor, column_name):
        if '.' in self.table:
            schema, table = self.table.split('.')
            query = "SELECT 1 AS column_exists " \
                    "FROM information_schema.columns " \
                    "WHERE table_schema = LOWER('{0}') AND table_name = LOWER('{1}') AND column_name = LOWER('{2}') LIMIT 1;".format(schema, table, column_name)
        else:
            query = "SELECT 1 AS column_exists " \
                    "FROM information_schema.columns " \
                    "WHERE table_name = LOWER('{0}') AND column_name = LOWER('{1}') LIMIT 1;".format(self.table, column_name)

        cursor.execute(query)
        result = cursor.fetchone()
        return bool(result)

    def _add_column_to_table(self, cursor, column):
        if len(column) == 1:
            raise ValueError("_add_column_to_table() column type not specified for {column}".format(column=column[0]))
        elif len(column) == 2:
            query = "ALTER TABLE {table} ADD COLUMN {column};".format(table=self.table, column=' '.join(column))
        elif len(column) == 3:
            query = "ALTER TABLE {table} ADD COLUMN {column} ENCODE {encoding};".format(table=self.table, column=' '.join(column[0:2]), encoding=column[2])
        else:
            raise ValueError("_add_column_to_table() found no matching behavior for {column}".format(column=column))

        cursor.execute(query)

    def post_copy_metacolumns(self, cursor):
        logger.info('Executing post copy metadata queries')
        for query in self.metadata_queries:
            cursor.execute(query)


class CopyToTable(luigi.task.MixinNaiveBulkComplete, _MetadataColumnsMixin, luigi.Task):
    """
    An abstract task for inserting a data set into RDBMS.

    Usage:

        Subclass and override the following attributes:

        * `host`,
        * `database`,
        * `user`,
        * `password`,
        * `table`
        * `columns`
        * `port`
    """

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
    @abc.abstractmethod
    def table(self):
        return None

    @property
    def port(self):
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

    def create_table(self, connection):
        """
        Override to provide code for creating the target table.

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

    @property
    def update_id(self):
        """
        This update id will be a unique identifier for this insert on this table.
        """
        return self.task_id

    @abc.abstractmethod
    def output(self):
        raise NotImplementedError("This method must be overridden")

    def init_copy(self, connection):
        """
        Override to perform custom queries.

        Any code here will be formed in the same transaction as the main copy, just prior to copying data.
        Example use cases include truncating the table or removing all data older than X in the database
        to keep a rolling window of data available in the table.
        """

        # TODO: remove this after sufficient time so most people using the
        # clear_table attribtue will have noticed it doesn't work anymore
        if hasattr(self, "clear_table"):
            raise Exception("The clear_table attribute has been removed. Override init_copy instead!")

        if self.enable_metadata_columns:
            self._add_metadata_columns(connection.cursor())

    def post_copy(self, connection):
        """
        Override to perform custom queries.

        Any code here will be formed in the same transaction as the main copy, just after copying data.
        Example use cases include cleansing data in temp table prior to insertion into real table.
        """
        pass

    @abc.abstractmethod
    def copy(self, cursor, file):
        raise NotImplementedError("This method must be overridden")


class Query(luigi.task.MixinNaiveBulkComplete, luigi.Task):
    """
    An abstract task for executing an RDBMS query.

    Usage:

        Subclass and override the following attributes:

        * `host`,
        * `database`,
        * `user`,
        * `password`,
        * `table`,
        * `query`

        Optionally override:

        * `port`,
        * `autocommit`
        * `update_id`

        Subclass and override the following methods:

        * `run`
        * `output`
    """

    @property
    @abc.abstractmethod
    def host(self):
        """
        Host of the RDBMS. Implementation should support `hostname:port`
        to encode port.
        """
        return None

    @property
    def port(self):
        """
        Override to specify port separately from host.
        """
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
    @abc.abstractmethod
    def table(self):
        return None

    @property
    @abc.abstractmethod
    def query(self):
        return None

    @property
    def autocommit(self):
        return False

    @property
    def update_id(self):
        """
        Override to create a custom marker table 'update_id' signature for Query subclass task instances
        """
        return self.task_id

    @abc.abstractmethod
    def run(self):
        raise NotImplementedError("This method must be overridden")

    @abc.abstractmethod
    def output(self):
        """
        Override with an RDBMS Target (e.g. PostgresTarget or RedshiftTarget) to record execution in a marker table
        """
        raise NotImplementedError("This method must be overridden")
