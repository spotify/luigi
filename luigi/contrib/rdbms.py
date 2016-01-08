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


class CopyToTable(luigi.task.MixinNaiveBulkComplete, luigi.Task):
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

        Subclass and override the following methods:

        * `output`
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

    @abc.abstractproperty
    def query(self):
        return None

    @abc.abstractmethod
    def run(self):
        raise NotImplementedError("This method must be overridden")

    @abc.abstractmethod
    def output(self):
        """
        Override with an RDBMS Target (e.g. PostgresTarget or RedshiftTarget) to record execution in a marker table
        """
        raise NotImplementedError("This method must be overridden")

    @property
    def update_id(self):
        """
        Override to create a custom marker table 'update_id' signature for Query subclass task instances
        """
        return self.task_id
