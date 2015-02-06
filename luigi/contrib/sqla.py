# -*- coding: utf-8 -*-
#
# Copyright (c) 2015 Gouthaman Balaraman
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
#
"""
Support for SQLAlchmey. Provides SQLAlchemyTarget for storing in databases
supported by SQLAlchemy. The user would be responsible for installing the
required database driver to connect using SQLAlchemy.

Minimal example of a job to copy data to database using SQLAlchemy is as shown
below:

.. code-block:: python

    from sqlalchemy import String
    import luigi
    from luigi.contrib import sqla

    class SQLATask(sqla.CopyToTable):
        # columns defines the table schema, with each element corresponding
        # to a column in the format (args, kwargs) which will be sent to
        # the sqlalchemy.Column(*args, **kwargs)
        columns = [
            (["item", String(64)], {"primary_key": True}),
            (["property", String(64)], {})
        ]
        connection_string = "sqlite://"  # in memory SQLite database
        table = "item_property"  # name of the table to store data

        def rows(self):
            for row in [("item1" "property1"), ("item2", "property2")]:
                yield row

    if __name__ == '__main__':
        task = SQLATask()
        luigi.build([task], local_scheduler=True)


If the target table where the data needs to be copied already exists, then
the column schema definition can be skipped and instead the reflect flag
can be set as True. Here is a modified version of the above example:

.. code-block:: python

    from sqlalchemy import String
    import luigi
    from luigi.contrib import sqla

    class SQLATask(sqla.CopyToTable):
        # If database table is already created, then the schema can be loaded
        # by setting the reflect flag to True
        reflect = True
        connection_string = "sqlite://"  # in memory SQLite database
        table = "item_property"  # name of the table to store data

        def rows(self):
            for row in [("item1" "property1"), ("item2", "property2")]:
                yield row

    if __name__ == '__main__':
        task = SQLATask()
        luigi.build([task], local_scheduler=True)


In the above examples, the data that needs to be copied was directly provided by
overriding the rows method. Alternately, if the data comes from another task, the
modified example would look as shown below:

.. code-block:: python

    from sqlalchemy import String
    import luigi
    from luigi.contrib import sqla
    from luigi.mock import MockFile

    class BaseTask(luigi.Task):
        def output(self):
            return MockFile("BaseTask")

        def run(self):
            out = self.output().open("w")
            TASK_LIST = ["item%d\\tproperty%d\\n" % (i, i) for i in range(10)]
            for task in TASK_LIST:
                out.write(task)
            out.close()

    class SQLATask(sqla.CopyToTable):
        # columns defines the table schema, with each element corresponding
        # to a column in the format (args, kwargs) which will be sent to
        # the sqlalchemy.Column(*args, **kwargs)
        columns = [
            (["item", String(64)], {"primary_key": True}),
            (["property", String(64)], {})
        ]
        connection_string = "sqlite://"  # in memory SQLite database
        table = "item_property"  # name of the table to store data

        def requires(self):
            return BaseTask()

    if __name__ == '__main__':
        task1, task2 = SQLATask(), BaseTask()
        luigi.build([task1, task2], local_scheduler=True)


In the above example, the output from `BaseTask` is copied into the
database. Here we did not have to implement the `rows` method because
by default `rows` implementation assumes every line is a row with
column values separated by a tab. One can define `column_separator`
option for the task if the values are say comma separated instead of
tab separated.

The other option to `sqla.CopyToTable` that can be of help with performance aspect is the
`chunk_size`. The default is 5000. This is the number of rows that will be inserted in
a transaction at a time. Depending on the size of the inserts, this value can be tuned
for performance.

Author: Gouthaman Balaraman
Date: 01/02/2015
"""


import abc
import datetime
import itertools
import logging

import luigi
import sqlalchemy

logger = logging.getLogger('luigi-interface')


class SQLAlchemyTarget(luigi.Target):
    """
    Database target using SQLAlchemy.

    This will rarely have to be directly instantiated by the user.

    Typical usage would be to override `luigi.contrib.sqla.CopyToTable` class
    to create a task to write to the database.
    """
    marker_table = None

    def __init__(self, connection_string, target_table, update_id, echo=False):
        """
        Constructor for the SQLAlchemyTarget.

        :param connection_string: (str) SQLAlchemy connection string
        :param target_table: (str) The table name for the data
        :param update_id: (str) An identifier for this data set
        :param echo: (bool) Flag to setup SQLAlchemy logging
        :return:
        """
        self.target_table = target_table
        self.update_id = update_id
        self.engine = sqlalchemy.create_engine(connection_string, echo=echo)
        self.marker_table_bound = None

    def touch(self):
        """
        Mark this update as complete.
        """
        if self.marker_table_bound is None:
            self.create_marker_table()

        table = self.marker_table_bound
        with self.engine.begin() as conn:
            id_exists = self.exists()
            if not id_exists:
                ins = table.insert().values(update_id=self.update_id, target_table=self.target_table)
            else:
                ins = table.update().values(update_id=self.update_id, target_table=self.target_table,
                                            inserted=datetime.datetime.now())
            conn.execute(ins)
        assert self.exists()

    def exists(self):
        row = None
        if self.marker_table_bound is None:
            self.create_marker_table()
        with self.engine.begin() as conn:
            table = self.marker_table_bound
            s = sqlalchemy.select([table]).where(table.c.update_id == self.update_id).limit(1)
            row = conn.execute(s).fetchone()
        return row is not None

    def create_marker_table(self):
        """
        Create marker table if it doesn't exist.

        Using a separate connection since the transaction might have to be reset.
        """
        if self.marker_table is None:
            self.marker_table = luigi.configuration.get_config().get('sqlalchemy', 'marker-table', 'table_updates')

        with self.engine.begin() as con:
            metadata = sqlalchemy.MetaData()
            if not con.dialect.has_table(con, self.marker_table):
                self.marker_table_bound = sqlalchemy.Table(
                    self.marker_table, metadata,
                    sqlalchemy.Column("update_id", sqlalchemy.String(128), primary_key=True),
                    sqlalchemy.Column("target_table", sqlalchemy.String(128)),
                    sqlalchemy.Column("inserted", sqlalchemy.DateTime, default=datetime.datetime.now()))
                metadata.create_all(self.engine)
            else:
                metadata.reflect(bind=self.engine)
                self.marker_table_bound = metadata.tables[self.marker_table]

    def open(self, mode):
        raise NotImplementedError("Cannot open() SQLAlchemyTarget")


class CopyToTable(luigi.Task):
    """
    An abstract task for inserting a data set into SQLAlchemy RDBMS

    Usage:

    * subclass and override the required `connection_string`, `table` and `columns` attributes.
    """
    echo = False

    @abc.abstractmethod
    def connection_string(self):
        return None

    @abc.abstractproperty
    def table(self):
        return None

    # specify the columns that define the schema. The format for the columns is a list
    # of tuples. For example :
    # columns = [
    #            (["id", sqlalchemy.Integer], dict(primary_key=True)),
    #            (["name", sqlalchemy.String(64)], {}),
    #            (["value", sqlalchemy.String(64)], {})
    #        ]
    # The tuple (args_list, kwargs_dict) here is the args and kwargs
    # that need to be passed to sqlalchemy.Column(*args, **kwargs).
    # If the tables have already been setup by another process, then you can
    # completely ignore the columns. Instead set the reflect value to True below
    columns = []

    # options
    column_separator = "\t"  # how columns are separated in the file copied into postgres
    chunk_size = 5000   # default chunk size for insert
    reflect = False  # Set this to true only if the table has already been created by alternate means

    def create_table(self, engine):
        """
        Override to provide code for creating the target table.

        By default it will be created using types specified in columns.
        If the table exists, then it binds to the existing table.

        If overridden, use the provided connection object for setting up the table in order to
        create the table and insert data using the same transaction.
        """
        def construct_sqla_columns(columns):
            retval = [sqlalchemy.Column(*c[0], **c[1]) for c in columns]
            return retval

        needs_setup = (len(self.columns) == 0) or (False in [len(c) == 2 for c in self.columns]) if not self.reflect else False
        if needs_setup:
            # only names of columns specified, no types
            raise NotImplementedError("create_table() not implemented for %r and columns types not specified" % self.table)
        else:
            # if columns is specified as (name, type) tuples
            with engine.begin() as con:
                metadata = sqlalchemy.MetaData()
                try:
                    if not con.dialect.has_table(con, self.table):
                        sqla_columns = construct_sqla_columns(self.columns)
                        self.table_bound = sqlalchemy.Table(self.table, metadata, *sqla_columns)
                        metadata.create_all(engine)
                    else:
                        metadata.reflect(bind=engine)
                        self.table_bound = metadata.tables[self.table]
                except Exception as e:
                    logger.exception(self.table + str(e))

    def update_id(self):
        """
        This update id will be a unique identifier for this insert on this table.
        """
        return self.task_id

    def output(self):
        return SQLAlchemyTarget(
            connection_string=self.connection_string,
            target_table=self.table,
            update_id=self.update_id(),
            echo=self.echo
        )

    def rows(self):
        """
        Return/yield tuples or lists corresponding to each row to be inserted.

        This method can be overridden for custom file types or formats.
        """
        with self.input().open('r') as fobj:
            for line in fobj:
                yield line.strip("\n").split(self.column_separator)

    def run(self):
        logger.info('Running task copy to table for update id %s for table %s', self.update_id(), self.table)
        output = self.output()
        self.create_table(output.engine)
        with output.engine.begin() as conn:
            rows = iter(self.rows())
            ins_rows = [dict(zip((c.key for c in self.table_bound.c), row))
                        for row in itertools.islice(rows, self.chunk_size)]
            while ins_rows:
                ins = self.table_bound.insert()
                conn.execute(ins, ins_rows)
                ins_rows = [dict(zip((c.key for c in self.table_bound.c), row))
                            for row in itertools.islice(rows, self.chunk_size)]
                logger.info('Finished inserting %d rows into SQLAlchemy target', len(ins_rows))
        output.touch()
        logger.info("Finished inserting rows into SQLAlchemy target")
