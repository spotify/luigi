"""
This file implements RDBMS target using SQLAlchemy. This was derived
based on rdbms.py, mysqldb.py

Author : Gouthaman Balaraman
Date : 01/02/2015
"""


import abc
import logging
import luigi
import datetime
from sqlalchemy import Table, MetaData, Column, String, DateTime, create_engine, select

logger = logging.getLogger('luigi-interface')

class SQLAlchemyTarget(luigi.Target):
    """Target for a resource in database using SQLAlchemy.

    This will rarely have to be directly instantiated by the user"""
    marker_table = luigi.configuration.get_config().get('sqlalchemy', 'marker-table', 'table_updates')


    def __init__(self, connection_string, target_table, update_id, echo=False):
        """ Constructor for the SQLAlchemyTarget
        :param connection_string: (str) SQLAlchemy connection string
        :param target_table: (str) The table name for the data
        :param update_id: (str) An identifier for this data set
        :param echo: (bool) Flag to setup SQLAlchemy logging
        :return:
        """
        self.target_table = target_table
        self.update_id = update_id
        self.engine = create_engine(connection_string, echo=echo)
        self.marker_table_bound = None

    def touch(self):
        """Mark this update as complete.
        """
        self.create_marker_table()

        table = self.marker_table_bound
        with self.engine.begin() as conn:
            id_exists = self.exists()
            if not id_exists:
                ins = table.insert().values(update_id=self.update_id, target_table=self.target_table)
            else:
                ins = table.update().values(update_id=self.update_id, target_table=self.target_table,inserted=datetime.datetime.now())
            conn.execute(ins)
        assert self.exists()

    def exists(self):
        row = None
        if self.marker_table_bound is None:
            self.create_marker_table()
        try:
            with self.engine.begin() as conn:
                table = self.marker_table_bound
                s = select([table]).where(table.c.update_id == self.update_id).limit(1)
                row = conn.execute(s).fetchone()
        except Exception as e:
            se = str(e)
            row = None
        return row is not None

    def connect(self):
        "Get a sqlalchemy connection object to the database where the table is"
        connection = self.engine.connect()
        return connection

    def create_marker_table(self):
        """Create marker table if it doesn't exist.

        Using a separate connection since the transaction might have to be reset"""

        metadata = MetaData()
        with self.engine.begin() as con:
            if not con.dialect.has_table(con, self.marker_table):
                self.marker_table_bound = Table(self.marker_table, metadata,
                                     Column("update_id", String(128), primary_key=True),
                                     Column("target_table", String(128)),
                                     Column("inserted",DateTime,default=datetime.datetime.now()) )
                metadata.create_all(self.engine)
            else:
                metadata.reflect(bind=self.engine)
                self.marker_table_bound = metadata.tables[self.marker_table]

    def open(self, mode):
        raise NotImplementedError("Cannot open() PostgresTarget")


class CopyToTable(luigi.Task):
    """
    An abstract task for inserting a data set into SQLAlchemy RDBMS

    Usage:
    Subclass and override the required `connection_string`, `table` and `columns` attributes.
    """

    data_table = None       # sqlalchemy table instance
    echo = False

    @abc.abstractmethod
    def connection_string(self):
        return None

    @abc.abstractproperty
    def table(self):
        return None

    # specify the columns that are to be inserted (same as are returned by columns)
    # overload this in subclasses with tuples with column name, sqlalchemy column type Class:
    # e.g.
    # columns = [
    #            sqlalchemy.Column("id", sqlalchemy.Integer,primary_key=True),
    #            sqlalchemy.Column("name", sqlalchemy.String(64)),
    #            sqlalchemy.Column("value", sqlalchemy.String(64))
    #        ]
    columns = []

    # options
    null_values = (None,)  # container of values that should be inserted as NULL values
    column_separator = "\t"  # how columns are separated in the file copied into postgres
    batch_size = 500

    def create_table(self, engine):
        """ Override to provide code for creating the target table.

        By default it will be created using types specified in columns. If the table
        exists, then it binds to the existing table.

        If overridden, use the provided connection object for setting up the table in order to
        create the table and insert data using the same transaction.
        """
        columns_faulty = (len(self.columns) == 0) or (False in [isinstance(col, Column) for col in self.columns])
        if columns_faulty:
            # only names of columns specified, no types
            raise NotImplementedError("create_table() not implemented for %r and columns types not specified" % self.table)
        else:
            # if columns is specified as (name, type) tuples
            metadata = MetaData()
            with engine.begin() as con:
                if not con.dialect.has_table(con, self.table):
                    self.table_bound = Table(self.table, metadata, *self.columns)
                    metadata.create_all(engine)
                else:
                    metadata.reflect(bind=engine)
                    self.table_bound = metadata.tables[self.table]

    def update_id(self):
        """This update id will be a unique identifier for this insert on this table."""
        return self.task_id

    def output(self):
        return SQLAlchemyTarget(
            connection_string=self.connection_string,
            target_table=self.table,
            update_id=self.update_id(),
            echo=self.echo
            )

    def rows(self):
        """Return/yield tuples or lists corresponding to each row to be inserted """
        with self.input().open('r') as fobj:
            for line in fobj:
                yield line.strip('\n').split(self.column_separator)

    def run(self):
        logger.info("Running task copy to table for update id %s for table %s" % (self.update_id(), self.table))
        output = self.output()
        self.create_table(output.engine)
        try:
            with output.engine.begin() as conn:
                rows = self.rows()
                ins_rows = [ dict(zip((c.key for c in self.table_bound.c), row)) for row in rows]
                ins = self.table_bound.insert()
                conn.execute(ins, ins_rows)
            output.touch()
        except Exception as e:
            raise RuntimeError(str(e))
        logger.info("Finished inserting rows into SQLAlchemy target")

