'''
A common module for posgres like databases, such as postgres or redshift
'''

import abc
import logging
import re

import luigi

logger = logging.getLogger('luigi-interface')

try:
    import psycopg2
    import psycopg2.errorcodes
    import psycopg2.extensions
except ImportError:
    logger.warning("Loading postgres module without psycopg2 installed. Will crash at runtime if postgres functionality is used.")


class MultiReplacer(object):
    # TODO: move to misc/util module
    """Object for one-pass replace of multiple words

    Substituted parts will not be matched against other replace patterns, as opposed to when using multipass replace.
    The order of the items in the replace_pairs input will dictate replacement precedence.

    Constructor arguments:
    replace_pairs -- list of 2-tuples which hold strings to be replaced and replace string

    Usage:
    >>> replace_pairs = [("a", "b"), ("b", "c")]
    >>> MultiReplacer(replace_pairs)("abcd")
    'bccd'
    >>> replace_pairs = [("ab", "x"), ("a", "x")]
    >>> MultiReplacer(replace_pairs)("ab")
    'x'
    >>> replace_pairs.reverse()
    >>> MultiReplacer(replace_pairs)("ab")
    'xb'
    """
    def __init__(self, replace_pairs):
        replace_list = list(replace_pairs)  # make a copy in case input is iterable
        self._replace_dict = dict(replace_list)
        pattern = '|'.join(re.escape(x) for x, y in replace_list)
        self._search_re = re.compile(pattern)

    def _replacer(self, match_object):
        # this method is used as the replace function in the re.sub below
        return self._replace_dict[match_object.group()]

    def __call__(self, search_string):
        # using function replacing for a per-result replace
        return self._search_re.sub(self._replacer, search_string)


# these are the escape sequences recognized by postgres COPY
# according to http://www.postgresql.org/docs/8.1/static/sql-copy.html
default_escape = MultiReplacer([('\\', '\\\\'),
                                ('\t', '\\t'),
                                ('\n', '\\n'),
                                ('\r', '\\r'),
                                ('\v', '\\v'),
                                ('\b', '\\b'),
                                ('\f', '\\f')
                                ])


class PostgreslikeTarget(luigi.Target):
    """Target for a resource in Postgres-like databases.

    Check out the concrete implementations such as postgres or redshift

    This will rarely have to be directly instantiated by the user"""

    def __init__(self, host, database, user, password, table, update_id):
        """
        Args:
            host (str): Postgres server address. Possibly a host:port string.
            database (str): Database name
            user (str): Database user
            password (str): Password for specified user
            update_id (str): An identifier for this data set

        """
        if ':' in host:
            self.host, self.port = host.split(':')
        else:
            self.host = host
            self.port = None
        self.database = database
        self.user = user
        self.password = password
        self.table = table
        self.update_id = update_id

    def touch(self, connection=None):
        """Mark this update as complete.

        Important: If the marker table doesn't exist, the connection transaction will be aborted
        and the connection reset. Then the marker table will be created.
        """
        raise NotImplementedError("This method must be overridden")

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
        except psycopg2.ProgrammingError, e:
            if e.pgcode == psycopg2.errorcodes.UNDEFINED_TABLE:
                row = None
            else:
                raise
        return row is not None

    def connect(self):
        "Get a psycopg2 connection object to the database where the table is"
        connection = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password)
        connection.set_client_encoding('utf-8')
        return connection

    def create_marker_table(self):
        """Create marker table if it doesn't exist.

        Using a separate connection since the transaction might have to be reset"""
        raise NotImplementedError("This method must be overridden")

    def open(self, mode):
        raise NotImplementedError("Cannot open() PostgresTarget")


class CopyToTable(luigi.Task):
    """
    Template task for inserting a data set into Postgres

    Usage:
    Subclass and override the required `host`, `database`, `user`,
    `password`, `table` and `columns` attributes.
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
        """ Override to provide code for creating the target table.

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


    def update_id(self):
        """This update id will be a unique identifier for this insert on this table."""
        return self.task_id

    @abc.abstractmethod
    def output(self):
        raise NotImplementedError("This method must be overridden")

    def init_copy(self, connection):
        """ Override to perform custom queries.

            Any code here will be formed in the same transaction as the main copy, just prior to copying data. Example use cases include truncating the table or removing all data older than X in the database to keep a rolling window of data available in the table.
        """

        # TODO: remove this after sufficient time so most people using the
        # clear_table attribtue will have noticed it doesn't work anymore
        if hasattr(self, "clear_table"):
            raise Exception("The clear_table attribute has been removed. Override init_copy instead!")

    @abc.abstractmethod
    def copy(self, cursor, file):
        raise NotImplementedError("This method must be overridden")
