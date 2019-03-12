import abc
import logging

import luigi
from luigi.contrib import rdbms
from luigi.contrib import redshift
from luigi.contrib import postgres

logger = logging.getLogger('luigi-interface')

# Snowflake's library does not have constant for their error code.
# We define our own
SNOWFLAKE_TABLE_EXISTS_ERROR = 2002
SNOWFLAKE_GENERIC_PROGRAMMING_ERROR = 2003

try:
    import snowflake
    import snowflake.connector
except ImportError:
    logger.warning("Loading snowflake module without snowflake-connector-python installed. "
                   "Will crash at runtime if snowflake functionality is used.")


class _SettingsMixins():
    """
    Generic mixins that defines how to fetch the database and warehouse
    configuration for a given snowflake integration.
    """
    @property
    def configuration_section(self):
        """
        Override to change the configuration section used
        to obtain default credentials.
        """
        return 'snowflake'

    @property
    def warehouse(self):
        """
        Fetch the warehouse configuration from the luigi configuration file under:

        [snowflake]
        warehouse=value
        """
        return luigi.configuration.get_config().get(self.configuration_section, 'warehouse', default=None)

    @property
    def role(self):
        """
        Fetch the role configuration from the luigi configuration file under:

        [snowflake]
        role=value
        """
        return luigi.configuration.get_config().get(self.configuration_section, 'role', default=None)


class _CredentialsMixin(redshift._CredentialsMixin):
    def _credentials(self):
        """
        Return a credential string for the provided task.

        This currently only support key-based credentials.

        If no valid credentials are set, raise a NotImplementedError.
        """
        if self.aws_access_key_id and self.aws_secret_access_key:
            return "AWS_KEY_ID='{key}' AWS_SECRET_KEY='{secret}'".format(
                key=self.aws_access_key_id,
                secret=self.aws_secret_access_key)
        else:
            raise NotImplementedError("Missing Credentials. "
                                      "Ensure that the auth args below are set "
                                      "in a configuration file, environment variables or by "
                                      "being overridden in the task: "
                                      "'aws_access_key_id' AND 'aws_secret_access_key'")


class SnowflakeTarget(postgres.PostgresTarget):
    """
    Target for a resource in Snowflake.

    Snowflake connection string is similar to postgres with a few adjustments.
    """
    marker_table = luigi.configuration.get_config().get(
        'snowflake',
        'marker-table',
        'table_updates')

    use_db_timestamps = False

    def __init__(self, host, database, warehouse, user, password, role, table, update_id):
        """
        Args:
            host (str): Snowflake server address.
            warehouse (str): Snowflake warehouse name.
            database (str): Database name
            user (str): Database user
            password (str): Password for specified user
            role (str): Snowflake user's role name.
            update_id (str): An identifier for this data set
        """
        self.host = host
        self.database = database
        self.warehouse = warehouse
        self.user = user
        self.password = password
        self.role = role
        self.table = table
        self.update_id = update_id

    def connect(self):
        """
        Get a snowflake connection object.
        """
        connection = snowflake.connector.connect(
            account=self.host,
            user=self.user,
            password=self.password)

        cursor = connection.cursor()

        # Default database, warehouse and role can be set directly at the user
        # level in the Snowflake configurations. Unless specified, we assume
        # that the defaults have been set.
        if self.role:
            cursor.execute("USE ROLE {role}".format(role=self.role))
        if self.warehouse:
            cursor.execute("USE WAREHOUSE {warehouse}".format(warehouse=self.warehouse))
        if self.database:
            cursor.execute("USE DATABASE {database}".format(database=self.database))

        return connection

    def exists(self, connection=None):
        """
        Check if the task has already been run for a given update_id from the
        marker_table

        Do not re-run the task if it has been previously run
        """
        new_connection = connection is None
        if new_connection:
            connection = self.connect()
            connection.autocommit = True
        cursor = connection.cursor()
        try:
            cursor.execute("""SELECT 1 FROM {marker_table} WHERE update_id = '{update_id}' LIMIT 1""".format(marker_table=self.marker_table,
                                                                                                             update_id=self.update_id))
            row = cursor.fetchone()
        except snowflake.connector.errors.ProgrammingError as e:
            # Snowflake doesn't seem to have constants for their error code
            if e.errno == SNOWFLAKE_GENERIC_PROGRAMMING_ERROR:
                row = None
            else:
                logger.error('Snowflake error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
                raise

        if new_connection:
            connection.close()

        return row is not None

    def create_marker_table(self):
        """
        Create marker table if it doesn't exist.
        Using a separate connection since the transaction might have to be reset.
        """
        connection = self.connect()
        connection.autocommit = True
        cursor = connection.cursor()
        if self.use_db_timestamps:
            sql = """CREATE TABLE {marker_table} (
                     update_id TEXT PRIMARY KEY,
                     target_table TEXT,
                     inserted TIMESTAMP DEFAULT NOW())""".format(marker_table=self.marker_table)
        else:
            sql = """CREATE TABLE {marker_table} (
                     update_id TEXT PRIMARY KEY,
                     target_table TEXT,
                     inserted TIMESTAMP);""".format(marker_table=self.marker_table)

        try:
            cursor.execute(sql)
        except snowflake.connector.errors.ProgrammingError as e:
            if e.errno == SNOWFLAKE_TABLE_EXISTS_ERROR:
                pass
            else:
                logger.error('Snowflake error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
                raise

        connection.close()


class S3CopyToTable(rdbms.CopyToTable, _CredentialsMixin, _SettingsMixins):
    """
    Template task for inserting a data set into Snowflake from s3.

    Subclass and override the required attributes:
        * `host`,
        * `database`,
        * `user`,
        * `password`,
        * `table`,
        * `columns`,
        * `s3_load_path`.

    You can also override the attributes provided by the CredentialsMixin if
    they are not supplied by your configuration or environment variables.
    """
    configuration_section = 'snowflake'

    @abc.abstractmethod
    def s3_load_path(self):
        """
        Override to return the load path.
        """
        return None

    @abc.abstractproperty
    def copy_options(self):
        """
        Override to add extra copy options, for example:

        * ON_ERROR
        * SIZE_LIMIT 1
        * PURGE
        * RETURN_FAILED_ONLY
        * ENFORCE_LENGTH

        For more information:
        https://docs.snowflake.net/manuals/sql-reference/sql/copy-into-table.html#copy-options-copyoptions
        """
        return ''

    @property
    def do_truncate_table(self):
        """
        Override to return true if table should be truncated before copying new
        data in.
        """
        return False

    @property
    def queries(self):
        """
        Override to return a list of queries to be executed in order.
        """
        return []

    def truncate_table(self, connection):
        """
        Truncate the table
        """
        query = "truncate {table}".format(table=self.table)
        cursor = connection.cursor()
        try:
            cursor.execute(query)
        finally:
            cursor.close()

    def create_schema(self, connection):
        """
        Will create the schema in the database if it does not exists.

        If no schema is detected in the table string, then we assume that it
        uses the default schema of the database which is "public" if not
        overriden.

        The default schema should always exists, so there is no point in
        creating it.
        """
        if '.' not in self.table:
            return

        query = 'CREATE SCHEMA IF NOT EXISTS {schema_name};'.format(schema_name=self.table.split('.')[0])
        connection.cursor().execute(query)

    def run(self):
        """
        COPY data residing on S3 to a Snowflake table following these steps:

        * Fetch the file path from S3
        * Define the ouput as a Snowflake Target
        * Create a connection and a cursor to copy data to Snowflake

        * Create the schema if it doesn't already exists
        * Create the table if it doesn't already exists
        * Truncate the table if the flag "do_truncate_table" is set

        * Copy the data from S3 to the Snowflake table

        * Run any potential "post_copy" queries

        * Save the state of this task to Snowflake as "been run"
        """
        path = self.s3_load_path()
        output = self.output()
        connection = output.connect()
        cursor = connection.cursor()

        self.init_copy(connection)
        self.copy(cursor, path)
        self.post_copy(cursor)

        # update marker table
        output.touch()
        connection.commit()

        # commit and clean up
        connection.close()

    def copy(self, cursor, f):
        """
        COPY data from from S3 into Snowflake.

        This currently only works for key-base credentials.
        """
        logger.info("Inserting file: {file}".format(file=f))
        colnames = ''
        if self.columns and len(self.columns) > 0:
            colnames = ",".join([x[0] for x in self.columns])
            colnames = '({})'.format(colnames)

        query = ("""COPY INTO {table} from {source} CREDENTIALS=({creds}) {options};""".format(
            table=self.table,
            source=f,
            creds=self._credentials(),
            options=self.copy_options)
        )
        cursor.execute(query)

    def output(self):
        """
        Returns a SnowflakeTarget representing the inserted dataset.
        """
        return SnowflakeTarget(
            host=self.host,
            warehouse=self.warehouse,
            database=self.database,
            user=self.user,
            password=self.password,
            role=self.role,
            table=self.table,
            update_id=self.update_id)

    def does_schema_exist(self, connection):
        """
        Determine whether the schema already exists.

        If no schema is detected in the table string, then we assume that it
        uses the default schema of the database which is "public" if not
        overriden.

        The default schema should always exists, so we automatically return
        true.
        """

        if '.' in self.table:
            schema = self.table.split('.')[0]
            query = ("select 1 as schema_exists "
                     "from information_schema.schemata "
                     "where lower(schema_name) = lower('{schema}') limit 1").format(schema=schema)
        else:
            return True

        cursor = connection.cursor()
        try:
            cursor.execute(query.format(schema=schema))
            result = cursor.fetchone()
            return bool(result)
        finally:
            cursor.close()

    def does_table_exist(self, connection):
        """
        Determine whether the table already exists.

        If a schema is detected in the table string we check if the table
        exists in that given schema else we check if it exists anywhere.

        We lower the table and schema value as Snowflake can default things to
        uppercase thus lowercasing everything ensures the same behavior
        every time.
        """

        if '.' in self.table:
            schema, table = self.table.split('.')
            query = ("select 1 as table_exists "
                     "from information_schema.tables "
                     "where lower(table_schema) = lower('{schema}') and lower(table_name) = lower('{table}') limit 1").format(schema=schema, table=table)
        else:
            query = ("select 1 as table_exists "
                     "from information_schema.tables "
                     "where lower(table_name) = lower('{table}') limit 1").format(table=table)
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            result = cursor.fetchone()
            return bool(result)
        finally:
            cursor.close()

    def init_copy(self, connection):
        """
        Perform pre-copy sql - such as creating schema, creating table or
        truncating.
        """
        if not self.does_schema_exist(connection):
            logger.info("Creating schema for {table}".format(table=self.table))
            self.create_schema(connection)

        if not self.does_table_exist(connection):
            logger.info("Creating table {table}".format(table=self.table))
            self.create_table(connection)

        if self.do_truncate_table:
            logger.info("Truncating table {table}".format(table=self.table))
            self.truncate_table(connection)

    def post_copy(self, cursor):
        """
        Performs post-copy sql - such as cleansing data as defined by the task
        """
        logger.info('Executing post copy queries')
        for query in self.queries:
            cursor.execute(query)


class SnowflakeQuery(rdbms.Query, _SettingsMixins):
    """
    Template task for making queries to Snowflake.

    Subclass and override the required attributes:
        * `host`,
        * `database`,
        * `user`,
        * `password`,
        * `table`,

    Note:
        This uses execute_string which is not safe for SQL injections
        https://docs.snowflake.net/manuals/user-guide/python-connector-api.html#execute_string

    Usage:
        Subclass and override the required `host`, `database`, `user`, `password`, `table`, and `query` attributes.
        Optionally one can override the `autocommit` attribute to put the connection for the query in autocommit mode.
        Override the `run` method if your use case requires some action with the query result.
        Task instances require a dynamic `update_id`, e.g. via parameter(s), otherwise the query will only execute once
        To customize the query signature as recorded in the database marker table, override the `update_id` property.
    """
    def run(self):
        connection = self.output().connect()
        connection.autocommit = self.autocommit

        logger.info('Executing query from task: {name}'.format(name=self.__class__.__name__))
        connection.execute_string(self.query, return_cursors=False)

        # Update marker table
        self.output().touch()

        # commit and close connection
        connection.commit()
        connection.close()

    def output(self):
            """
            Returns a SnowflakeTarget representing the executed query.
            """
            return SnowflakeTarget(
                host=self.host,
                warehouse=self.warehouse,
                database=self.database,
                user=self.user,
                password=self.password,
                role=self.role,
                table=self.table,
                update_id=self.update_id)
