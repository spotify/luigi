import abc
import json
import logging
import luigi.postgres
import luigi
import json
import time
from luigi.contrib import rdbms
from luigi import postgres
from luigi.s3 import S3PathTask, S3Target


logger = logging.getLogger('luigi-interface')


try:
    import psycopg2
    import psycopg2.errorcodes
except ImportError:
    logger.warning("Loading postgres module without psycopg2 installed. Will crash at runtime if postgres functionality is used.")


class RedshiftTarget(postgres.PostgresTarget):
    """
    Target for a resource in Redshift.

    Redshift is similar to postgres with a few adjustments required by redshift
    """
    marker_table = luigi.configuration.get_config().get('redshift', 'marker-table', 'table_updates')

    use_db_timestamps = False


class S3CopyToTable(rdbms.CopyToTable):
    """
    Template task for inserting a data set into Redshift from s3.

    Usage:
    Subclass and override the required attributes:
    `host`, `database`, `user`, `password`, `table`, `columns`,
    `aws_access_key_id`, `aws_secret_access_key`, `s3_load_path`
    """

    @abc.abstractproperty
    def s3_load_path(self):
        'override to return the load path'
        return None

    @abc.abstractproperty
    def aws_access_key_id(self):
        'override to return the key id'
        return None

    @abc.abstractproperty
    def aws_secret_access_key(self):
        'override to return the secret access key'
        return None

    @abc.abstractproperty
    def copy_options(self):
        '''Add extra copy options, for example:

         TIMEFORMAT 'auto'
         IGNOREHEADER 1
         TRUNCATECOLUMNS
         IGNOREBLANKLINES
        '''
        return ''

    def do_truncate_table(self):
        """
        Return True if table should be truncated before copying new data in.
        """
        return False

    def truncate_table(self, connection):
        query = "truncate %s" % self.table
        cursor = connection.cursor()
        try:
            cursor.execute(query)
        finally:
            cursor.close()

    def does_table_exist(self, connection):
        """
        Determine whether the table already exists.
        """
        query = "select 1 as table_exists from pg_table_def where tablename = %s limit 1"
        cursor = connection.cursor()
        try:
            cursor.execute(query, (self.table,))
            result = cursor.fetchone()
            return bool(result)
        finally:
            cursor.close()

    def run(self):
        """
        If the target table doesn't exist, self.create_table will be called
        to attempt to create the table.
        """
        if not (self.table):
            raise Exception("table need to be specified")

        path = self.s3_load_path()
        connection = self.output().connect()

        if not self.does_table_exist(connection):
            # try creating table
            logger.info("Creating table %s with columns %s", (self.table, self.columns))
            connection.reset()
            self.create_table(connection)
        elif self.do_truncate_table():
            logger.info("Truncating table %s", self.table)
            self.truncate_table(connection)
            logger.info("Done truncating table %s", self.table)

        logger.info("Inserting file: %s", path)
        cursor = connection.cursor()
        self.init_copy(connection)
        self.copy(cursor, path)
        self.output().touch(connection)
        connection.commit()

        # commit and clean up
        connection.close()

    def copy(self, cursor, f):
        '''
        Defines copying from s3 into redshift
        '''

        cursor.execute("""
         COPY %s from '%s'
         CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'
         delimiter '%s'
         %s
         ;""" % (self.table, f, self.aws_access_key_id,
                 self.aws_secret_access_key, self.column_separator,
                 self.copy_options))

    def output(self):
        """Returns a RedshiftTarget representing the inserted dataset.

        Normally you don't override this.
        """
        return RedshiftTarget(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                table=self.table,
                update_id=self.update_id())


class S3CopyJSONToTable(S3CopyToTable):
    """
    Template task for inserting a JSON data set into Redshift from s3.

    Usage:
    Subclass and override the required attributes:
    `host`, `database`, `user`, `password`, `table`, `columns`,
    `aws_access_key_id`, `aws_secret_access_key`, `s3_load_path`,
    `jsonpath`, `copy_json_options`
    """

    @abc.abstractproperty
    def jsonpath(self):
        'override the jsonpath schema location for the table'
        return ''

    @abc.abstractproperty
    def copy_json_options(self):
        '''Add extra copy options, for example:
        GZIP
        LZOP
        '''
        return ''

    def copy(self, cursor, f):
        '''
        Defines copying JSON from s3 into redshift
        '''

        cursor.execute("""
         COPY %s from '%s'
         CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'
         JSON AS '%s' %s
         %s
         ;""" % (self.table, f, self.aws_access_key_id,
                 self.aws_secret_access_key, self.jsonpath,
                 self.copy_json_options, self.copy_options))


class RedshiftManifestTask(S3PathTask):
    """
    Generic task to generate a manifest file that can be used
    in S3CopyToTable in order to copy multiple files from your
    s3 folder into a redshift table at once

    For full description on how to use the manifest file see:
    http://docs.aws.amazon.com/redshift/latest/dg/loading-data-files-using-manifest.html

    Usage:
    Requires parameters
        path - s3 path to the generated manifest file, including the
               name of the generated file
                      to be copied into a redshift table
        folder_paths - s3 paths to the folders containing files you wish to be copied
    Output:
        generated manifest file
    """

    # should be over ridden to point to a variety of folders you wish to copy from
    folder_paths = luigi.Parameter()

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
        target.write(json.dumps(manifest))
        target.close()

class KillOpenRedshiftSessions(luigi.Task):
    """
    An abstract task for killing any open Redshift sessions
    in a given database. This is necessary to prevent open user sessions
    from blocking drop or truncate table commands.

    Usage:
    Subclass and override the required `host`, `database`, `user`,
    and `password` attributes.
    """

    # time in seconds to wait before
    # reconnecting to Redshift if our session is killed too.
    # 30 seconds is usually fine; 60 is conservative
    connection_reset_wait_seconds = luigi.IntParameter(60)

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
    
    def update_id(self):
        """This update id will be a unique identifier for this insert on this table."""
        return self.task_id

    def output(self):
        """Returns a RedshiftTarget representing the inserted dataset.

        Normally you don't override this.
        """
        return RedshiftTarget(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                table=self.__class__.__name__,
                update_id=self.update_id())

    def run(self):
        """
        Kill any Redshift sessions for the given database.
        """
        connection = self.output().connect()
        # kill any sessions other than ours and
        # internal Redshift sessions (rdsdb)
        query = """SELECT pg_terminate_backend(process) 
                     FROM STV_SESSIONS 
                    WHERE db_name=%s  
                      AND user_name != 'rdsdb' 
                      AND process != pg_backend_pid()"""
        cursor = connection.cursor()
        logger.info("Killing all open Redshift sessions for database: %s" % self.database)
        try:
            cursor.execute(query, (self.database,))
            cursor.close()
            connection.commit()
        except psycopg2.DatabaseError, e:
            if e.message and 'EOF' in e.message:
                # sometimes this operation kills the current session.
                # rebuild the connection. Need to pause for 30-60 seconds
                # before Redshift will allow us back in.
                connection.close()
                logger.info("Pausing %s seconds for Redshift to reset connection" % \
                    self.connection_reset_wait_seconds)
                time.sleep(self.connection_reset_wait_seconds)
                logger.info("Reconnecting to Redshift")
                connection = self.output().connect()
            else:
                raise

        try:
            self.output().touch(connection)
            connection.commit()
        finally:
            connection.close()
        logger.info("Done killing all open Redshift sessions for database: %s" % self.database)
