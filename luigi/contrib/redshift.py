import abc
import logging
import luigi.postgres
import luigi
from luigi.contrib import rdbms
from luigi import postgres

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

    def run(self):
        """
        If the target table doesn't exist, self.create_table will be called
        to attempt to create the table.
        """
        if not (self.table):
            raise Exception("table need to be specified")

        connection = self.output().connect()

        path = self.s3_load_path()
        logger.info("Inserting file: %s", path)

        # attempt to copy the data into postgres
        # if it fails because the target table doesn't exist
        # try to create it by running self.create_table
        for attempt in xrange(2):
            try:
                cursor = connection.cursor()
                self.init_copy(connection)
                self.copy(cursor, path)
            except psycopg2.ProgrammingError, e:
                if e.pgcode == psycopg2.errorcodes.UNDEFINED_TABLE and attempt == 0:
                    # if first attempt fails with "relation not found",
                    # try creating table
                    logger.info("Creating table %s", self.table)
                    connection.reset()
                    self.create_table(connection)
                else:
                    raise
            else:
                break

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
