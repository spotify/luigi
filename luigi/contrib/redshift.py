import datetime
import abc
import logging
import luigi.postgres
import luigi
from luigi.contrib import postgreslike

logger = logging.getLogger('luigi-interface')

try:
    import psycopg2
    import psycopg2.errorcodes
    import psycopg2.extensions
except ImportError:
    logger.warning("Loading postgres module without psycopg2 installed. Will crash at runtime if postgres functionality is used.")


class RedshiftTarget(postgreslike.PostgreslikeTarget):
    """
    Target for a resource in Redshift.

    Redshift is similar to postgres with a few adjustments required by redshift
    """
    marker_table = luigi.configuration.get_config().get('redshift', 'marker-table', 'table_updates')

    def touch(self, connection=None):
        """Mark this update as complete.

        Important: If the marker table doesn't exist,
        the connection transaction will be aborted
        and the connection reset. Then the marker table will be created.
        """
        self.create_marker_table()

        if connection is None:
            connection = self.connect()
            # if connection created here, we commit it here
            connection.autocommit = True

        # Automatic timestamps aren't supported by redshift, so we
        # send the insertion date explicitly
        connection.cursor().execute(
                """INSERT INTO {marker_table} (update_id, target_table, inserted)
                     VALUES (%s, %s, %s);
                """.format(marker_table=self.marker_table),
                        (self.update_id, self.table,
                        datetime.datetime.now())
        )
        # make sure update is properly marked
        assert self.exists(connection)

    def create_marker_table(self):
        """Create marker table if it doesn't exist.

        Using a separate connection since the transaction might have to be reset
        """
        connection = self.connect()
        connection.autocommit = True
        cursor = connection.cursor()
        try:
                cursor.execute(
                        """ CREATE TABLE {marker_table} (
                                        update_id TEXT PRIMARY KEY,
                                        target_table TEXT,
                                        inserted TIMESTAMP);
                        """
                        .format(marker_table=self.marker_table)
                )
        except psycopg2.ProgrammingError, e:
                if e.pgcode == psycopg2.errorcodes.DUPLICATE_TABLE:
                        pass
                else:
                        raise
        connection.close()


class S3CopyToTable(postgreslike.CopyToTable):
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
