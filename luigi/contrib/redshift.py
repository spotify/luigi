import abc
import logging
import luigi.postgres
import luigi
import json
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
