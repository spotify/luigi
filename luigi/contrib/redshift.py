import abc
import logging
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
    logger.warning("Loading postgres module without psycopg2 installed. "
                   "Will crash at runtime if postgres functionality is used.")


class RedshiftTarget(postgres.PostgresTarget):
    """
    Target for a resource in Redshift.

    Redshift is similar to postgres with a few adjustments
    required by redshift.
    """
    marker_table = luigi.configuration.get_config().get(
        'redshift',
        'marker-table',
        'table_updates')

    use_db_timestamps = False


class S3CopyToTable(rdbms.CopyToTable):
    """
    Template task for inserting a data set into Redshift from s3.

    Usage:

    * Subclass and override the required attributes:
      * `host`,
      * `database`,
      * `user`,
      * `password`,
      * `table`,
      * `columns`,
      * `aws_access_key_id`,
      * `aws_secret_access_key`,
      * `s3_load_path`.
    """

    @abc.abstractproperty
    def s3_load_path(self):
        """
        Override to return the load path.
        """
        return None

    @abc.abstractproperty
    def aws_access_key_id(self):
        """
        Override to return the key id.
        """
        return None

    @abc.abstractproperty
    def aws_secret_access_key(self):
        """
        Override to return the secret access key.
        """
        return None

    @abc.abstractproperty
    def copy_options(self):
        """
        Add extra copy options, for example:

        * TIMEFORMAT 'auto'
        * IGNOREHEADER 1
        * TRUNCATECOLUMNS
        * IGNOREBLANKLINES
        """
        return ''

    def table_attributes(self):
        '''Add extra table attributes, for example:
        DISTSTYLE KEY
        DISTKEY (MY_FIELD)
        SORTKEY (MY_FIELD_2, MY_FIELD_3)
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

    def create_table(self, connection):
        """
        Override to provide code for creating the target table.

        By default it will be created using types (optionally)
        specified in columns.

        If overridden, use the provided connection object for
        setting up the table in order to create the table and
        insert data using the same transaction.
        """
        if len(self.columns[0]) == 1:
            # only names of columns specified, no types
            raise NotImplementedError("create_table() not implemented "
                                      "for %r and columns types not "
                                      "specified" % self.table)
        elif len(self.columns[0]) == 2:
            # if columns is specified as (name, type) tuples
            coldefs = ','.join(
                '{name} {type}'.format(
                    name=name,
                    type=type) for name, type in self.columns
            )
            query = ("CREATE TABLE "
                     "{table} ({coldefs}) "
                     "{table_attributes}").format(
                table=self.table,
                coldefs=coldefs,
                table_attributes=self.table_attributes())
            connection.cursor().execute(query)

    def run(self):
        """
        If the target table doesn't exist, self.create_table
        will be called to attempt to create the table.
        """
        if not (self.table):
            raise Exception("table need to be specified")

        path = self.s3_load_path()
        connection = self.output().connect()
        if not self.does_table_exist(connection):
            # try creating table
            logger.info("Creating table %s", self.table)
            connection.reset()
            self.create_table(connection)
        elif self.do_truncate_table():
            logger.info("Truncating table %s", self.table)
            self.truncate_table(connection)

        logger.info("Inserting file: %s", path)
        cursor = connection.cursor()
        self.init_copy(connection)
        self.copy(cursor, path)
        self.output().touch(connection)
        connection.commit()

        # commit and clean up
        connection.close()

    def copy(self, cursor, f):
        """
        Defines copying from s3 into redshift.
        """

        cursor.execute("""
         COPY %s from '%s'
         CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'
         delimiter '%s'
         %s
         ;""" % (self.table, f, self.aws_access_key_id,
                 self.aws_secret_access_key, self.column_separator,
                 self.copy_options))

    def output(self):
        """
        Returns a RedshiftTarget representing the inserted dataset.

        Normally you don't override this.
        """
        return RedshiftTarget(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            table=self.table,
            update_id=self.update_id())

    def does_table_exist(self, connection):
        """
        Determine whether the table already exists.
        """
        query = ("select 1 as table_exists "
                 "from pg_table_def "
                 "where tablename = %s limit 1")
        cursor = connection.cursor()
        try:
            cursor.execute(query, (self.table,))
            result = cursor.fetchone()
            return bool(result)
        finally:
            cursor.close()


class S3CopyJSONToTable(S3CopyToTable):
    """
    Template task for inserting a JSON data set into Redshift from s3.

    Usage:

        * Subclass and override the required attributes:

            * `host`,
            * `database`,
            * `user`,
            * `password`,
            * `table`,
            * `columns`,
            * `aws_access_key_id`,
            * `aws_secret_access_key`,
            * `s3_load_path`,
            * `jsonpath`,
            * `copy_json_options`.
    """

    @abc.abstractproperty
    def jsonpath(self):
        """
        Override the jsonpath schema location for the table.
        """
        return ''

    @abc.abstractproperty
    def copy_json_options(self):
        """
        Add extra copy options, for example:

        * GZIP
        * LZOP
        """
        return ''

    def copy(self, cursor, f):
        """
        Defines copying JSON from s3 into redshift.
        """

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
    s3 folder into a redshift table at once.

    For full description on how to use the manifest file see
    http://docs.aws.amazon.com/redshift/latest/dg/loading-data-files-using-manifest.html

    Usage:

        * requires parameters
            * path - s3 path to the generated manifest file, including the
                     name of the generated file
                     to be copied into a redshift table
            * folder_paths - s3 paths to the folders containing files you wish to be copied

    Output:

        * generated manifest file
    """

    # should be over ridden to point to a variety
    # of folders you wish to copy from
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
