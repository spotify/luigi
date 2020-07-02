# -*- coding: utf-8 -*-
#
# Copyright 2015 Twitter Inc
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

import collections
import logging
import luigi.target
import time
from luigi.contrib import gcp

logger = logging.getLogger('luigi-interface')

try:
    from googleapiclient import discovery
    from googleapiclient import http
except ImportError:
    logger.warning('BigQuery module imported, but google-api-python-client is '
                   'not installed. Any BigQuery task will fail')


class CreateDisposition:
    CREATE_IF_NEEDED = 'CREATE_IF_NEEDED'
    CREATE_NEVER = 'CREATE_NEVER'


class WriteDisposition:
    WRITE_TRUNCATE = 'WRITE_TRUNCATE'
    WRITE_APPEND = 'WRITE_APPEND'
    WRITE_EMPTY = 'WRITE_EMPTY'


class QueryMode:
    INTERACTIVE = 'INTERACTIVE'
    BATCH = 'BATCH'


class SourceFormat:
    AVRO = 'AVRO'
    CSV = 'CSV'
    DATASTORE_BACKUP = 'DATASTORE_BACKUP'
    NEWLINE_DELIMITED_JSON = 'NEWLINE_DELIMITED_JSON'


class FieldDelimiter:
    """
    The separator for fields in a CSV file. The separator can be any ISO-8859-1 single-byte character.
    To use a character in the range 128-255, you must encode the character as UTF8.
    BigQuery converts the string to ISO-8859-1 encoding, and then uses the
    first byte of the encoded string to split the data in its raw, binary state.
    BigQuery also supports the escape sequence "\t" to specify a tab separator.
    The default value is a comma (',').

    https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load
    """

    COMMA = ','  # Default
    TAB = "\t"
    PIPE = "|"


class PrintHeader:
    TRUE = True
    FALSE = False


class DestinationFormat:
    AVRO = 'AVRO'
    CSV = 'CSV'
    NEWLINE_DELIMITED_JSON = 'NEWLINE_DELIMITED_JSON'


class Compression:
    GZIP = 'GZIP'
    NONE = 'NONE'


class Encoding:
    """
    [Optional] The character encoding of the data. The supported values are UTF-8 or ISO-8859-1. The default value is UTF-8.

    BigQuery decodes the data after the raw, binary data has been split using the values of the quote and fieldDelimiter properties.
    """

    UTF_8 = 'UTF-8'
    ISO_8859_1 = 'ISO-8859-1'


BQDataset = collections.namedtuple('BQDataset', 'project_id dataset_id location')


class BQTable(collections.namedtuple('BQTable', 'project_id dataset_id table_id location')):
    @property
    def dataset(self):
        return BQDataset(project_id=self.project_id, dataset_id=self.dataset_id, location=self.location)

    @property
    def uri(self):
        return "bq://" + self.project_id + "/" + \
               self.dataset.dataset_id + "/" + self.table_id


class BigQueryClient:
    """A client for Google BigQuery.

    For details of how authentication and the descriptor work, see the
    documentation for the GCS client. The descriptor URL for BigQuery is
    https://www.googleapis.com/discovery/v1/apis/bigquery/v2/rest
    """

    def __init__(self, oauth_credentials=None, descriptor='', http_=None):
        authenticate_kwargs = gcp.get_authenticate_kwargs(oauth_credentials, http_)

        if descriptor:
            self.client = discovery.build_from_document(descriptor, **authenticate_kwargs)
        else:
            self.client = discovery.build('bigquery', 'v2', cache_discovery=False, **authenticate_kwargs)

    def dataset_exists(self, dataset):
        """Returns whether the given dataset exists.
        If regional location is specified for the dataset, that is also checked
        to be compatible with the remote dataset, otherwise an exception is thrown.

           :param dataset:
           :type dataset: BQDataset
        """

        try:
            response = self.client.datasets().get(projectId=dataset.project_id,
                                                  datasetId=dataset.dataset_id).execute()
            if dataset.location is not None:
                fetched_location = response.get('location')
                if dataset.location != fetched_location:
                    raise Exception('''Dataset already exists with regional location {}. Can't use {}.'''.format(
                        fetched_location if fetched_location is not None else 'unspecified',
                        dataset.location))

        except http.HttpError as ex:
            if ex.resp.status == 404:
                return False
            raise

        return True

    def table_exists(self, table):
        """Returns whether the given table exists.

           :param table:
           :type table: BQTable
        """
        if not self.dataset_exists(table.dataset):
            return False

        try:
            self.client.tables().get(projectId=table.project_id,
                                     datasetId=table.dataset_id,
                                     tableId=table.table_id).execute()
        except http.HttpError as ex:
            if ex.resp.status == 404:
                return False
            raise

        return True

    def make_dataset(self, dataset, raise_if_exists=False, body=None):
        """Creates a new dataset with the default permissions.

           :param dataset:
           :type dataset: BQDataset
           :param raise_if_exists: whether to raise an exception if the dataset already exists.
           :raises luigi.target.FileAlreadyExists: if raise_if_exists=True and the dataset exists
        """

        if body is None:
            body = {}

        try:
            # Construct a message body in the format required by
            # https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/python/latest/bigquery_v2.datasets.html#insert
            body['datasetReference'] = {
                'projectId': dataset.project_id,
                'datasetId': dataset.dataset_id
            }
            if dataset.location is not None:
                body['location'] = dataset.location
            self.client.datasets().insert(projectId=dataset.project_id, body=body).execute()
        except http.HttpError as ex:
            if ex.resp.status == 409:
                if raise_if_exists:
                    raise luigi.target.FileAlreadyExists()
            else:
                raise

    def delete_dataset(self, dataset, delete_nonempty=True):
        """Deletes a dataset (and optionally any tables in it), if it exists.

           :param dataset:
           :type dataset: BQDataset
           :param delete_nonempty: if true, will delete any tables before deleting the dataset
        """

        if not self.dataset_exists(dataset):
            return

        self.client.datasets().delete(projectId=dataset.project_id,
                                      datasetId=dataset.dataset_id,
                                      deleteContents=delete_nonempty).execute()

    def delete_table(self, table):
        """Deletes a table, if it exists.

           :param table:
           :type table: BQTable
        """

        if not self.table_exists(table):
            return

        self.client.tables().delete(projectId=table.project_id,
                                    datasetId=table.dataset_id,
                                    tableId=table.table_id).execute()

    def list_datasets(self, project_id):
        """Returns the list of datasets in a given project.

           :param project_id:
           :type project_id: str
        """

        request = self.client.datasets().list(projectId=project_id,
                                              maxResults=1000)
        response = request.execute()

        while response is not None:
            for ds in response.get('datasets', []):
                yield ds['datasetReference']['datasetId']

            request = self.client.datasets().list_next(request, response)
            if request is None:
                break

            response = request.execute()

    def list_tables(self, dataset):
        """Returns the list of tables in a given dataset.

           :param dataset:
           :type dataset: BQDataset
        """

        request = self.client.tables().list(projectId=dataset.project_id,
                                            datasetId=dataset.dataset_id,
                                            maxResults=1000)
        response = request.execute()

        while response is not None:
            for t in response.get('tables', []):
                yield t['tableReference']['tableId']

            request = self.client.tables().list_next(request, response)
            if request is None:
                break

            response = request.execute()

    def get_view(self, table):
        """Returns the SQL query for a view, or None if it doesn't exist or is not a view.

        :param table: The table containing the view.
        :type table: BQTable
        """

        request = self.client.tables().get(projectId=table.project_id,
                                           datasetId=table.dataset_id,
                                           tableId=table.table_id)

        try:
            response = request.execute()
        except http.HttpError as ex:
            if ex.resp.status == 404:
                return None
            raise

        return response['view']['query'] if 'view' in response else None

    def update_view(self, table, view):
        """Updates the SQL query for a view.

        If the output table exists, it is replaced with the supplied view query. Otherwise a new
        table is created with this view.

        :param table: The table to contain the view.
        :type table: BQTable
        :param view: The SQL query for the view.
        :type view: str
        """

        body = {
            'tableReference': {
                'projectId': table.project_id,
                'datasetId': table.dataset_id,
                'tableId': table.table_id
            },
            'view': {
                'query': view
            }
        }

        if self.table_exists(table):
            self.client.tables().update(projectId=table.project_id,
                                        datasetId=table.dataset_id,
                                        tableId=table.table_id,
                                        body=body).execute()
        else:
            self.client.tables().insert(projectId=table.project_id,
                                        datasetId=table.dataset_id,
                                        body=body).execute()

    def run_job(self, project_id, body, dataset=None):
        """Runs a BigQuery "job". See the documentation for the format of body.

           .. note::
               You probably don't need to use this directly. Use the tasks defined below.

           :param dataset:
           :type dataset: BQDataset
           :return: the job id of the job.
           :rtype: str
           :raises luigi.contrib.BigQueryExecutionError: if the job fails.
        """

        if dataset and not self.dataset_exists(dataset):
            self.make_dataset(dataset)

        new_job = self.client.jobs().insert(projectId=project_id, body=body).execute()
        job_id = new_job['jobReference']['jobId']
        logger.info('Started import job %s:%s', project_id, job_id)
        while True:
            status = self.client.jobs().get(projectId=project_id, jobId=job_id).execute(num_retries=10)
            if status['status']['state'] == 'DONE':
                if status['status'].get('errorResult'):
                    raise BigQueryExecutionError(job_id, status['status']['errorResult'])
                return job_id

            logger.info('Waiting for job %s:%s to complete...', project_id, job_id)
            time.sleep(5)

    def copy(self,
             source_table,
             dest_table,
             create_disposition=CreateDisposition.CREATE_IF_NEEDED,
             write_disposition=WriteDisposition.WRITE_TRUNCATE):
        """Copies (or appends) a table to another table.

            :param source_table:
            :type source_table: BQTable
            :param dest_table:
            :type dest_table: BQTable
            :param create_disposition: whether to create the table if needed
            :type create_disposition: CreateDisposition
            :param write_disposition: whether to append/truncate/fail if the table exists
            :type write_disposition: WriteDisposition
        """

        job = {
            "configuration": {
                "copy": {
                    "sourceTable": {
                        "projectId": source_table.project_id,
                        "datasetId": source_table.dataset_id,
                        "tableId": source_table.table_id,
                    },
                    "destinationTable": {
                        "projectId": dest_table.project_id,
                        "datasetId": dest_table.dataset_id,
                        "tableId": dest_table.table_id,
                    },
                    "createDisposition": create_disposition,
                    "writeDisposition": write_disposition,
                }
            }
        }

        self.run_job(dest_table.project_id, job, dataset=dest_table.dataset)


class BigQueryTarget(luigi.target.Target):
    def __init__(self, project_id, dataset_id, table_id, client=None, location=None):
        self.table = BQTable(project_id=project_id, dataset_id=dataset_id, table_id=table_id, location=location)
        self.client = client or BigQueryClient()

    @classmethod
    def from_bqtable(cls, table, client=None):
        """A constructor that takes a :py:class:`BQTable`.

           :param table:
           :type table: BQTable
        """
        return cls(table.project_id, table.dataset_id, table.table_id, client=client)

    def exists(self):
        return self.client.table_exists(self.table)

    def __str__(self):
        return str(self.table)


class MixinBigQueryBulkComplete:
    """
    Allows to efficiently check if a range of BigQueryTargets are complete.
    This enables scheduling tasks with luigi range tools.

    If you implement a custom Luigi task with a BigQueryTarget output, make sure to also inherit
    from this mixin to enable range support.
    """

    @classmethod
    def bulk_complete(cls, parameter_tuples):
        # Instantiate the tasks to inspect them
        tasks_with_params = [(cls(p), p) for p in parameter_tuples]
        if not tasks_with_params:
            return

        # Grab the set of BigQuery datasets we are interested in
        datasets = {t.output().table.dataset for t, p in tasks_with_params}
        logger.info('Checking datasets %s for available tables', datasets)

        # Query the available tables for all datasets
        client = tasks_with_params[0][0].output().client
        available_datasets = filter(client.dataset_exists, datasets)
        available_tables = {d: set(client.list_tables(d)) for d in available_datasets}

        # Return parameter_tuples belonging to available tables
        for t, p in tasks_with_params:
            table = t.output().table
            if table.table_id in available_tables.get(table.dataset, []):
                yield p


class BigQueryLoadTask(MixinBigQueryBulkComplete, luigi.Task):
    """Load data into BigQuery from GCS."""
    @property
    def source_format(self):
        """The source format to use (see :py:class:`SourceFormat`)."""
        return SourceFormat.NEWLINE_DELIMITED_JSON

    @property
    def encoding(self):
        """The encoding of the data that is going to be loaded (see :py:class:`Encoding`)."""
        return Encoding.UTF_8

    @property
    def write_disposition(self):
        """What to do if the table already exists. By default this will fail the job.

           See :py:class:`WriteDisposition`"""
        return WriteDisposition.WRITE_EMPTY

    @property
    def schema(self):
        """Schema in the format defined at https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load.schema.

        If the value is falsy, it is omitted and inferred by BigQuery."""
        return []

    @property
    def max_bad_records(self):
        """ The maximum number of bad records that BigQuery can ignore when reading data.

        If the number of bad records exceeds this value, an invalid error is returned in the job result."""
        return 0

    @property
    def field_delimiter(self):
        """The separator for fields in a CSV file. The separator can be any ISO-8859-1 single-byte character."""
        return FieldDelimiter.COMMA

    def source_uris(self):
        """The fully-qualified URIs that point to your data in Google Cloud Storage.

        Each URI can contain one '*' wildcard character and it must come after the 'bucket' name."""
        return [x.path for x in luigi.task.flatten(self.input())]

    @property
    def skip_leading_rows(self):
        """The number of rows at the top of a CSV file that BigQuery will skip when loading the data.

        The default value is 0. This property is useful if you have header rows in the file that should be skipped."""
        return 0

    @property
    def allow_jagged_rows(self):
        """Accept rows that are missing trailing optional columns. The missing values are treated as nulls.

        If false, records with missing trailing columns are treated as bad records, and if there are too many bad records,

        an invalid error is returned in the job result. The default value is false. Only applicable to CSV, ignored for other formats."""
        return False

    @property
    def ignore_unknown_values(self):
        """Indicates if BigQuery should allow extra values that are not represented in the table schema.

        If true, the extra values are ignored. If false, records with extra columns are treated as bad records,

        and if there are too many bad records, an invalid error is returned in the job result. The default value is false.

        The sourceFormat property determines what BigQuery treats as an extra value:

        CSV: Trailing columns JSON: Named values that don't match any column names"""
        return False

    @property
    def allow_quoted_new_lines(self):
        """	Indicates if BigQuery should allow quoted data sections that contain newline characters in a CSV file. The default value is false."""
        return False

    def run(self):
        output = self.output()
        assert isinstance(output, BigQueryTarget), 'Output must be a BigQueryTarget, not %s' % (output)

        bq_client = output.client

        source_uris = self.source_uris()
        assert all(x.startswith('gs://') for x in source_uris)

        job = {
            'configuration': {
                'load': {
                    'destinationTable': {
                        'projectId': output.table.project_id,
                        'datasetId': output.table.dataset_id,
                        'tableId': output.table.table_id,
                    },
                    'encoding': self.encoding,
                    'sourceFormat': self.source_format,
                    'writeDisposition': self.write_disposition,
                    'sourceUris': source_uris,
                    'maxBadRecords': self.max_bad_records,
                    'ignoreUnknownValues': self.ignore_unknown_values
                }
            }
        }

        if self.source_format == SourceFormat.CSV:
            job['configuration']['load']['fieldDelimiter'] = self.field_delimiter
            job['configuration']['load']['skipLeadingRows'] = self.skip_leading_rows
            job['configuration']['load']['allowJaggedRows'] = self.allow_jagged_rows
            job['configuration']['load']['allowQuotedNewlines'] = self.allow_quoted_new_lines

        if self.schema:
            job['configuration']['load']['schema'] = {'fields': self.schema}
        else:
            job['configuration']['load']['autodetect'] = True

        bq_client.run_job(output.table.project_id, job, dataset=output.table.dataset)


class BigQueryRunQueryTask(MixinBigQueryBulkComplete, luigi.Task):

    @property
    def write_disposition(self):
        """What to do if the table already exists. By default this will fail the job.

           See :py:class:`WriteDisposition`"""
        return WriteDisposition.WRITE_TRUNCATE

    @property
    def create_disposition(self):
        """Whether to create the table or not. See :py:class:`CreateDisposition`"""
        return CreateDisposition.CREATE_IF_NEEDED

    @property
    def flatten_results(self):
        """Flattens all nested and repeated fields in the query results.
        allowLargeResults must be true if this is set to False."""
        return True

    @property
    def query(self):
        """The query, in text form."""
        raise NotImplementedError()

    @property
    def query_mode(self):
        """The query mode. See :py:class:`QueryMode`."""
        return QueryMode.INTERACTIVE

    @property
    def udf_resource_uris(self):
        """Iterator of code resource to load from a Google Cloud Storage URI (gs://bucket/path).
        """
        return []

    @property
    def use_legacy_sql(self):
        """Whether to use legacy SQL
        """
        return True

    def run(self):
        output = self.output()
        assert isinstance(output, BigQueryTarget), 'Output must be a BigQueryTarget, not %s' % (output)

        query = self.query
        assert query, 'No query was provided'

        bq_client = output.client

        logger.info('Launching Query')
        logger.info('Query destination: %s (%s)', output, self.write_disposition)
        logger.info('Query SQL: %s', query)

        job = {
            'configuration': {
                'query': {
                    'query': query,
                    'priority': self.query_mode,
                    'destinationTable': {
                        'projectId': output.table.project_id,
                        'datasetId': output.table.dataset_id,
                        'tableId': output.table.table_id,
                    },
                    'allowLargeResults': True,
                    'createDisposition': self.create_disposition,
                    'writeDisposition': self.write_disposition,
                    'flattenResults': self.flatten_results,
                    'userDefinedFunctionResources': [{"resourceUri": v} for v in self.udf_resource_uris],
                    'useLegacySql': self.use_legacy_sql,
                }
            }
        }

        bq_client.run_job(output.table.project_id, job, dataset=output.table.dataset)


class BigQueryCreateViewTask(luigi.Task):
    """
    Creates (or updates) a view in BigQuery.

    The output of this task needs to be a BigQueryTarget.
    Instances of this class should specify the view SQL in the view property.

    If a view already exist in BigQuery at output(), it will be updated.
    """

    @property
    def view(self):
        """The SQL query for the view, in text form."""
        raise NotImplementedError()

    def complete(self):
        output = self.output()
        assert isinstance(output, BigQueryTarget), 'Output must be a BigQueryTarget, not %s' % (output)

        if not output.exists():
            return False

        existing_view = output.client.get_view(output.table)
        return existing_view == self.view

    def run(self):
        output = self.output()
        assert isinstance(output, BigQueryTarget), 'Output must be a BigQueryTarget, not %s' % (output)

        view = self.view
        assert view, 'No view was provided'

        logger.info('Create view')
        logger.info('Destination: %s', output)
        logger.info('View SQL: %s', view)

        output.client.update_view(output.table, view)


class ExternalBigQueryTask(MixinBigQueryBulkComplete, luigi.ExternalTask):
    """
    An external task for a BigQuery target.
    """
    pass


class BigQueryExtractTask(luigi.Task):
    """
    Extracts (unloads) a table from BigQuery to GCS.

    This tasks requires the input to be exactly one BigQueryTarget while the
    output should be one or more GCSTargets from luigi.contrib.gcs depending on
    the use of destinationUris property.
    """
    @property
    def destination_uris(self):
        """
        The fully-qualified URIs that point to your data in Google Cloud
        Storage. Each URI can contain one '*' wildcard character and it must
        come after the 'bucket' name.

        Wildcarded destinationUris in GCSQueryTarget might not be resolved
        correctly and result in incomplete data. If a GCSQueryTarget is used to
        pass wildcarded destinationUris be sure to overwrite this property to
        suppress the warning.
        """
        return [x.path for x in luigi.task.flatten(self.output())]

    @property
    def print_header(self):
        """Whether to print the header or not."""
        return PrintHeader.TRUE

    @property
    def field_delimiter(self):
        """
        The separator for fields in a CSV file. The separator can be any
        ISO-8859-1 single-byte character.
        """
        return FieldDelimiter.COMMA

    @property
    def destination_format(self):
        """
        The destination format to use (see :py:class:`DestinationFormat`).
        """
        return DestinationFormat.CSV

    @property
    def compression(self):
        """Whether to use compression."""
        return Compression.NONE

    def run(self):
        input = luigi.task.flatten(self.input())[0]
        assert (
            isinstance(input, BigQueryTarget) or
            (len(input) == 1 and isinstance(input[0], BigQueryTarget))), \
            'Input must be exactly one BigQueryTarget, not %s' % (input)
        bq_client = input.client

        destination_uris = self.destination_uris
        assert all(x.startswith('gs://') for x in destination_uris)

        logger.info('Launching Extract Job')
        logger.info('Extract source: %s', input)
        logger.info('Extract destination: %s', destination_uris)

        job = {
            'configuration': {
                'extract': {
                    'sourceTable': {
                        'projectId': input.table.project_id,
                        'datasetId': input.table.dataset_id,
                        'tableId': input.table.table_id
                    },
                    'destinationUris': destination_uris,
                    'destinationFormat': self.destination_format,
                    'compression': self.compression
                }
            }
        }

        if self.destination_format == 'CSV':
            # "Only exports to CSV may specify a field delimiter."
            job['configuration']['extract']['printHeader'] = self.print_header
            job['configuration']['extract']['fieldDelimiter'] = \
                self.field_delimiter

        bq_client.run_job(
            input.table.project_id,
            job,
            dataset=input.table.dataset)


# the original inconsistently capitalized aliases, for backwards compatibility
BigqueryClient = BigQueryClient
BigqueryTarget = BigQueryTarget
MixinBigqueryBulkComplete = MixinBigQueryBulkComplete
BigqueryLoadTask = BigQueryLoadTask
BigqueryRunQueryTask = BigQueryRunQueryTask
BigqueryCreateViewTask = BigQueryCreateViewTask
ExternalBigqueryTask = ExternalBigQueryTask


class BigQueryExecutionError(Exception):
    def __init__(self, job_id, error_message) -> None:
        """
        :param job_id: BigQuery Job ID
        :type job_id: str
        :param error_message: status['status']['errorResult'] for the failed job
        :type error_message: str
        """
        super().__init__('BigQuery job {} failed: {}'.format(job_id, error_message))
        self.error_message = error_message
        self.job_id = job_id
