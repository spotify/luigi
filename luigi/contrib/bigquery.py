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
import random
from luigi.contrib import gcp
from luigi.contrib.gcs import GCSClient

#  chunking parameters
MAX_POPULATION_SIZE = 10  # sample pupulation
GB_TO_BYTES = 1000000000
MAX_SOURCE_URIS = 10000  # limit by Google

logger = logging.getLogger('luigi-interface')

try:
    from googleapiclient import discovery
    from googleapiclient import http
except ImportError:
    logger.warning('BigQuery module imported, but google-api-python-client is '
                   'not installed. Any BigQuery task will fail')


class CreateDisposition(object):
    CREATE_IF_NEEDED = 'CREATE_IF_NEEDED'
    CREATE_NEVER = 'CREATE_NEVER'


class WriteDisposition(object):
    WRITE_TRUNCATE = 'WRITE_TRUNCATE'
    WRITE_APPEND = 'WRITE_APPEND'
    WRITE_EMPTY = 'WRITE_EMPTY'


class QueryMode(object):
    INTERACTIVE = 'INTERACTIVE'
    BATCH = 'BATCH'


class SourceFormat(object):
    AVRO = 'AVRO'
    CSV = 'CSV'
    DATASTORE_BACKUP = 'DATASTORE_BACKUP'
    NEWLINE_DELIMITED_JSON = 'NEWLINE_DELIMITED_JSON'


class FieldDelimiter(object):
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


class Encoding(object):
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


class BigQueryClient(object):
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
            self.client = discovery.build('bigquery', 'v2', **authenticate_kwargs)

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

    def make_dataset(self, dataset, raise_if_exists=False, body={}):
        """Creates a new dataset with the default permissions.

           :param dataset:
           :type dataset: BQDataset
           :param raise_if_exists: whether to raise an exception if the dataset already exists.
           :raises luigi.target.FileAlreadyExists: if raise_if_exists=True and the dataset exists
        """

        try:
            body['id'] = '{}:{}'.format(dataset.project_id, dataset.dataset_id)
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
                    raise Exception('BigQuery job failed: {}'.format(status['status']['errorResult']))
                return

            logger.info('Waiting for job %s:%s to complete...', project_id, job_id)
            time.sleep(5)

    def submit_job(self, project_id, body, dataset=None):
        """Submits a BigQuery "job". Returns job id. See the documentation for the format of body.

           :param dataset:
           :type dataset: BQDataset
        """

        if dataset and not self.dataset_exists(dataset):
            self.make_dataset(dataset)

        new_job = self.client.jobs().insert(projectId=project_id, body=body).execute()
        return new_job['jobReference']['jobId']

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
            "projectId": dest_table.project_id,
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
    def __init__(self, project_id, dataset_id, table_id, client=None, location=None, enable_chunking=False, chunk_size_gb=1000):
        self.table = BQTable(project_id=project_id, dataset_id=dataset_id, table_id=table_id, location=location)
        self.client = client or BigQueryClient()
        self.enable_chunking = enable_chunking
        self.chunk_size_gb = chunk_size_gb

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


class MixinBigQueryBulkComplete(object):
    """
    Allows to efficiently check if a range of BigQueryTargets are complete.
    This enables scheduling tasks with luigi range tools.

    If you implement a custom Luigi task with a BigQueryTarget output, make sure to also inherit
    from this mixin to enable range support.
    """

    @classmethod
    def bulk_complete(cls, parameter_tuples):
        if len(parameter_tuples) < 1:
            return

        # Instantiate the tasks to inspect them
        tasks_with_params = [(cls(p), p) for p in parameter_tuples]

        # Grab the set of BigQuery datasets we are interested in
        datasets = set([t.output().table.dataset for t, p in tasks_with_params])
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

        If the value is falsy, it is omitted and inferred by BigQuery, which only works for AVRO and CSV inputs."""
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

        print "Enable chunking :" + str(output.enable_chunking)
        print "Chunk Size: " + str(output.chunk_size_gb)
        project_id = output.table.project_id

        bq_client = output.client
        gcs_client = GCSClient()

        source_uris = self.source_uris()
        assert all(x.startswith('gs://') for x in source_uris)

        partial_table_ids = []
        load_start_time = time.time()

        def submit_load_job(uris, table_id):
            job = {
                'projectId': output.table.project_id,
                'configuration': {
                    'load': {
                        'destinationTable': {
                            'projectId': output.table.project_id,
                            'datasetId': output.table.dataset_id,
                            'tableId': table_id,
                        },
                        'encoding': self.encoding,
                        'sourceFormat': self.source_format,
                        'writeDisposition': self.write_disposition,
                        'sourceUris': uris,
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

            print "Job :" + str(job)

            bq_client.run_job(output.table.project_id, job, dataset=output.table.dataset)
            if output.enable_chunking:
                partial_table_ids.append(output.table.project_id + "." + output.table.dataset_id + "." + table_id)

        if not output.enable_chunking:
            print "NO CHUNK"
            # Default to previous behaviour
            submit_load_job(source_uris, output.table.table_id)
        else:
            print "CHUNK"
            source_counter = 0
            for source_uri in source_uris:
                if '*' in source_uri:
                    # we chunk for every source_uri with a wildcard
                    print "Wildcard path for: " + source_uri

                    uris = []
                    for uri in gcs_client.list_wildcard(source_uri):
                        uris.append(uri)
                    num_of_uris = len(uris)

                    chunk_intervals, uris_per_chunk = self.calculate_chunk_intervals(gcs_client, uris, output.chunk_size_gb)

                    print "About to schedule " + str(len(chunk_intervals)) + " chunks, each max " \
                          + str(uris_per_chunk) + " uris"

                    for chunk_num in chunk_intervals:
                        chunk_uris = uris[chunk_num:min(chunk_num+uris_per_chunk, num_of_uris)]
                        # min func to handle the very last chunk from the list, usually shorter then "uris_per_chunk"
                        partial_table_id = "TEMP_" + output.table.table_id + "_" + str(source_counter) + "_" \
                                           + str(chunk_num) + "_" + str(int(load_start_time))

                        submit_load_job(chunk_uris, partial_table_id)
                else:
                    # we don't chunk for non-wildcard source_uri
                    print "Non-wildcard path for: " + source_uri
                    table_id = "TEMP_" + output.table.table_id + "_" + str(source_counter) + "_" + str(int(load_start_time))
                    submit_load_job([source_uri], table_id)

                source_counter += 1

            self.merge_tables(bq_client, output, partial_table_ids, project_id)

            self.remove_tables(bq_client, partial_table_ids)

        load_end_time = time.time()
        print "UPLOAD DONE, it took: " + str(load_end_time - load_start_time) + " seconds"

    @staticmethod
    def calculate_avg_blob_size(gcs_client, uris):
        population_size = min(MAX_POPULATION_SIZE, len(uris))
        sampled_uris = random.sample(uris, population_size)
        total_sampled_blobs_bytes = 0
        for uri in sampled_uris:
            bucket_name = uri.split("/")[2]
            blob_name = "/".join(uri.split("/")[3:])
            blob = gcs_client.get_object(bucket_name, blob_name)
            if blob is not None:
                blob_size_bytes = blob['size']
                total_sampled_blobs_bytes += int(blob_size_bytes)
            else:
                raise Exception("BigQueryLoadTask failed: Object + " + uri + " doesn't exist")
        return total_sampled_blobs_bytes / population_size

    def calculate_chunk_intervals(self, gcs_client, uris, chunk_size_gb):
        avg_blob_size_bytes = self.calculate_avg_blob_size(gcs_client, uris)
        #  todo: what if avg_blob_size_bytes * num_of_uris < MAX_UPLOAD_SIZE_BYTES ??
        #  todo: then we will upload this data in one chunk, but at the end we will still merge this one table
        #  todo: so this step could be skipped
        print "Summary avg blob size is: " + str(avg_blob_size_bytes) + " bytes"
        chunk_size_bytes = chunk_size_gb * GB_TO_BYTES
        uris_per_chunk = min(MAX_SOURCE_URIS, max(1, chunk_size_bytes / avg_blob_size_bytes))
        chunk_intervals = range(0, len(uris), uris_per_chunk)
        return chunk_intervals, uris_per_chunk

    def merge_tables(self, bq_client, output, table_ids, project_id):
        select_queries = map(lambda table_id: "(SELECT * FROM `{}`)".format(table_id), table_ids)
        merge_query = " UNION ALL ".join(select_queries)
        merge_job = {
            'projectId': output.table.project_id,
            'configuration': {
                'query': {
                    'query': merge_query,
                    'useLegacySql': False,
                    'destinationTable': {
                        'projectId': output.table.project_id,
                        'datasetId': output.table.dataset_id,
                        'tableId': output.table.table_id,
                    },
                    'writeDisposition': self.write_disposition
                }
            }
        }
        if self.schema:
            merge_job['configuration']['query']['schema'] = {'fields': self.schema}
        print "Starting merge"
        start_merge = time.time()
        bq_client.run_job(project_id, merge_job, dataset=output.table.dataset)
        end_merge = time.time()
        print "Merge done, it took: " + str(end_merge - start_merge) + " seconds"

    @staticmethod
    def remove_tables(bq_client, table_ids):
        print "starting cleaning up"
        for t in table_ids:
            table_params = t.split(".")
            bq_client.client.tables().delete(projectId=table_params[0], datasetId=table_params[1],
                                             tableId=table_params[2]).execute()
        print "Cleaning up is done, removed " + str(len(table_ids)) + " tables"


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
            'projectId': output.table.project_id,
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


# the original inconsistently capitalized aliases, for backwards compatibility
BigqueryClient = BigQueryClient
BigqueryTarget = BigQueryTarget
MixinBigqueryBulkComplete = MixinBigQueryBulkComplete
BigqueryLoadTask = BigQueryLoadTask
BigqueryRunQueryTask = BigQueryRunQueryTask
BigqueryCreateViewTask = BigQueryCreateViewTask
ExternalBigqueryTask = ExternalBigQueryTask
