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

logger = logging.getLogger('luigi-interface')

try:
    import httplib2
    import oauth2client

    from googleapiclient import discovery
    from googleapiclient import http
except ImportError:
    logger.warning('Bigquery module imported, but google-api-python-client is '
                   'not installed. Any bigquery task will fail')


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
    CSV = 'CSV'
    DATASTORE_BACKUP = 'DATASTORE_BACKUP'
    NEWLINE_DELIMITED_JSON = 'NEWLINE_DELIMITED_JSON'


BQDataset = collections.namedtuple('BQDataset', 'project_id dataset_id')


class BQTable(collections.namedtuple('BQTable', 'project_id dataset_id table_id')):
    @property
    def dataset(self):
        return BQDataset(project_id=self.project_id, dataset_id=self.dataset_id)


class BigqueryClient(object):
    """A client for Google BigQuery.

    For details of how authentication and the descriptor work, see the
    documentation for the GCS client. The descriptor URL for BigQuery is
    https://www.googleapis.com/discovery/v1/apis/bigquery/v2/rest
    """

    def __init__(self, oauth_credentials=None, descriptor='', http_=None):
        http_ = http_ or httplib2.Http()

        if not oauth_credentials:
            oauth_credentials = oauth2client.client.GoogleCredentials.get_application_default()

        if descriptor:
            self.client = discovery.build_from_document(descriptor, credentials=oauth_credentials, http=http_)
        else:
            self.client = discovery.build('bigquery', 'v2', credentials=oauth_credentials, http=http_)

    def dataset_exists(self, dataset):
        """Returns whether the given dataset exists.

           :param dataset:
           :type dataset: BQDataset
        """

        try:
            self.client.datasets().get(projectId=dataset.project_id,
                                       datasetId=dataset.dataset_id).execute()
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
            self.client.datasets().insert(projectId=dataset.project_id, body=dict(
                {'id': '{}:{}'.format(dataset.project_id, dataset.dataset_id)}, **body)).execute()
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

        request = self.client.datasets().list(projectId=project_id)
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
                                            datasetId=dataset.dataset_id)
        response = request.execute()

        while response is not None:
            for t in response.get('tables', []):
                yield t['tableReference']['tableId']

            request = self.client.tables().list_next(request, response)
            if request is None:
                break

            response = request.execute()

    def run_job(self, project_id, body, dataset=None):
        """Runs a bigquery "job". See the documentation for the format of body.

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
            status = self.client.jobs().get(projectId=project_id, jobId=job_id).execute()
            if status['status']['state'] == 'DONE':
                if status['status'].get('errors'):
                    raise Exception('Bigquery job failed: {}'.format(status['status']['errors']))
                return

            logger.info('Waiting for job %s:%s to complete...', project_id, job_id)
            time.sleep(5.0)

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


class BigqueryTarget(luigi.target.Target):
    def __init__(self, project_id, dataset_id, table_id, client=None):
        self.table = BQTable(project_id=project_id, dataset_id=dataset_id, table_id=table_id)
        self.client = client or BigqueryClient()

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


class BigqueryLoadTask(luigi.Task):
    """Load data into bigquery from GCS."""

    @property
    def source_format(self):
        """The source format to use (see :py:class:`SourceFormat`)."""
        return SourceFormat.NEWLINE_DELIMITED_JSON

    @property
    def write_disposition(self):
        """What to do if the table already exists. By default this will fail the job.

           See :py:class:`WriteDisposition`"""
        return WriteDisposition.WRITE_EMPTY

    @property
    def schema(self):
        """Schema in the format defined at https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load.schema.

        If the value is falsy, it is omitted and inferred by bigquery, which only works for CSV inputs."""
        return []

    @property
    def max_bad_records(self):
        return 0

    @property
    def source_uris(self):
        """Source data which should be in GCS."""
        return [x.path for x in luigi.task.flatten(self.input())]

    def run(self):
        output = self.output()
        assert isinstance(output, BigqueryTarget), 'Output should be a bigquery target, not %s' % (output)

        bq_client = output.client

        source_uris = self.source_uris()
        assert all(x.startswith('gs://') for x in source_uris)

        job = {
            'projectId': output.table.project_id,
            'configuration': {
                'load': {
                    'destinationTable': {
                        'projectId': output.table.project_id,
                        'datasetId': output.table.dataset_id,
                        'tableId': output.table.table_id,
                    },
                    'sourceFormat': self.source_format,
                    'writeDisposition': self.write_disposition,
                    'sourceUris': source_uris,
                    'maxBadRecords': self.max_bad_records,
                }
            }
        }
        if self.schema:
            job['configuration']['load']['schema'] = {'fields': self.schema}

        bq_client.run_job(output.table.project_id, job, dataset=output.table.dataset)


class BigqueryRunQueryTask(luigi.Task):

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
    def query(self):
        """The query, in text form."""
        raise NotImplementedError()

    @property
    def query_mode(self):
        """The query mode. See :py:class:`QueryMode`."""
        return QueryMode.INTERACTIVE

    def run(self):
        output = self.output()
        assert isinstance(output, BigqueryTarget), 'Output should be a bigquery target, not %s' % (output)

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
                }
            }
        }

        bq_client.run_job(output.table.project_id, job, dataset=output.table.dataset)
