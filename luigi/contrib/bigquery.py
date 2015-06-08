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

    def exists(self, project_id, dataset_id, table_id=None):
        """Returns whether the given project/dataset/table exists.

           ``table_id`` may be omitted if you want to check the existence of a dataset.
        """

        try:
            self.client.datasets().get(projectId=project_id, datasetId=dataset_id).execute()
        except http.HttpError as ex:
            if ex.resp.status == 404:
                return False
            raise

        if table_id is not None:
            try:
                self.client.tables().get(projectId=project_id, datasetId=dataset_id,
                                         tableId=table_id).execute()
            except http.HttpError as ex:
                if ex.resp.status == 404:
                    return False
                raise

        return True

    def make_dataset(self, project_id, dataset_id, raise_if_exists=False, body={}):
        """Creates a new dataset with the default permissions.

           :param raise_if_exists whether to raise an exception if the dataset already exists.
           :raises luigi.target.FileAlreadyExists
        """

        try:
            self.client.datasets().insert(projectId=project_id, body=dict(
                {'id': '{}:{}'.format(project_id, dataset_id)}, **body)).execute()
        except http.HttpError as ex:
            if ex.resp.status == 409:
                if raise_if_exists:
                    raise luigi.target.FileAlreadyExists()
            else:
                raise

    def delete_dataset(self, project_id, dataset_id, delete_nonempty=True):
        """Deletes a dataset (and optionally any tables in it), if it exists.

           :param delete_nonempty if true, will delete any tables before deleting the dataset
        """

        if not self.exists(project_id, dataset_id):
            return

        self.client.datasets().delete(projectId=project_id, datasetId=dataset_id,
                                      deleteContents=delete_nonempty).execute()

    def delete_table(self, project_id, dataset_id, table_id):
        """Deletes a table, if it exists."""

        if not self.exists(project_id, dataset_id, table_id):
            return

        self.client.tables().delete(projectId=project_id, datasetId=dataset_id,
                                    tableId=table_id).execute()

    def list_datasets(self, project_id):
        """Returns the list of datasets in a given project."""

        request = self.client.datasets().list(projectId=project_id)
        response = request.execute()

        while response is not None:
            for ds in response.get('datasets', []):
                yield ds['datasetReference']['datasetId']

            request = self.client.datasets().list_next(request, response)
            if request is None:
                break

            response = request.execute()

    def list_tables(self, project_id, dataset_id):
        """Returns the list of tables in a given dataset."""

        request = self.client.tables().list(projectId=project_id, datasetId=dataset_id)
        response = request.execute()

        while response is not None:
            for t in response.get('tables', []):
                yield t['tableReference']['tableId']

            request = self.client.tables().list_next(request, response)
            if request is None:
                break

            response = request.execute()

    def run_job(self, project_id, body, dataset_id=None):
        """Runs a bigquery "job". See the documentation for the format of body.

           :note You probably don't need to use this directly.
        """

        if not self.exists(project_id, dataset_id):
            self.make_dataset(project_id, dataset_id)

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
             source_project_id,
             source_dataset_id,
             source_table_id,
             dest_project_id,
             dest_dataset_id,
             dest_table_id,
             create_disposition=CreateDisposition.CREATE_IF_NEEDED,
             write_disposition=WriteDisposition.WRITE_TRUNCATE):
        """Copies (or appends) a table to another table.

           :param create_disposition whether to create the table if needed
           :param write_disposition whether to append/truncate/fail if the table exists"""

        job = {
            "projectId": dest_project_id,
            "configuration": {
                "copy": {
                    "sourceTable": {
                        "projectId": source_project_id,
                        "datasetId": source_dataset_id,
                        "tableId": source_table_id,
                    },
                    "destinationTable": {
                        "projectId": dest_project_id,
                        "datasetId": dest_dataset_id,
                        "tableId": dest_table_id,
                    },
                    "createDisposition": create_disposition,
                    "writeDisposition": write_disposition,
                }
            }
        }

        self.run_job(dest_project_id, job, dataset_id=dest_dataset_id)


class BigqueryTarget(luigi.target.Target):
    def __init__(self, project_id, dataset_id, table_id, client=None):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.client = client or BigqueryClient()

    def exists(self):
        return self.client.exists(self.project_id, self.dataset_id, self.table_id)

    def __str__(self):
        return 'bq://' + self.project_id + '/' + self.dataset_id + '/' + self.table_id


class BigqueryLoadTask(luigi.Task):
    """Load data into bigquery from GCS."""

    @property
    def source_format(self):
        """The source format to use (see :py:class:SourceFormat)."""
        return SourceFormat.NEWLINE_DELIMITED_JSON

    @property
    def write_disposition(self):
        """What to do if the table already exists. By default this will fail the job.

           See :py:class:WriteDisposition"""
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
            'projectId': output.project_id,
            'configuration': {
                'load': {
                    'destinationTable': {
                        'projectId': output.project_id,
                        'datasetId': output.dataset_id,
                        'tableId': output.table_id,
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

        bq_client.run_job(output.project_id, job, dataset_id=output.dataset_id)


class BigqueryRunQueryTask(luigi.Task):

    @property
    def write_disposition(self):
        """What to do if the table already exists. By default this will fail the job.

           See :py:class:WriteDisposition"""
        return WriteDisposition.WRITE_TRUNCATE

    @property
    def create_disposition(self):
        """Whether to create the table or not. See :py:class:CreateDisposition"""
        return CreateDisposition.CREATE_IF_NEEDED

    @property
    def query(self):
        """The query, in text form."""
        raise NotImplementedError()

    @property
    def query_mode(self):
        """The query mode. See :py:class:QueryMode."""
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
            'projectId': output.project_id,
            'configuration': {
                'query': {
                    'query': query,
                    'priority': self.query_mode,
                    'destinationTable': {
                        'projectId': output.project_id,
                        'datasetId': output.dataset_id,
                        'tableId': output.table_id,
                    },
                    'allowLargeResults': True,
                    'createDisposition': self.create_disposition,
                    'writeDisposition': self.write_disposition,
                }
            }
        }

        bq_client.run_job(output.project_id, job, dataset_id=output.dataset_id)
