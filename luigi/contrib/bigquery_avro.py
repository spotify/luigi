"""Specialized tasks for handling Avro data in BigQuery from GCS.
"""
from itertools import iter, next
import logging

from luigi.contrib.bigquery import BigQueryLoadTask, SourceFormat
from luigi.contrib.gcs import GCSClient
from luigi.task import flatten

try:
    import avro
except ImportError:
    raise Exception('bigquery_avro module imported, but avro is not installed.')

logger = logging.getLogger(__name__)


class BigQueryLoadAvro(BigQueryLoadTask):
    """A helper for loading specifically Avro data into BigQuery from GCS.

    Additional goodies - takes field documentation from the input data and propagates it
    to BigQuery table description and field descriptions.

    Suitable for use via subclassing: override requires() to return Task(s) that output
    to GCS Targets; their paths are expected to be URIs of .avro files or URI prefixes
    (GCS "directories") containing one or many .avro files.

    Override output() to return a BigQueryTarget representing the destination table.
    """
    source_format = SourceFormat.AVRO

    def _avro_uri(self, target):
        path_or_uri = target.uri if hasattr(target, 'uri') else target.path
        return path_or_uri if path_or_uri.endswith('.avro') else path_or_uri.rstrip('/') + '/*.avro'

    def source_uris(self):
        return [self._avro_uri(x) for x in flatten(self.input())]

    def _get_input_schema(self):
        '''Arbitrarily picks an object in input and reads the Avro schema from it.'''
        input_target = next(iter(flatten(self.input())))
        input_fs = input_target.fs if hasattr(input_target, 'fs') else GCSClient()
        input_uri = next(iter(self.source_uris()))
        if '*' in input_uri:
            input_uri = next(iter(input_fs.list_wildcard(input_uri)))

        schema = []

        def read_schema(fp):
            # We rely on that the DataFileReader will initialize itself fine as soon as the file
            # header with schema is downloaded, without requiring the remainder of the file...
            try:
                reader = avro.datafile.DataFileReader(fp, avro.io.DatumReader())
                schema.append(reader.datum_reader.writers_schema)
            except Exception as e:
                logger.info('%s', e)
                return False
            return True

        input_fs.download(input_uri, 64 * 1024, read_schema).close()  # TODO check with various chunksizes (suspect the file position might matter)

        return schema[0]

    def _set_output_doc(self, avro_schema):
        table = self.output().table
        current = self.client.tables().get(projectId=table.project_id,
                                           datasetId=table.dataset_id,
                                           tableId=table.table_id).execute()
        patch = {
            'description': avro_schema['doc'],
            'schema': current['schema'],
        }
        # TODO update patch['schema']

        self.client.tables().patch(projectId=table.project_id,
                                   datasetId=table.dataset_id,
                                   tableId=table.table_id,
                                   body=patch).execute()

    def run(self):
        super(BigQueryLoadAvro, self).run()

        # We propagate documentation in one fire-and-forget attempt; the output table will
        # be left to exist but without documentation if this step raises an exception.
        self._set_output_doc(self._get_input_schema())
