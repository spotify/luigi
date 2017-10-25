"""Specialized tasks for handling Avro data in BigQuery from GCS.
"""
import logging

from luigi.contrib.bigquery import BigQueryLoadTask, SourceFormat
from luigi.task import flatten

logger = logging.getLogger('luigi-interface')


class BigQueryLoadAvro(BigQueryLoadTask):
    """A helper for loading specifically Avro data into BigQuery from GCS.

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

    def run(self):
        super(BigQueryLoadAvro, self).run()
