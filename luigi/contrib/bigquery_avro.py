"""Specialized tasks for handling Avro data in BigQuery from GCS.
"""
import logging

from luigi.contrib.bigquery import BigQueryLoadTask, SourceFormat
from luigi.contrib.gcs import GCSClient
from luigi.task import flatten

logger = logging.getLogger('luigi-interface')

try:
    import avro
    import avro.datafile
except ImportError:
    logger.warning('bigquery_avro module imported, but avro is not installed. Any '
                   'BigQueryLoadAvro task will fail to propagate schema documentation')


class BigQueryLoadAvro(BigQueryLoadTask):
    """A helper for loading specifically Avro data into BigQuery from GCS.

    Additional goodies - takes field documentation from the input data and propagates it
    to BigQuery table description and field descriptions.  Supports the following Avro schema
    types: Primitives, Enums, Records, Arrays, Unions, and Maps.  For Map schemas nested maps
    and unions are not supported.  For Union Schemas only nested Primitive and Record Schemas
    are currently supported.

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
        assert avro, 'avro module required'

        input_target = flatten(self.input())[0]
        input_fs = input_target.fs if hasattr(input_target, 'fs') else GCSClient()
        input_uri = self.source_uris()[0]
        if '*' in input_uri:
            file_uris = list(input_fs.list_wildcard(input_uri))
            if file_uris:
                input_uri = file_uris[0]
            else:
                raise RuntimeError('No match for ' + input_uri)

        schema = []
        exception_reading_schema = []

        def read_schema(fp):
            # fp contains the file part downloaded thus far. We rely on that the DataFileReader
            # initializes itself fine as soon as the file header with schema is downloaded, without
            # requiring the remainder of the file...
            try:
                reader = avro.datafile.DataFileReader(fp, avro.io.DatumReader())
                schema[:] = [reader.datum_reader.writers_schema]
            except Exception as e:
                # Save but assume benign unless schema reading ultimately fails. The benign
                # exception in case of insufficiently big downloaded file part seems to be:
                # TypeError('ord() expected a character, but string of length 0 found',).
                exception_reading_schema[:] = [e]
                return False
            return True

        input_fs.download(input_uri, 64 * 1024, read_schema).close()
        if not schema:
            raise exception_reading_schema[0]
        return schema[0]

    def _set_output_doc(self, avro_schema):
        bq_client = self.output().client.client
        table = self.output().table

        current_bq_schema = bq_client.tables().get(projectId=table.project_id,
                                                   datasetId=table.dataset_id,
                                                   tableId=table.table_id).execute()

        def get_fields_with_description(bq_fields, avro_fields):
            new_fields = []
            for field in bq_fields:
                avro_field = avro_fields[field[u'name']]
                field_type = type(avro_field.type)

                # Primitive Support
                if field_type is avro.schema.PrimitiveSchema:
                    field[u'description'] = avro_field.doc

                # Enum Support
                if field_type is avro.schema.EnumSchema:
                    field[u'description'] = avro_field.type.doc

                # Record Support
                if field_type is avro.schema.RecordSchema:
                    field[u'description'] = avro_field.type.doc
                    field[u'fields'] = get_fields_with_description(field[u'fields'], avro_field.type.fields_dict)

                # Array Support
                if field_type is avro.schema.ArraySchema:
                    field[u'description'] = avro_field.type.items.doc
                    field[u'fields'] = get_fields_with_description(field[u'fields'], avro_field.type.items.fields_dict)

                # Union Support
                if type(avro_field.type) is avro.schema.UnionSchema:
                    for schema in avro_field.type.schemas:
                        if type(schema) is avro.schema.PrimitiveSchema:
                            field[u'description'] = avro_field.doc

                        if type(schema) is avro.schema.RecordSchema:
                            field[u'description'] = schema.doc
                            field[u'fields'] = get_fields_with_description(field[u'fields'], schema.fields_dict)

                            # Support for Enums, Arrays, Maps, and Unions inside of a union is not yet implemented

                # Map Support
                if field_type is avro.schema.MapSchema:
                    field[u'description'] = avro_field.doc

                    # Big Query Avro loader creates artificial key and value attributes in the Big Query schema
                    # ignoring the key and operating directly on the value
                    # https://cloud.google.com/bigquery/data-formats#avro_format
                    bq_map_value_field = field[u'fields'][-1]
                    avro_map_value = avro_field.type.values
                    value_field_type = type(avro_map_value)

                    # Primitive Support: Unfortunately the map element doesn't directly have a doc attribute
                    # so there is no way to get documentation on the primitive types for the value attribute

                    if value_field_type is avro.schema.EnumSchema:
                        bq_map_value_field[u'description'] = avro_map_value.type.doc

                    if value_field_type is avro.schema.RecordSchema:
                        # Set values description using type's doc
                        bq_map_value_field[u'description'] = avro_map_value.doc

                        # This is jumping into the map value directly and working with that
                        bq_map_value_field[u'fields'] = get_fields_with_description(bq_map_value_field[u'fields'], avro_map_value.fields_dict)

                    if value_field_type is avro.schema.ArraySchema:
                        bq_map_value_field[u'description'] = avro_map_value.items.doc
                        bq_map_value_field[u'fields'] = get_fields_with_description(bq_map_value_field[u'fields'], avro_map_value.items.fields_dict)

                        # Support for unions and maps nested inside of a map is not yet implemented

                new_fields.append(field)
            return new_fields

        field_descriptions = get_fields_with_description(current_bq_schema['schema']['fields'], avro_schema.fields_dict)

        patch = {
            'description': avro_schema.doc,
            'schema': {'fields': field_descriptions, },
        }

        bq_client.tables().patch(projectId=table.project_id,
                                 datasetId=table.dataset_id,
                                 tableId=table.table_id,
                                 body=patch).execute()

    def run(self):
        super(BigQueryLoadAvro, self).run()

        # We propagate documentation in one fire-and-forget attempt; the output table is
        # left to exist without documentation if this step raises an exception.
        try:
            self._set_output_doc(self._get_input_schema())
        except Exception as e:
            logger.warning('Could not propagate Avro doc to BigQuery table field descriptions: %r', e)
