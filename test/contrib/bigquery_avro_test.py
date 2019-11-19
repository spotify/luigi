# -*- coding: utf-8 -*-
#
# Copyright 2019 Spotify AB
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

"""
These are the unit tests for the BigQueryLoadAvro class.
"""

import unittest
import avro
from luigi.contrib.bigquery_avro import BigQueryLoadAvro
from nose.plugins.attrib import attr


@attr('gcloud')
class BigQueryAvroTest(unittest.TestCase):

    def test_writer_schema_method_existence(self):
        avro_schema = """
        {
            "namespace": "example.avro",
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "favorite_number",  "type": ["int", "null"]},
                {"name": "favorite_color", "type": ["string", "null"]}
            ]
        }
        """
        reader = avro.io.DatumReader(avro_schema, avro_schema)
        actual_schema = BigQueryLoadAvro._get_writer_schema(reader)
        self.assertEqual(actual_schema, avro_schema, 
                         "writer(s) avro_schema attribute not found")
        # otherwise AttributeError is thrown
