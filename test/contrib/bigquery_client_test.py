# -*- coding: utf-8 -*-
#
# Copyright 2023 Spotify AB
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
These are the unit tests for the BigQueryClient class.
"""

import unittest

from mock.mock import MagicMock

from luigi.contrib import bigquery
try:
    from googleapiclient import errors
except ImportError:
    raise unittest.SkipTest('Unable to load googleapiclient module')


class BigQueryClientTest(unittest.TestCase):

    def test_retry_succeeds_on_second_attempt(self):
        client = MagicMock(spec=bigquery.BigQueryClient)
        attempts = 0

        @bigquery.bq_retry
        def fail_once(bq_client):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise errors.HttpError(
                    resp=MagicMock(status=500),
                    content=b'{"error": {"message": "stub"}',
                )
            else:
                return MagicMock(status=200)

        response = fail_once(client)
        client._initialise_client.assert_called_once()
        self.assertEqual(attempts, 2)
        self.assertEqual(response.status, 200)
