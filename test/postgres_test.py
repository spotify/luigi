# Copyright (c) 2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import datetime
import luigi
import luigi.postgres
from luigi.tools.range import RangeDaily
from helpers import unittest
import mock


def datetime_to_epoch(dt):
    td = dt - datetime.datetime(1970, 1, 1)
    return td.days * 86400 + td.seconds + td.microseconds / 1E6


class MockPostgresCursor(mock.Mock):
    """
    Keeps state to simulate executing SELECT queries and fetching results.
    """
    def __init__(self, existing_update_ids):
        super(MockPostgresCursor, self).__init__()
        self.existing = existing_update_ids

    def execute(self, query, params):
        if query.startswith('SELECT 1 FROM table_updates'):
            self.fetchone_result = (1, ) if params[0] in self.existing else None
        else:
            self.fetchone_result = None

    def fetchone(self):
        return self.fetchone_result


class DummyPostgresImporter(luigi.postgres.CopyToTable):
    date = luigi.DateParameter()

    host = 'dummy_host'
    database = 'dummy_database'
    user = 'dummy_user'
    password = 'dummy_password'
    table = 'dummy_table'
    columns = (
        ('some_text', 'text'),
        ('some_int', 'int'),
    )


class DailyCopyToTableTest(unittest.TestCase):
    maxDiff = None

    @mock.patch('psycopg2.connect')
    def test_bulk_complete(self, mock_connect):
        mock_cursor = MockPostgresCursor([
            'DummyPostgresImporter(date=2015-01-03)'
        ])
        mock_connect.return_value.cursor.return_value = mock_cursor

        task = RangeDaily(of='DummyPostgresImporter',
                          start=datetime.date(2015, 1, 2),
                          now=datetime_to_epoch(datetime.datetime(2015, 1, 7)))
        actual = [t.task_id for t in task.requires()]

        self.assertEqual(actual, [
            'DummyPostgresImporter(date=2015-01-02)',
            'DummyPostgresImporter(date=2015-01-04)',
            'DummyPostgresImporter(date=2015-01-05)',
            'DummyPostgresImporter(date=2015-01-06)',
        ])
        self.assertFalse(task.complete())
