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
import re
from nose.plugins.attrib import attr


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

    def execute(self, query, params=""):
        query = re.sub(r"[\t \n]+", " ", query).strip()
        if query.startswith('SELECT 1 FROM table_updates'):
            self.fetchone_result = (1, ) if params[0] in self.existing else None
        elif query.startswith(
            "SELECT update_id FROM table_updates WHERE target_table = 'dummy_table'"
        ):
            self.fetchall_result = [
                ("DummyPostgresImporter(date=2015-01-01)", ),
                ("Non_Requested_UpdateId", )
            ]
        else:
            self.fetchone_result = None

    def fetchone(self):
        return self.fetchone_result

    def fetchall(self):
        return self.fetchall_result

    def close(self):
        pass


class DummyPostgresImporter(
        luigi.postgres.MixinPostgresBulkComplete,
        luigi.postgres.CopyToTable
    ):
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


@attr('postgres')
class DailyCopyToTableTest(unittest.TestCase):
    maxDiff = None

    @mock.patch('psycopg2.connect')
    def test_bulk_complete(self, mock_connect):
        mock_cursor = MockPostgresCursor([
            DummyPostgresImporter(date=datetime.datetime(2015, 1, 3)).task_id
        ])
        mock_connect.return_value.cursor.return_value = mock_cursor

        task = RangeDaily(of=DummyPostgresImporter,
                          start=datetime.date(2015, 1, 2),
                          now=datetime_to_epoch(datetime.datetime(2015, 1, 7)))
        actual = sorted([t.task_id for t in task.requires()])

        self.assertEqual(actual, sorted([
            DummyPostgresImporter(date=datetime.datetime(2015, 1, 2)).task_id,
            DummyPostgresImporter(date=datetime.datetime(2015, 1, 4)).task_id,
            DummyPostgresImporter(date=datetime.datetime(2015, 1, 5)).task_id,
            DummyPostgresImporter(date=datetime.datetime(2015, 1, 6)).task_id,
        ]))
        self.assertFalse(task.complete())


@attr('postgres')
class BulkCompleteTest(unittest.TestCase):

    @mock.patch('psycopg2.connect')
    def test_bulk_complete(self, mock_connect):
        task = DummyPostgresImporter(date=datetime.date(2015, 1, 1))
        mock_cursor = MockPostgresCursor([task.task_id])
        mock_connect.return_value.cursor.return_value = mock_cursor
        c = DummyPostgresImporter.bulk_complete(
            (
                (datetime.date(2015, 1, 1,), ),
                (datetime.date(2015, 1, 2,), )
            )
        )
        self.assertIn(task.update_id(), c)
