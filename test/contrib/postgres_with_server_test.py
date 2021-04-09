# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
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
import os

from helpers import unittest
import pytest

import luigi
import luigi.notifications
from luigi.contrib import postgres

"""
Typical use cases that should be tested:

* Daily overwrite of all data in table
* Daily inserts of new segment in table
* (Daily insertion/creation of new table)
* Daily insertion of multiple (different) new segments into table


"""

host = 'localhost'
database = 'spotify'
user = os.getenv('POSTGRES_USER', 'spotify')
password = 'guest'


try:
    import psycopg2
    conn = psycopg2.connect(
        user=user,
        host=host,
        database=database,
        password=password,
    )
    conn.close()
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)
except Exception:
    raise unittest.SkipTest('Unable to connect to postgres')


# to avoid copying:

class CopyToTestDB(postgres.CopyToTable):
    host = host
    database = database
    user = user
    password = password


class TestPostgresTask(CopyToTestDB):
    table = 'test_table'
    columns = (('test_text', 'text'),
               ('test_int', 'int'),
               ('test_float', 'float'))

    def create_table(self, connection):
        connection.cursor().execute(
            "CREATE TABLE {table} (id SERIAL PRIMARY KEY, test_text TEXT, test_int INT, test_float FLOAT)"
            .format(table=self.table))

    def rows(self):
        yield 'foo', 123, 123.45
        yield None, '-100', '5143.213'
        yield '\t\n\r\\N', 0, 0
        yield u'éцү我', 0, 0
        yield '', 0, r'\N'  # Test working default null charcter


class MetricBase(CopyToTestDB):
    table = 'metrics'
    columns = [('metric', 'text'),
               ('value', 'int')
               ]


class Metric1(MetricBase):
    param = luigi.Parameter()

    def rows(self):
        yield 'metric1', 1
        yield 'metric1', 2
        yield 'metric1', 3


class Metric2(MetricBase):
    param = luigi.Parameter()

    def rows(self):
        yield 'metric2', 1
        yield 'metric2', 4
        yield 'metric2', 3


@pytest.mark.postgres
class TestPostgresImportTask(unittest.TestCase):

    def test_default_escape(self):
        self.assertEqual(postgres.default_escape('foo'), 'foo')
        self.assertEqual(postgres.default_escape('\n'), '\\n')
        self.assertEqual(postgres.default_escape('\\\n'), '\\\\\\n')
        self.assertEqual(postgres.default_escape('\n\r\\\t\\N\\'),
                         '\\n\\r\\\\\\t\\\\N\\\\')

    def test_repeat(self):
        task = TestPostgresTask()
        conn = task.output().connect()
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute('DROP TABLE IF EXISTS {table}'.format(table=task.table))
        cursor.execute('DROP TABLE IF EXISTS {marker_table}'.format(marker_table=postgres.PostgresTarget.marker_table))

        luigi.build([task], local_scheduler=True)
        luigi.build([task], local_scheduler=True)  # try to schedule twice

        cursor.execute("""SELECT test_text, test_int, test_float
                          FROM test_table
                          ORDER BY id ASC""")

        rows = tuple(cursor)

        self.assertEqual(rows, (
            ('foo', 123, 123.45),
            (None, -100, 5143.213),
            ('\t\n\r\\N', 0.0, 0),
            (u'éцү我', 0, 0),
            (u'', 0, None),  # Test working default null charcter
        ))

    def test_multimetric(self):
        metrics = MetricBase()
        conn = metrics.output().connect()
        conn.autocommit = True
        conn.cursor().execute('DROP TABLE IF EXISTS {table}'.format(table=metrics.table))
        conn.cursor().execute('DROP TABLE IF EXISTS {marker_table}'.format(marker_table=postgres.PostgresTarget.marker_table))
        luigi.build([Metric1(20), Metric1(21), Metric2("foo")], local_scheduler=True)

        cursor = conn.cursor()
        cursor.execute('select count(*) from {table}'.format(table=metrics.table))
        self.assertEqual(tuple(cursor), ((9,),))

    def test_clear(self):
        class Metric2Copy(Metric2):

            def init_copy(self, connection):
                query = "TRUNCATE {0}".format(self.table)
                connection.cursor().execute(query)

        clearer = Metric2Copy(21)
        conn = clearer.output().connect()
        conn.autocommit = True
        conn.cursor().execute('DROP TABLE IF EXISTS {table}'.format(table=clearer.table))
        conn.cursor().execute('DROP TABLE IF EXISTS {marker_table}'.format(marker_table=postgres.PostgresTarget.marker_table))

        luigi.build([Metric1(0), Metric1(1)], local_scheduler=True)
        luigi.build([clearer], local_scheduler=True)
        cursor = conn.cursor()
        cursor.execute('select count(*) from {table}'.format(table=clearer.table))
        self.assertEqual(tuple(cursor), ((3,),))
