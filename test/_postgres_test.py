from unittest import TestCase
import luigi
from luigi import postgres
import luigi.notifications
luigi.notifications.DEBUG = True
luigi.namespace('postgres_test')

"""
Typical use cases that should be tested:

* Daily overwrite of all data in table
* Daily inserts of new segment in table
* (Daily insertion/creation of new table)
* Daily insertion of multiple (different) new segments into table


"""

# to avoid copying:


class CopyToTestDB(postgres.CopyToTable):
    host = 'localhost'
    database = 'luigi'
    user = 'luigi'
    password = 'foo'


class TestPostgresTask(CopyToTestDB):
    table = 'test_table'
    columns = (('test_text', 'text'),
               ('test_int', 'int'),
               ('test_float', 'float'))

    def create_table(self, connection):
        connection.cursor().execute(
            """CREATE TABLE {table}
            (id SERIAL PRIMARY KEY,
             test_text TEXT,
             test_int INT,
             test_float FLOAT)"""
            .format(table=self.table))

    def rows(self):
        yield 'foo', 123, 123.45
        yield None, '-100', '5143.213'
        yield '\t\n\r\\N', 0, 0


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


class TestPostgresImportTask(TestCase):
    def test_default_escape(self):
        self.assertEquals(postgres.default_escape('foo'), 'foo')
        self.assertEquals(postgres.default_escape('\n'), '\\n')
        self.assertEquals(postgres.default_escape('\\\n'), '\\\\\\n')
        self.assertEquals(postgres.default_escape('\n\r\\\t\\N\\'),
                          '\\n\\r\\\\\\t\\\\N\\\\')

    def clear(self, connection, table):
        connection.cursor().execute(
            "DROP TABLE IF EXISTS {table}".format(table=table))
        connection.cursor().execute(
            "DROP TABLE IF EXISTS {marker_table}".format(
                marker_table=postgres.PostgresTarget.marker_table))

    def test_repeat(self):
        task = TestPostgresTask()
        conn = task.output().connect()
        conn.autocommit = True
        self.clear(conn, task.table)

        luigi.build([task], local_scheduler=True)
        luigi.build([task], local_scheduler=True)  # try to schedule twice
        cursor = conn.cursor()
        cursor.execute("""SELECT test_text, test_int, test_float
                          FROM test_table
                          ORDER BY id ASC""")

        rows = tuple(cursor)

        self.assertEquals(rows, (
            ('foo', 123, 123.45),
            (None, -100, 5143.213),
            ('\t\n\r\\N', 0.0, 0))
        )

    def test_multimetric(self):
        metrics = MetricBase()
        conn = metrics.output().connect()
        conn.autocommit = True
        self.clear(conn, metrics.table)
        luigi.build(
            [Metric1(20), Metric1(21), Metric2("foo")], local_scheduler=True)

        cursor = conn.cursor()
        cursor.execute(
            'select count(*) from {table}'.format(table=metrics.table))
        self.assertEquals(tuple(cursor), ((9,),))

    def test_clear(self):
        class Metric2Copy(Metric2):

            def init_copy(self, connection):
                query = "TRUNCATE {0}".format(self.table)
                connection.cursor().execute(query)

        clearer = Metric2Copy(21)
        conn = clearer.output().connect()
        conn.autocommit = True
        self.clear(conn, clearer.table)

        luigi.build([Metric1(0), Metric1(1)], local_scheduler=True)
        luigi.build([clearer], local_scheduler=True)
        cursor = conn.cursor()
        cursor.execute(
            'select count(*) from {table}'.format(table=clearer.table))
        self.assertEquals(tuple(cursor), ((3,),))

    def _test_exists_query(self, task):
        conn = task.output().connect()
        conn.autocommit = True
        self.clear(conn, task.table)
        task.run()
        self.assertTrue(task.complete())  # uses marker

        conn.cursor().execute(
            "DELETE FROM {marker_table} WHERE target_table = %s".format(
                marker_table=postgres.PostgresTarget.marker_table),
            (task.table,)
        )
        self.assertFalse(task.complete())  # uses marker
        task.run()
        self.assertTrue(task.complete())
        # now, there should not be duplicate data
        c = conn.cursor()
        c.execute("SELECT count(*) FROM {data_table}".format(
            data_table=task.table))
        return c.fetchone()

    def test_no_exists_query_fail(self):
        # results are duplicated if there is no exists query
        class TestExistsTask(TestPostgresTask):
            table = "exists_test"

        task = TestExistsTask()
        result_rows = self._test_exists_query(task)
        self.assertEquals(result_rows, (6,))

    def test_exists_query(self):
        # result marker is inserted if there is an exists query
        class TestExistsTask2(TestPostgresTask):
            table = "exists_test"

            def exists_query(self):
                return "SELECT * FROM {0} LIMIT 1".format(self.table)

        task = TestExistsTask2()
        result_rows = self._test_exists_query(task)
        self.assertEquals(result_rows, (3,))

luigi.namespace()
