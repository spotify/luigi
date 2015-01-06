__author__ = 'gbalaraman'

import unittest
import sqlalchemy
import luigi
from luigi.mock import MockFile
from luigi.contrib import sqla

CONNECTION_STRING = "sqlite:///"
TASK_LIST = ["item%d\tproperty%d\n" % (i, i) for i in range(10)]


class BaseTask(luigi.Task):

    def output(self):
        return MockFile("BaseTask",  mirror_on_stderr=True)

    def run(self):
        out = self.output().open("w")
        for task in TASK_LIST:
            out.write(task)
        out.close()

class SQLATask(sqla.CopyToTable):
    columns = [
        sqlalchemy.Column("item", sqlalchemy.String(64)),
        sqlalchemy.Column("property", sqlalchemy.String(64))
    ]
    connection_string = "sqlite:////tmp/test.db" #CONNECTION_STRING
    table = "item_property"

    def requires(self):
        return BaseTask()




class TestSQLA(unittest.TestCase):

    def _clear_tables(self):
        meta = sqlalchemy.MetaData()
        meta.reflect(bind=self.engine)
        for table in reversed(meta.sorted_tables):
            self.engine.execute(table.delete())

    def setUp(self):
        self.engine = sqlalchemy.create_engine(SQLATask.connection_string)
        self._clear_tables()

    def tearDown(self):
        pass

    def test_create_table(self):
        """
        Test that this method creates table that we require
        :return:
        """
        class TestSQLData(sqla.CopyToTable):
            connection_string = CONNECTION_STRING
            table = "test_table"
            columns = [
                sqlalchemy.Column("id", sqlalchemy.Integer,primary_key=True),
                sqlalchemy.Column("name", sqlalchemy.String(64)),
                sqlalchemy.Column("value", sqlalchemy.String(64))
            ]

        def output(self):
            pass


        sql_copy = TestSQLData()
        eng = sqlalchemy.create_engine(TestSQLData.connection_string)
        self.assertEqual(False, eng.dialect.has_table(eng.connect(), TestSQLData.table))
        sql_copy.create_table(eng)
        self.assertEqual(True, eng.dialect.has_table(eng.connect(), TestSQLData.table))
        # repeat and ensure it just binds to existing table
        sql_copy.create_table(eng)

    def test_create_table_raises_no_columns(self):
        """
        Check that the test fails when the columns are not set
        :return:
        """
        class TestSQLData(sqla.CopyToTable):
            connection_string = CONNECTION_STRING
            table = "test_table"
            columns = []

        def output(self):
            pass

        sql_copy = TestSQLData()
        eng = sqlalchemy.create_engine(TestSQLData.connection_string)
        self.assertRaises(NotImplementedError, sql_copy.create_table, eng)

    def _check_entries(self, engine):
        with engine.begin() as conn:
            meta = sqlalchemy.MetaData()
            meta.reflect(bind=engine)
            self.assertSetEqual(set([u'table_updates', u'item_property']), set(meta.tables.keys()))
            table = meta.tables[SQLATask.table]
            s = sqlalchemy.select([sqlalchemy.func.count(table.c.item)])
            result = conn.execute(s).fetchone()
            self.assertEqual(len(TASK_LIST),  result[0])
            s = sqlalchemy.select([table]).order_by(table.c.item)
            result = conn.execute(s).fetchall()
            for i in range(len(TASK_LIST)):
                given = TASK_LIST[i].strip("\n").split("\t")
                given = (unicode(given[0]), unicode(given[1]))
                self.assertTupleEqual(given, tuple(result[i]))

    def test_rows(self):
        task, task0 = SQLATask(), BaseTask()
        luigi.build([task, task0], local_scheduler=True)

        for i,row in enumerate(task.rows()):
            given = TASK_LIST[i].strip("\n").split("\t")
            self.assertListEqual(row, given)

    def test_run(self):

        task, task0 = SQLATask(), BaseTask()
        luigi.build([task, task0], local_scheduler=True)
        self._check_entries(self.engine)

        # rerun and the num entries should be the same
        luigi.build([task, task0], local_scheduler=True)
        self._check_entries(self.engine)

    def test_run_with_chunk_size(self):
        task, task0 = SQLATask(), BaseTask()
        task.chunk_size = 2 # change chunk size and check it runs ok
        luigi.build([task, task0], local_scheduler=True)
        self._check_entries(self.engine)

    def test_create_marker_table(self):
        target = sqla.SQLAlchemyTarget(CONNECTION_STRING, "test_table", "12312123")
        target.create_marker_table()
        self.assertTrue(target.engine.dialect.has_table(target.engine.connect(), target.marker_table))

    def test_touch(self):
        target = sqla.SQLAlchemyTarget(CONNECTION_STRING, "test_table", "12312123")
        target.create_marker_table()
        self.assertFalse(target.exists())
        target.touch()
        self.assertTrue(target.exists())


