__author__ = 'gbalaraman'

import unittest
import sqlalchemy
import luigi
from luigi.mock import MockFile
from luigi.contrib import sqla

CONNECTION_STRING = "sqlite:///"



class BaseTask(luigi.Task):

    def output(self):
        return MockFile("BaseTask",  mirror_on_stderr=True)

    def run(self):
        out = self.output().open("w")
        for i in range(10):
            out.write("item%d\tproperty%d\n" %(i, i))
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



class TestSQLACopyToTable(unittest.TestCase):

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


    def test_rows(self):
        sqla_task = SQLATask()
        for row in sqla_task.rows():
            print row


class SQLAlchemyTarget(unittest.TestCase):

    def test_create_marker_table(self):
        target = sqla.SQLAlchemyTarget(CONNECTION_STRING, "test_table", "12312123")
        target.create_marker_table()
        self.assertEqual(True, target.engine.dialect.has_table(target.engine.connect(), target.marker_table))

    def test_touch(self):
        target = sqla.SQLAlchemyTarget(CONNECTION_STRING, "test_table", "12312123")
        target.create_marker_table()
        self.assertEqual(False, target.exists())
        target.touch()
        self.assertEqual(True, target.exists())

    def test_run(self):

        task, task0 = SQLATask(), BaseTask()
        meta = sqlalchemy.MetaData()
        engine = task.output().engine
        meta.reflect(bind=engine)
        with engine.begin() as con:
            if con.dialect.has_table(con, task.table):
                table_bound = meta.tables[task.table]
                table_bound.drop(engine)
            if con.dialect.has_table(con, task.output().marker_table):
                table_bound = meta.tables[task.output().marker_table]
                table_bound.drop(engine)

        luigi.build([task, task0], local_scheduler=True)

        engine = sqlalchemy.create_engine(task.connection_string)
        with engine.begin() as conn:
        #with task.output().engine.begin() as conn:
            meta = sqlalchemy.MetaData()
            meta.reflect(bind=engine)
            self.assertSetEqual(set([u'table_updates', u'item_property']), set(meta.tables.keys()))

            table = meta.tables[task.table]
            s = sqlalchemy.select([sqlalchemy.func.count(table.c.item)])
            result = conn.execute(s).fetchone()
            self.assertEqual(10,  result[0])

