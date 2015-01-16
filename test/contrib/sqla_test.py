# Copyright (c) 2015 Gouthaman Balaraman
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

"""
This file implements unit test cases for luigi/contrib/sqla.py
Author: Gouthaman Balaraman
Date: 01/02/2015
"""
import unittest
import sqlalchemy
import luigi
import os
import shutil
import tempfile
from luigi.mock import MockFile
from luigi.contrib import sqla

#################################
# Globals part of the test case #
#################################
TEMPDIR = tempfile.mkdtemp()
SQLITEPATH = os.path.join(TEMPDIR, "sqlatest.db")
CONNECTION_STRING = "sqlite:///%s" % SQLITEPATH
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
        (["item", sqlalchemy.String(64)], {}),
        (["property", sqlalchemy.String(64)], {})
    ]
    connection_string = CONNECTION_STRING
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
        if not os.path.exists(TEMPDIR):
            os.mkdir(TEMPDIR)
        self.engine = sqlalchemy.create_engine(CONNECTION_STRING)

    def tearDown(self):
        self._clear_tables()
        if os.path.exists(TEMPDIR):
            shutil.rmtree(TEMPDIR)


    def test_create_table(self):
        """
        Test that this method creates table that we require
        :return:
        """
        class TestSQLData(sqla.CopyToTable):
            connection_string = CONNECTION_STRING
            table = "test_table"
            columns = [
                (["id", sqlalchemy.Integer], dict(primary_key=True)),
                (["name", sqlalchemy.String(64)], {}),
                (["value", sqlalchemy.String(64)], {})
            ]

            def output(self):
                pass

        sql_copy = TestSQLData()
        eng = sqlalchemy.create_engine(TestSQLData.connection_string)
        self.assertFalse(eng.dialect.has_table(eng.connect(), TestSQLData.table))
        sql_copy.create_table(eng)
        self.assertTrue(eng.dialect.has_table(eng.connect(), TestSQLData.table))
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

        for i, row in enumerate(task.rows()):
            given = TASK_LIST[i].strip("\n").split("\t")
            self.assertListEqual(row, given)

    def test_run(self):
        """
        Checking that the runs go as expected. Rerunning the same shouldn't end up
        inserting more rows into the db.
        :return:
        """
        task, task0 = SQLATask(), BaseTask()
        self.engine = sqlalchemy.create_engine(task.connection_string)
        luigi.build([task0, task], local_scheduler=True)
        self._check_entries(self.engine)

        # rerun and the num entries should be the same
        luigi.build([task0, task], local_scheduler=True)
        self._check_entries(self.engine)

    def test_run_with_chunk_size(self):
        """
        The chunk_size can be specified in order to control the batch size for inserts.
        :return:
        """
        task, task0 = SQLATask(), BaseTask()
        self.engine = sqlalchemy.create_engine(task.connection_string)
        task.chunk_size = 2 # change chunk size and check it runs ok
        luigi.build([task, task0], local_scheduler=True)
        self._check_entries(self.engine)

    def test_reflect(self):
        """
        If the table is setup already, then one can set reflect to True, and
        completely skip the columns part. It is not even required at that point.
        :return:
        """

        class AnotherSQLATask(sqla.CopyToTable):
            connection_string = CONNECTION_STRING
            table = "item_property"
            reflect = True

            def requires(self):
                return SQLATask()

        task0, task1, task2 = AnotherSQLATask(), SQLATask(), BaseTask()
        luigi.build([task0, task1, task2], local_scheduler=True)
        self._check_entries(self.engine)


    def test_create_marker_table(self):
        """
        Is the marker table created as expected for the SQLAlchemyTarget
        :return:
        """
        target = sqla.SQLAlchemyTarget(CONNECTION_STRING, "test_table", "12312123")
        target.create_marker_table()
        self.assertTrue(target.engine.dialect.has_table(target.engine.connect(), target.marker_table))

    def test_touch(self):
        """
        Touch takes care of creating a checkpoint for task completion
        :return:
        """
        target = sqla.SQLAlchemyTarget(CONNECTION_STRING, "test_table", "12312123")
        target.create_marker_table()
        self.assertFalse(target.exists())
        target.touch()
        self.assertTrue(target.exists())


    def test_row_overload(self):
        """Overload the rows method and we should be able to insert data into database"""

        class SQLARowOverloadTest(sqla.CopyToTable):
            columns = [
                (["item", sqlalchemy.String(64)], {}),
                (["property", sqlalchemy.String(64)], {})
            ]
            connection_string = CONNECTION_STRING
            table = "item_property"

            def rows(self):
                tasks = [("item0", "property0"), ("item1", "property1"), ("item2", "property2"), ("item3", "property3"),
                         ("item4", "property4"), ("item5", "property5"), ("item6", "property6"), ("item7", "property7"),
                         ("item8", "property8"), ("item9", "property9")]
                for row in tasks:
                    yield row

        task = SQLARowOverloadTest()
        luigi.build([task], local_scheduler=True)
        self._check_entries(self.engine)


    def test_column_row_separator(self):

        class ModBaseTask(luigi.Task):

            def output(self):
                return MockFile("ModBaseTask",  mirror_on_stderr=True)

            def run(self):
                out = self.output().open("w")
                tasks = ["item%d,property%d\n" % (i, i) for i in range(10)]
                for task in tasks:
                    out.write(task)
                out.close()


        class ModSQLATask(sqla.CopyToTable):
            columns = [
                (["item", sqlalchemy.String(64)], {}),
                (["property", sqlalchemy.String(64)], {})
            ]
            connection_string = CONNECTION_STRING
            table = "item_property"
            column_separator = ","

            def requires(self):
                return ModBaseTask()

        task1, task2 = ModBaseTask(), ModSQLATask()
        luigi.build([task1, task2], local_scheduler=True)
        self._check_entries(self.engine)