# -*- coding: utf-8 -*-
#
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
#
"""
This file implements unit test cases for luigi/contrib/sqla.py
Author: Gouthaman Balaraman
Date: 01/02/2015
"""
import os
import shutil
import tempfile
import unittest

from luigi import six

import luigi
import sqlalchemy
from luigi.contrib import sqla
from luigi.mock import MockFile
from nose.plugins.attrib import attr
from helpers import skipOnTravis

if six.PY3:
    unicode = str


class BaseTask(luigi.Task):

    TASK_LIST = ["item%d\tproperty%d\n" % (i, i) for i in range(10)]

    def output(self):
        return MockFile("BaseTask", mirror_on_stderr=True)

    def run(self):
        out = self.output().open("w")
        for task in self.TASK_LIST:
            out.write(task)
        out.close()


@attr('sqlalchemy')
class TestSQLA(unittest.TestCase):
    NUM_WORKERS = 1

    def _clear_tables(self):
        meta = sqlalchemy.MetaData()
        meta.reflect(bind=self.engine)
        for table in reversed(meta.sorted_tables):
            self.engine.execute(table.delete())

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.connection_string = self.get_connection_string()
        self.connect_args = {'timeout': 5.0}
        self.engine = sqlalchemy.create_engine(self.connection_string, connect_args=self.connect_args)

        # Create SQLATask and store in self
        class SQLATask(sqla.CopyToTable):
            columns = [
                (["item", sqlalchemy.String(64)], {}),
                (["property", sqlalchemy.String(64)], {})
            ]
            connection_string = self.connection_string
            connect_args = self.connect_args
            table = "item_property"
            chunk_size = 1

            def requires(self):
                return BaseTask()

        self.SQLATask = SQLATask

    def tearDown(self):
        self._clear_tables()
        if os.path.exists(self.tempdir):
            shutil.rmtree(self.tempdir)

    def get_connection_string(self, db='sqlatest.db'):
        return "sqlite:///{path}".format(path=os.path.join(self.tempdir, db))

    def test_create_table(self):
        """
        Test that this method creates table that we require
        :return:
        """
        class TestSQLData(sqla.CopyToTable):
            connection_string = self.connection_string
            connect_args = self.connect_args
            table = "test_table"
            columns = [
                (["id", sqlalchemy.Integer], dict(primary_key=True)),
                (["name", sqlalchemy.String(64)], {}),
                (["value", sqlalchemy.String(64)], {})
            ]
            chunk_size = 1

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
            connection_string = self.connection_string
            table = "test_table"
            columns = []
            chunk_size = 1

        def output(self):
            pass

        sql_copy = TestSQLData()
        eng = sqlalchemy.create_engine(TestSQLData.connection_string)
        self.assertRaises(NotImplementedError, sql_copy.create_table, eng)

    def _check_entries(self, engine):
        with engine.begin() as conn:
            meta = sqlalchemy.MetaData()
            meta.reflect(bind=engine)
            self.assertEqual(set([u'table_updates', u'item_property']), set(meta.tables.keys()))
            table = meta.tables[self.SQLATask.table]
            s = sqlalchemy.select([sqlalchemy.func.count(table.c.item)])
            result = conn.execute(s).fetchone()
            self.assertEqual(len(BaseTask.TASK_LIST), result[0])
            s = sqlalchemy.select([table]).order_by(table.c.item)
            result = conn.execute(s).fetchall()
            for i in range(len(BaseTask.TASK_LIST)):
                given = BaseTask.TASK_LIST[i].strip("\n").split("\t")
                given = (unicode(given[0]), unicode(given[1]))
                self.assertEqual(given, tuple(result[i]))

    def test_rows(self):
        task, task0 = self.SQLATask(), BaseTask()
        luigi.build([task, task0], local_scheduler=True, workers=self.NUM_WORKERS)

        for i, row in enumerate(task.rows()):
            given = BaseTask.TASK_LIST[i].strip("\n").split("\t")
            self.assertEqual(row, given)

    def test_run(self):
        """
        Checking that the runs go as expected. Rerunning the same shouldn't end up
        inserting more rows into the db.
        :return:
        """
        task, task0 = self.SQLATask(), BaseTask()
        self.engine = sqlalchemy.create_engine(task.connection_string)
        luigi.build([task0, task], local_scheduler=True)
        self._check_entries(self.engine)

        # rerun and the num entries should be the same
        luigi.build([task0, task], local_scheduler=True, workers=self.NUM_WORKERS)
        self._check_entries(self.engine)

    def test_run_with_chunk_size(self):
        """
        The chunk_size can be specified in order to control the batch size for inserts.
        :return:
        """
        task, task0 = self.SQLATask(), BaseTask()
        self.engine = sqlalchemy.create_engine(task.connection_string)
        task.chunk_size = 2  # change chunk size and check it runs ok
        luigi.build([task, task0], local_scheduler=True, workers=self.NUM_WORKERS)
        self._check_entries(self.engine)

    def test_reflect(self):
        """
        If the table is setup already, then one can set reflect to True, and
        completely skip the columns part. It is not even required at that point.
        :return:
        """
        SQLATask = self.SQLATask

        class AnotherSQLATask(sqla.CopyToTable):
            connection_string = self.connection_string
            table = "item_property"
            reflect = True
            chunk_size = 1

            def requires(self):
                return SQLATask()

            def copy(self, conn, ins_rows, table_bound):
                ins = table_bound.update().\
                    where(table_bound.c.property == sqlalchemy.bindparam("_property")).\
                    values({table_bound.c.item: sqlalchemy.bindparam("_item")})
                conn.execute(ins, ins_rows)

            def rows(self):
                for line in BaseTask.TASK_LIST:
                    yield line.strip("\n").split("\t")

        task0, task1, task2 = AnotherSQLATask(), self.SQLATask(), BaseTask()
        luigi.build([task0, task1, task2], local_scheduler=True, workers=self.NUM_WORKERS)
        self._check_entries(self.engine)

    def test_create_marker_table(self):
        """
        Is the marker table created as expected for the SQLAlchemyTarget
        :return:
        """
        target = sqla.SQLAlchemyTarget(self.connection_string, "test_table", "12312123")
        target.create_marker_table()
        self.assertTrue(target.engine.dialect.has_table(target.engine.connect(), target.marker_table))

    def test_touch(self):
        """
        Touch takes care of creating a checkpoint for task completion
        :return:
        """
        target = sqla.SQLAlchemyTarget(self.connection_string, "test_table", "12312123")
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
            connection_string = self.connection_string
            table = "item_property"
            chunk_size = 1

            def rows(self):
                tasks = [("item0", "property0"), ("item1", "property1"), ("item2", "property2"), ("item3", "property3"),
                         ("item4", "property4"), ("item5", "property5"), ("item6", "property6"), ("item7", "property7"),
                         ("item8", "property8"), ("item9", "property9")]
                for row in tasks:
                    yield row

        task = SQLARowOverloadTest()
        luigi.build([task], local_scheduler=True, workers=self.NUM_WORKERS)
        self._check_entries(self.engine)

    def test_column_row_separator(self):
        """
        Test alternate column row separator works
        :return:
        """
        class ModBaseTask(luigi.Task):

            def output(self):
                return MockFile("ModBaseTask", mirror_on_stderr=True)

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
            connection_string = self.connection_string
            table = "item_property"
            column_separator = ","
            chunk_size = 1

            def requires(self):
                return ModBaseTask()

        task1, task2 = ModBaseTask(), ModSQLATask()
        luigi.build([task1, task2], local_scheduler=True, workers=self.NUM_WORKERS)
        self._check_entries(self.engine)

    def test_update_rows_test(self):
        """
        Overload the copy() method and implement an update action.
        :return:
        """
        class ModBaseTask(luigi.Task):

            def output(self):
                return MockFile("BaseTask", mirror_on_stderr=True)

            def run(self):
                out = self.output().open("w")
                for task in self.TASK_LIST:
                    out.write("dummy_" + task)
                out.close()

        class ModSQLATask(sqla.CopyToTable):
            connection_string = self.connection_string
            table = "item_property"
            columns = [
                (["item", sqlalchemy.String(64)], {}),
                (["property", sqlalchemy.String(64)], {})
            ]
            chunk_size = 1

            def requires(self):
                return ModBaseTask()

        class UpdateSQLATask(sqla.CopyToTable):
            connection_string = self.connection_string
            table = "item_property"
            reflect = True
            chunk_size = 1

            def requires(self):
                return ModSQLATask()

            def copy(self, conn, ins_rows, table_bound):
                ins = table_bound.update().\
                    where(table_bound.c.property == sqlalchemy.bindparam("_property")).\
                    values({table_bound.c.item: sqlalchemy.bindparam("_item")})
                conn.execute(ins, ins_rows)

            def rows(self):
                for task in self.TASK_LIST:
                    yield task.strip("\n").split("\t")

        # Running only task1, and task2 should fail
        task1, task2, task3 = ModBaseTask(), ModSQLATask(), UpdateSQLATask()
        luigi.build([task1, task2, task3], local_scheduler=True, workers=self.NUM_WORKERS)
        self._check_entries(self.engine)

    @skipOnTravis('AssertionError: 10 != 7; https://travis-ci.org/spotify/luigi/jobs/156732446')
    def test_multiple_tasks(self):
        """
        Test a case where there are multiple tasks
        :return:
        """
        class SmallSQLATask(sqla.CopyToTable):
            item = luigi.Parameter()
            property = luigi.Parameter()
            columns = [
                (["item", sqlalchemy.String(64)], {}),
                (["property", sqlalchemy.String(64)], {})
            ]
            connection_string = self.connection_string
            table = "item_property"
            chunk_size = 1

            def rows(self):
                yield (self.item, self.property)

        class ManyBaseTask(luigi.Task):
            def requires(self):
                for t in BaseTask.TASK_LIST:
                    item, property = t.strip().split("\t")
                    yield SmallSQLATask(item=item, property=property)

        task2 = ManyBaseTask()
        luigi.build([task2], local_scheduler=True, workers=self.NUM_WORKERS)
        self._check_entries(self.engine)

    def test_multiple_engines(self):
        """
        Test case where different tasks require different SQL engines.
        """
        alt_db = self.get_connection_string("sqlatest2.db")

        class MultiEngineTask(self.SQLATask):
            connection_string = alt_db

        task0, task1, task2 = BaseTask(), self.SQLATask(), MultiEngineTask()
        self.assertTrue(task1.output().engine != task2.output().engine)
        luigi.build([task2, task1, task0], local_scheduler=True, workers=self.NUM_WORKERS)
        self._check_entries(task1.output().engine)
        self._check_entries(task2.output().engine)


@attr('sqlalchemy')
class TestSQLA2(TestSQLA):
    """ 2 workers version
    """
    NUM_WORKERS = 2
