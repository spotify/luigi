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

from helpers import unittest

import mysql.connector
from luigi.contrib.mysqldb import MySqlTarget

host = 'localhost'
port = 3306
database = 'luigi_test'
username = None
password = None
table_updates = 'table_updates'


def _create_test_database():
    con = mysql.connector.connect(user=username,
                                  password=password,
                                  host=host,
                                  port=port,
                                  autocommit=True)
    con.cursor().execute('CREATE DATABASE IF NOT EXISTS %s' % database)


_create_test_database()
target = MySqlTarget(host, database, username, password, '', 'update_id')


class MySqlTargetTest(unittest.TestCase):

    def test_touch_and_exists(self):
        drop()
        self.assertFalse(target.exists(),
                         'Target should not exist before touching it')
        target.touch()
        self.assertTrue(target.exists(),
                        'Target should exist after touching it')


def drop():
    con = target.connect(autocommit=True)
    con.cursor().execute('DROP TABLE IF EXISTS %s' % table_updates)
