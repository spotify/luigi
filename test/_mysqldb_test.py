import mysql.connector
from luigi.contrib.mysqldb import MySqlTarget
import unittest

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
