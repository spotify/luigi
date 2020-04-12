import unittest

import mock
from pyhive.presto import Cursor, Connection
from pyhive.exc import DatabaseError

from luigi.contrib.presto import PrestoTask, PrestoClient, PrestoTarget


class WithPrestoClientTest(unittest.TestCase):
    def test_creates_client_with_expected_params(self):
        # arrange
        class _Task(PrestoTask):
            host = '127.0.0.1'
            port = 8089
            user = 'user_123'
            database = 'db1'
            table = 'tbl1'

        expected_connection_kwargs = {
            'host': '127.0.0.1',
            'port': 8089,
            'username': 'user_123',
            'catalog': 'hive',
            'protocol': 'https',
            'source': 'pyhive',
            'poll_interval': 1.0,
            'schema': 'db1',
            'requests_kwargs': {
                'verify': False
            }
        }

        # act
        task = _Task()

        # assert
        client = task._client
        assert isinstance(client, PrestoClient)
        connection = client._connection
        assert not connection._args
        assert connection._kwargs == expected_connection_kwargs


class PrestoClientTest(unittest.TestCase):
    @mock.patch('luigi.contrib.presto.sleep', return_value=None)
    def test_watch(self, sleep):
        # arrange
        status = {
            "stats": {
                "progressPercentage": 1.2
            },
            "infoUri": "http://127.0.0.1:8080/ui/query.html?query=123"
        }
        cursor = mock.MagicMock(spec=Cursor)
        cursor.poll.side_effect = [status, None]

        connection = mock.MagicMock(spec=Connection)
        connection.cursor.return_value = cursor

        client = PrestoClient(connection)
        query = 'select 1'

        # act
        statuses = list(client.execute(query))

        # assert
        assert client.percentage_progress == 1.2
        assert client.info_uri == 'http://127.0.0.1:8080/ui/query.html?query=123'
        assert statuses == [status]
        cursor.execute.assert_called_once_with(query, None)
        cursor.close.assert_called_once_with()

    @mock.patch('luigi.contrib.presto.sleep', return_value=None)
    def test_fetch(self, sleep):
        # arrange
        status = {
            "infoUri": "http://127.0.0.1:8080/ui/query.html?query=123"
        }
        cursor = mock.MagicMock(spec=Cursor)
        cursor.poll.side_effect = [status, None]
        cursor.fetchall.return_value = [(1,), (2,)]

        connection = mock.MagicMock(spec=Connection)
        connection.cursor.return_value = cursor

        client = PrestoClient(connection)
        query = 'select 1'

        # act
        result = list(client.execute(query, mode='fetch'))

        # assert
        assert client.percentage_progress == .1
        assert client.info_uri == "http://127.0.0.1:8080/ui/query.html?query=123"
        cursor.execute.assert_called_once_with(query, None)
        cursor.close.assert_called_once_with()
        assert result == [(1,), (2,)]


class PrestoTargetTest(unittest.TestCase):
    def test_non_partitioned(self):
        # arrange
        client = mock.MagicMock(spec=PrestoClient)
        client.execute.return_value = iter([(7, None), ])

        catalog = 'hive'
        database = 'schm1'
        table = 'tbl1'

        # act
        target = PrestoTarget(client, catalog, database, table)
        count = target.count()
        exists = target.exists()

        # assert
        client.execute.assert_called_once_with(
            'SELECT COUNT(*) AS cnt FROM hive.schm1.tbl1 WHERE 1 = %s LIMIT 1',
            [1, ],
            mode='fetch'
        )
        assert count == 7
        assert exists

    def test_partitioned(self):
        # arrange
        client = mock.MagicMock(spec=PrestoClient)
        client.execute.return_value = iter([(7, None), ])

        catalog = 'hive'
        database = 'schm1'
        table = 'tbl1'
        partition = {
            'a': 2,
            'b': 'x'
        }

        # act
        target = PrestoTarget(client, catalog, database, table, partition)
        count = target.count()
        exists = target.exists()

        # assert
        client.execute.assert_called_once_with(
            'SELECT COUNT(*) AS cnt FROM hive.schm1.tbl1 WHERE a = %s AND b = %s LIMIT 1',
            [2, 'x'],
            mode='fetch'
        )
        assert count == 7
        assert exists

    def test_table_doesnot_exist(self):
        # arrange
        e = DatabaseError()
        setattr(e, 'message', {u'message': u'line 1:15: Table hive.schm1.tbl1 does not exist'})

        client = mock.MagicMock(spec=PrestoClient)
        client.execute.side_effect = e

        catalog = 'hive'
        database = 'schm1'
        table = 'tbl1'

        # act
        target = PrestoTarget(client, catalog, database, table)
        exists = target.exists()

        # assert
        client.execute.assert_called_once_with(
            'SELECT COUNT(*) AS cnt FROM hive.schm1.tbl1 WHERE 1 = %s LIMIT 1',
            [1],
            mode='fetch'
        )
        assert not exists


class PrestoTest(unittest.TestCase):

    @mock.patch('luigi.contrib.presto.sleep', return_value=None)
    def test_run(self, sleep):
        # arrange
        client = mock.MagicMock(spec=PrestoClient)
        client.execute.return_value = [(), (), ()]
        client.info_uri = 'http://127.0.0.1:8080/ui/query.html?query=123'
        client.percentage_progress = 2.3

        class _Task(PrestoTask):
            host = '127.0.0.1'
            port = 8089
            user = 'user_123'
            password = '123'
            database = 'db1'
            table = 'tbl1'
            query = 'select 1'

        # act
        with mock.patch('luigi.contrib.presto.PrestoClient', return_value=client):
            task = _Task()
            task.set_progress_percentage = mock.MagicMock()
            task.set_tracking_url = mock.MagicMock()
            task.run()

        # assert
        assert task.protocol == 'https'
        assert task.output().catalog == 'hive'
        assert task.output().database == 'db1'
        assert task.output().table == 'tbl1'
        assert task.output().partition is None

        client.execute.assert_called_once_with('select 1')
        task.set_tracking_url.assert_called_once_with('http://127.0.0.1:8080/ui/query.html?query=123')
        assert task.set_progress_percentage.mock_calls == [
            mock.call(2.3),
            mock.call(2.3),
            mock.call(2.3),
        ]
