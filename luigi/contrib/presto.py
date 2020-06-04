import inspect
import logging
import re
from collections import OrderedDict
from contextlib import closing
from enum import Enum
from time import sleep

import luigi
from luigi.contrib import rdbms
from luigi.task_register import Register

logger = logging.getLogger('luigi-interface')

try:
    from pyhive.presto import Connection, Cursor
    from pyhive.exc import DatabaseError
except ImportError:
    logger.warning("pyhive[presto] is not installed.")


class presto(luigi.Config):  # NOQA
    host = luigi.Parameter(default='localhost', description='Presto host')
    port = luigi.IntParameter(default=8090, description='Presto port')
    user = luigi.Parameter(default='anonymous', description='Presto user')
    catalog = luigi.Parameter(default='hive', description='Default catalog')
    password = luigi.Parameter(default=None, description='User password')
    protocol = luigi.Parameter(default='https', description='Presto connection protocol')
    poll_interval = luigi.FloatParameter(
        default=1.0,
        description=' how often to ask the Presto REST interface for a progress update, defaults to a second'
    )


class PrestoClient:
    """
    Helper class wrapping `pyhive.presto.Connection`
    for executing presto queries and tracking progress
    """

    def __init__(self, connection, sleep_time=1):
        self.sleep_time = sleep_time
        self._connection = connection
        self._status = {'state': 'initial'}

    @property
    def percentage_progress(self):
        """
        :return: percentage of query overall progress
        """
        return self._status.get('stats', {}).get('progressPercentage', 0.1)

    @property
    def info_uri(self):
        """
        :return: query UI link
        """
        return self._status.get('infoUri')

    def execute(self, query, parameters=None, mode=None):
        """

        :param query: query to run
        :param parameters: parameters should be injected in the query
        :param mode: "fetch" - yields rows, "watch" - yields log entries
        :return:
        """
        class Mode(Enum):
            watch = 'watch'
            fetch = 'fetch'

        _mode = Mode(mode) if mode else Mode.watch

        with closing(self._connection.cursor()) as cursor:
            cursor.execute(query, parameters)
            status = self._status
            while status:
                sleep(self.sleep_time)
                status = cursor.poll()
                if status:
                    if _mode == Mode.watch:
                        yield status
                    self._status = status

            if _mode == Mode.fetch:
                for row in cursor.fetchall():
                    yield row


class WithPrestoClient(Register):
    """
    A metaclass for injecting `PrestoClient` as a `_client` field into a new instance of class `T`
    Presto connection options are taken from `T`-instance fields
    Fields should have the same names as in `pyhive.presto.Cursor`
    """

    def __new__(cls, name, bases, attrs):
        def _client(self):
            def _kwargs():
                """
                replace to
                ```
                (_self, *args), *_ = inspect.getfullargspec(Cursor.__init__)
                ```
                after py2-deprecation
                """
                args = inspect.getargspec(Cursor.__init__)[0][1:]
                for parameter in args:
                    val = getattr(self, parameter)
                    if val:
                        yield parameter, val

            connection = Connection(**dict(_kwargs()))
            return PrestoClient(connection=connection)

        attrs.update({
            '_client': property(_client)
        })
        return super(cls, WithPrestoClient).__new__(cls, name, bases, attrs)


class PrestoTarget(luigi.Target):
    """
    Target for presto-accessible tables
    """
    def __init__(self, client, catalog, database, table, partition=None):
        self.catalog = catalog
        self.database = database
        self.table = table
        self.partition = partition
        self._client = client
        self._count = None

    @property
    def _count_query(self):
        partition = OrderedDict(self.partition or {1: 1})

        def _clauses():
            for k in partition.keys():
                yield '{} = %s'.format(k)

        clauses = ' AND '.join(_clauses())

        query = 'SELECT COUNT(*) AS cnt FROM {}.{}.{} WHERE {} LIMIT 1'.format(
            self.catalog,
            self.database,
            self.table,
            clauses
        )
        params = list(partition.values())
        return query, params

    def _table_doesnot_exist(self, exception):
        pattern = re.compile(
            r'line (\d+):(\d+): Table {}.{}.{} does not exist'.format(
                self.catalog,
                self.database,
                self.table
            )
        )
        try:
            message = exception.message['message']
            if pattern.match(message):
                return True
        finally:
            return False

    def count(self):
        if not self._count:
            '''
            replace to
            self._count, *_ = next(self._client.execute(*self.count_query, 'fetch'))
            after py2 deprecation
            '''
            self._count = next(self._client.execute(*self._count_query, mode='fetch'))[0]
        return self._count

    def exists(self):
        """

        :return: `True` if given table exists and there are any rows in a given partition
                 `False` if no rows in the partition exists or table is absent
        """
        try:
            return self.count() > 0
        except DatabaseError as exception:
            if self._table_doesnot_exist(exception):
                return False
        except Exception:
            raise


class PrestoTask(rdbms.Query, metaclass=WithPrestoClient):
    """
    Task for executing presto queries
    During its executions tracking url and percentage progress are set
    """
    _tracking_url_set = False

    @property
    def host(self):
        return presto().host

    @property
    def port(self):
        return presto().port

    @property
    def user(self):
        return presto().user

    @property
    def username(self):
        return self.user

    @property
    def schema(self):
        return self.database

    @property
    def password(self):
        return presto().password

    @property
    def catalog(self):
        return presto().catalog

    @property
    def poll_interval(self):
        return presto().poll_interval

    @property
    def source(self):
        return 'pyhive'

    @property
    def partition(self):
        return None

    @property
    def protocol(self):
        return 'https' if self.password else presto().protocol

    @property
    def session_props(self):
        return None

    @property
    def requests_session(self):
        return None

    @property
    def requests_kwargs(self):
        return {
            'verify': False
        }

    query = None

    def _maybe_set_tracking_url(self):
        if not self._tracking_url_set:
            self.set_tracking_url(self._client.info_uri)
            self._tracking_url_set = True

    def _set_progress(self):
        self.set_progress_percentage(self._client.percentage_progress)

    def run(self):
        for _ in self._client.execute(self.query):
            self._maybe_set_tracking_url()
            self._set_progress()

    def output(self):
        return PrestoTarget(
            client=self._client,
            catalog=self.catalog,
            database=self.database,
            table=self.table,
            partition=self.partition,
        )
