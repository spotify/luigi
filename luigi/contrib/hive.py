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

import abc
import collections
import logging
import operator
import os
import re
import subprocess
import sys
import tempfile
import warnings

import luigi
import luigi.contrib.hadoop
from luigi.contrib.hdfs import get_autoconfig_client
from luigi.target import FileAlreadyExists, FileSystemTarget
from luigi.task import flatten

logger = logging.getLogger('luigi-interface')


class HiveCommandError(RuntimeError):

    def __init__(self, message, out=None, err=None):
        super(HiveCommandError, self).__init__(message, out, err)
        self.message = message
        self.out = out
        self.err = err


def load_hive_cmd():
    return luigi.configuration.get_config().get('hive', 'command', 'hive').split(' ')


def get_hive_syntax():
    return luigi.configuration.get_config().get('hive', 'release', 'cdh4')


def get_hive_warehouse_location():
    return luigi.configuration.get_config().get('hive', 'warehouse_location', '/user/hive/warehouse')


def get_ignored_file_masks():
    return luigi.configuration.get_config().get('hive', 'ignored_file_masks', None)


def run_hive(args, check_return_code=True):
    """
    Runs the `hive` from the command line, passing in the given args, and
    returning stdout.

    With the apache release of Hive, so of the table existence checks
    (which are done using DESCRIBE do not exit with a return code of 0
    so we need an option to ignore the return code and just return stdout for parsing
    """
    cmd = load_hive_cmd() + args
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    if check_return_code and p.returncode != 0:
        raise HiveCommandError("Hive command: {0} failed with error code: {1}".format(" ".join(cmd), p.returncode),
                               stdout, stderr)
    return stdout.decode('utf-8')


def run_hive_cmd(hivecmd, check_return_code=True):
    """
    Runs the given hive query and returns stdout.
    """
    return run_hive(['-e', hivecmd], check_return_code)


def run_hive_script(script):
    """
    Runs the contents of the given script in hive and returns stdout.
    """
    if not os.path.isfile(script):
        raise RuntimeError("Hive script: {0} does not exist.".format(script))
    return run_hive(['-f', script])


def _is_ordered_dict(dikt):
    if isinstance(dikt, collections.OrderedDict):
        return True

    if sys.version_info >= (3, 7):
        return isinstance(dikt, dict)

    return False


def _validate_partition(partition):
    """
    If partition is set and its size is more than one and not ordered,
    then we're unable to restore its path in the warehouse
    """
    if (
            partition
            and len(partition) > 1
            and not _is_ordered_dict(partition)
    ):
        raise ValueError('Unable to restore table/partition location')


class HiveClient(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def table_location(self, table, database='default', partition=None):
        """
        Returns location of db.table (or db.table.partition). partition is a dict of partition key to
        value.
        """
        pass

    @abc.abstractmethod
    def table_schema(self, table, database='default'):
        """
        Returns list of [(name, type)] for each column in database.table.
        """
        pass

    @abc.abstractmethod
    def table_exists(self, table, database='default', partition=None):
        """
        Returns true if db.table (or db.table.partition) exists. partition is a dict of partition key to
        value.
        """
        pass

    @abc.abstractmethod
    def partition_spec(self, partition):
        """ Turn a dict into a string partition specification """
        pass


class HiveCommandClient(HiveClient):
    """
    Uses `hive` invocations to find information.
    """

    def table_location(self, table, database='default', partition=None):
        cmd = "use {0}; describe formatted {1}".format(database, table)
        if partition is not None:
            cmd += " PARTITION ({0})".format(self.partition_spec(partition))

        stdout = run_hive_cmd(cmd)

        for line in stdout.split("\n"):
            if "Location:" in line:
                return line.split("\t")[1]

    def table_exists(self, table, database='default', partition=None):
        if partition is None:
            stdout = run_hive_cmd('use {0}; show tables like "{1}";'.format(database, table))

            return stdout and table.lower() in stdout
        else:
            stdout = run_hive_cmd("""use %s; show partitions %s partition
                                (%s)""" % (database, table, self.partition_spec(partition)))

            if stdout:
                return True
            else:
                return False

    def table_schema(self, table, database='default'):
        describe = run_hive_cmd("use {0}; describe {1}".format(database, table))
        if not describe or "does not exist" in describe:
            return None
        return [tuple([x.strip() for x in line.strip().split("\t")]) for line in describe.strip().split("\n")]

    def partition_spec(self, partition):
        """
        Turns a dict into the a Hive partition specification string.
        """
        return ','.join(["`{0}`='{1}'".format(k, v) for (k, v) in
                         sorted(partition.items(), key=operator.itemgetter(0))])


class ApacheHiveCommandClient(HiveCommandClient):
    """
    A subclass for the HiveCommandClient to (in some cases) ignore the return code from
    the hive command so that we can just parse the output.
    """

    def table_schema(self, table, database='default'):
        describe = run_hive_cmd("use {0}; describe {1}".format(database, table), False)
        if not describe or "Table not found" in describe:
            return None
        return [tuple([x.strip() for x in line.strip().split("\t")]) for line in describe.strip().split("\n")]


class MetastoreClient(HiveClient):

    def table_location(self, table, database='default', partition=None):
        with HiveThriftContext() as client:
            if partition is not None:
                try:
                    import hive_metastore.ttypes
                    partition_str = self.partition_spec(partition)
                    thrift_table = client.get_partition_by_name(database, table, partition_str)
                except hive_metastore.ttypes.NoSuchObjectException:
                    return ''
            else:
                thrift_table = client.get_table(database, table)
            return thrift_table.sd.location

    def table_exists(self, table, database='default', partition=None):
        with HiveThriftContext() as client:
            if partition is None:
                return table in client.get_all_tables(database)
            else:
                return partition in self._existing_partitions(table, database, client)

    def _existing_partitions(self, table, database, client):
        def _parse_partition_string(partition_string):
            partition_def = {}
            for part in partition_string.split("/"):
                name, value = part.split("=")
                partition_def[name] = value
            return partition_def

        # -1 is max_parts, the # of partition names to return (-1 = unlimited)
        partition_strings = client.get_partition_names(database, table, -1)
        return [_parse_partition_string(existing_partition) for existing_partition in partition_strings]

    def table_schema(self, table, database='default'):
        with HiveThriftContext() as client:
            return [(field_schema.name, field_schema.type) for field_schema in client.get_schema(database, table)]

    def partition_spec(self, partition):
        return "/".join("%s=%s" % (k, v) for (k, v) in sorted(partition.items(), key=operator.itemgetter(0)))


class HiveThriftContext:
    """
    Context manager for hive metastore client.
    """

    def __enter__(self):
        try:
            from thrift.transport import TSocket
            from thrift.transport import TTransport
            from thrift.protocol import TBinaryProtocol
            # Note that this will only work with a CDH release.
            # This uses the thrift bindings generated by the ThriftHiveMetastore service in Beeswax.
            # If using the Apache release of Hive this import will fail.
            from hive_metastore import ThriftHiveMetastore
            config = luigi.configuration.get_config()
            host = config.get('hive', 'metastore_host')
            port = config.getint('hive', 'metastore_port')
            transport = TSocket.TSocket(host, port)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            transport.open()
            self.transport = transport
            return ThriftHiveMetastore.Client(protocol)
        except ImportError as e:
            raise Exception('Could not import Hive thrift library:' + str(e))

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.transport.close()


class WarehouseHiveClient(HiveClient):
    """
    Client for managed tables that makes decision based on presence of directory in hdfs
    """

    def __init__(self, hdfs_client=None, warehouse_location=None):
        self.hdfs_client = hdfs_client or get_autoconfig_client()
        self.warehouse_location = warehouse_location or get_hive_warehouse_location()

    def table_schema(self, table, database='default'):
        return NotImplemented

    def table_location(self, table, database='default', partition=None):
        return os.path.join(
            self.warehouse_location,
            database + '.db',
            table,
            self.partition_spec(partition)
        )

    def table_exists(self, table, database='default', partition=None):
        """
        The table/partition is considered existing if corresponding path in hdfs exists
        and contains file except those which match pattern set in  `ignored_file_masks`
        """
        path = self.table_location(table, database, partition)
        if self.hdfs_client.exists(path):
            ignored_files = get_ignored_file_masks()
            if ignored_files is None:
                return True

            filenames = self.hdfs_client.listdir(path)
            pattern = re.compile(ignored_files)
            for filename in filenames:
                if not pattern.match(filename):
                    return True

        return False

    def partition_spec(self, partition):
        _validate_partition(partition)
        return '/'.join([
            '{}={}'.format(k, v) for (k, v) in (partition or {}).items()
        ])


def get_default_client():
    syntax = get_hive_syntax()
    if syntax == "apache":
        return ApacheHiveCommandClient()
    elif syntax == "metastore":
        return MetastoreClient()
    elif syntax == 'warehouse':
        return WarehouseHiveClient()
    else:
        return HiveCommandClient()


client = get_default_client()


class HiveQueryTask(luigi.contrib.hadoop.BaseHadoopJobTask):
    """
    Task to run a hive query.
    """

    # by default, we let hive figure these out.
    n_reduce_tasks = None
    bytes_per_reducer = None
    reducers_max = None

    @abc.abstractmethod
    def query(self):
        """ Text of query to run in hive """
        raise RuntimeError("Must implement query!")

    def hiverc(self):
        """
        Location of an rc file to run before the query
        if hiverc-location key is specified in luigi.cfg, will default to the value there
        otherwise returns None.

        Returning a list of rc files will load all of them in order.
        """
        return luigi.configuration.get_config().get('hive', 'hiverc-location', default=None)

    def hivevars(self):
        """
        Returns a dict of key=value settings to be passed along
        to the hive command line via --hivevar.
        This option can be used as a separated namespace for script local variables.
        See https://cwiki.apache.org/confluence/display/Hive/LanguageManual+VariableSubstitution
        """
        return {}

    def hiveconfs(self):
        """
        Returns a dict of key=value settings to be passed along
        to the hive command line via --hiveconf. By default, sets
        mapred.job.name to task_id and if not None, sets:

        * mapred.reduce.tasks (n_reduce_tasks)
        * mapred.fairscheduler.pool (pool) or mapred.job.queue.name (pool)
        * hive.exec.reducers.bytes.per.reducer (bytes_per_reducer)
        * hive.exec.reducers.max (reducers_max)
        """
        jcs = {}
        jcs['mapred.job.name'] = "'" + self.task_id + "'"
        if self.n_reduce_tasks is not None:
            jcs['mapred.reduce.tasks'] = self.n_reduce_tasks
        if self.pool is not None:
            # Supporting two schedulers: fair (default) and capacity using the same option
            scheduler_type = luigi.configuration.get_config().get('hadoop', 'scheduler', 'fair')
            if scheduler_type == 'fair':
                jcs['mapred.fairscheduler.pool'] = self.pool
            elif scheduler_type == 'capacity':
                jcs['mapred.job.queue.name'] = self.pool
        if self.bytes_per_reducer is not None:
            jcs['hive.exec.reducers.bytes.per.reducer'] = self.bytes_per_reducer
        if self.reducers_max is not None:
            jcs['hive.exec.reducers.max'] = self.reducers_max
        return jcs

    def job_runner(self):
        return HiveQueryRunner()


class HiveQueryRunner(luigi.contrib.hadoop.JobRunner):
    """
    Runs a HiveQueryTask by shelling out to hive.
    """

    def prepare_outputs(self, job):
        """
        Called before job is started.

        If output is a `FileSystemTarget`, create parent directories so the hive command won't fail
        """
        outputs = flatten(job.output())
        for o in outputs:
            if isinstance(o, FileSystemTarget):
                parent_dir = os.path.dirname(o.path)
                if parent_dir and not o.fs.exists(parent_dir):
                    logger.info("Creating parent directory %r", parent_dir)
                    try:
                        # there is a possible race condition
                        # which needs to be handled here
                        o.fs.mkdir(parent_dir)
                    except FileAlreadyExists:
                        pass

    def get_arglist(self, f_name, job):
        arglist = load_hive_cmd() + ['-f', f_name]
        hiverc = job.hiverc()
        if hiverc:
            if isinstance(hiverc, str):
                hiverc = [hiverc]
            for rcfile in hiverc:
                arglist += ['-i', rcfile]
        hiveconfs = job.hiveconfs()
        if hiveconfs:
            for k, v in hiveconfs.items():
                arglist += ['--hiveconf', '{0}={1}'.format(k, v)]
        hivevars = job.hivevars()
        if hivevars:
            for k, v in hivevars.items():
                arglist += ['--hivevar', '{0}={1}'.format(k, v)]
        logger.info(arglist)
        return arglist

    def run_job(self, job, tracking_url_callback=None):
        if tracking_url_callback is not None:
            warnings.warn("tracking_url_callback argument is deprecated, task.set_tracking_url is "
                          "used instead.", DeprecationWarning)

        self.prepare_outputs(job)
        with tempfile.NamedTemporaryFile() as f:
            query = job.query()
            if isinstance(query, str):
                query = query.encode('utf8')
            f.write(query)
            f.flush()
            arglist = self.get_arglist(f.name, job)
            return luigi.contrib.hadoop.run_and_track_hadoop_job(arglist, job.set_tracking_url)


class HivePartitionTarget(luigi.Target):
    """
    Target representing Hive table or Hive partition
    """

    def __init__(self, table, partition, database='default', fail_missing_table=True, client=None):
        """
        @param table: Table name
        @type table: str
        @param partition: partition specificaton in form of
        dict of {"partition_column_1": "partition_value_1", "partition_column_2": "partition_value_2", ... }
        If `partition` is `None` or `{}` then target is Hive nonpartitioned table
        @param database: Database name
        @param fail_missing_table: flag to ignore errors raised due to table nonexistence
        @param client: `HiveCommandClient` instance. Default if `client is None`
        """
        self.database = database
        self.table = table
        self.partition = partition
        self.client = client or get_default_client()
        self.fail_missing_table = fail_missing_table

    def __str__(self):
        return self.path

    def exists(self):
        """
        returns `True` if the partition/table exists
        """
        try:
            logger.debug(
                "Checking Hive table '{d}.{t}' for partition {p}".format(
                    d=self.database,
                    t=self.table,
                    p=str(self.partition or {})
                )
            )

            return self.client.table_exists(self.table, self.database, self.partition)
        except HiveCommandError:
            if self.fail_missing_table:
                raise
            else:
                if self.client.table_exists(self.table, self.database):
                    # a real error occurred
                    raise
                else:
                    # oh the table just doesn't exist
                    return False

    @property
    def path(self):
        """
        Returns the path for this HiveTablePartitionTarget's data.
        """
        location = self.client.table_location(self.table, self.database, self.partition)
        if not location:
            raise Exception("Couldn't find location for table: {0}".format(str(self)))
        return location


class HiveTableTarget(HivePartitionTarget):
    """
    Target representing non-partitioned table
    """

    def __init__(self, table, database='default', client=None):
        super(HiveTableTarget, self).__init__(
            table=table,
            partition=None,
            database=database,
            fail_missing_table=False,
            client=client,
        )


class ExternalHiveTask(luigi.ExternalTask):
    """
    External task that depends on a Hive table/partition.
    """
    database = luigi.Parameter(default='default')
    table = luigi.Parameter()
    partition = luigi.DictParameter(
        default={},
        description='Python dictionary specifying the target partition e.g. {"date": "2013-01-25"}'
    )

    def output(self):
        return HivePartitionTarget(
            database=self.database,
            table=self.table,
            partition=self.partition,
        )
