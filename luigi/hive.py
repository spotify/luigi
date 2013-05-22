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

import abc
import logging
import luigi
import luigi.hadoop
import luigi.interface
from luigi.target import FileSystemTarget, FileAlreadyExists
import os
import subprocess
import tempfile
from luigi.task import flatten

logger = logging.getLogger('luigi-interface')


class HiveCommandError(RuntimeError):
    def __init__(self, message, out=None, err=None):
        super(HiveCommandError, self).__init__(message, out, err)
        self.message = message
        self.out = out
        self.err = err


def load_hive_cmd():
    return luigi.interface.get_config().get('hive', 'command', 'hive')


def run_hive(args):
    """runs the `hive` from the command line, passing in the given args, and
       returning stdout"""
    cmd = [load_hive_cmd()] + args
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    if (p.returncode != 0):
        raise HiveCommandError("Hive command: {0} failed with error code: {1}".format(" ".join(cmd), p.returncode),
                               stdout, stderr)
    return stdout


def run_hive_cmd(hivecmd):
    """Runs the given hive query and returns stdout"""
    return run_hive(['-e', hivecmd])


def run_hive_script(script):
    """Runs the contents of the given script in hive and returns stdout"""
    if not os.path.isfile(script):
        raise RuntimeError("Hive script: {0} does not exist.".format(script))
    return run_hive(['-f', script])


class HiveClient(object):  # interface
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def table_location(self, table, database='default', partition={}):
        """
        Returns location of db.table (or db.table.partition). partition is a dict of partition key to
        value.
        """
        pass

    @abc.abstractmethod
    def table_schema(self, table, database='default'):
        """
        Returns list of [(name, type)] for each column in database.table
        """
        pass

    @abc.abstractmethod
    def table_exists(self, table, database='default', partition={}):
        """
        Returns true iff db.table (or db.table.partition) exists. partition is a dict of partition key to
        value.
        """
        pass

    @abc.abstractmethod
    def partition_spec(self, partition):
        """
        Turn a dict into a string partition specification
        """
        pass


class HiveCommandClient(HiveClient):
    '''
    Uses `hive` invocations to find information
    '''
    def table_location(self, table, database='default', partition={}):
        cmd = "use {0}; describe formatted {1}".format(database, table)
        if partition:
            cmd += " PARTITION ({0})".format(self.partition_spec(partition))

        stdout = run_hive_cmd(cmd)

        for line in stdout.split("\n"):
            if "Location:" in line:
                return line.split("\t")[1]

    def table_exists(self, table, database='default', partition={}):
        if not partition:
            stdout = run_hive_cmd('use {0}; describe {1}'.format(database, table))

            return not "does not exist" in stdout
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
        return [tuple(line.strip().split("\t")) for line in describe.strip().split("\n")]

    def partition_spec(self, partition):
        """
        Turns a dict into the a Hive partition specification string
        """
        return ','.join(["{0}='{1}'".format(k, v) for (k, v) in partition.items()])


class MetastoreClient(HiveClient):
    def table_location(self, table, database='default', partition={}):
        with HiveThriftContext() as client:
            if partition:
                partition_str = self.partition_spec(partition)
                thrift_table = client.get_partition_by_name(database, table, partition_str)
            else:
                thrift_table = client.get_table(database, table)
            return thrift_table.sd.location

    def table_exists(self, table, database='default', partition={}):
        with HiveThriftContext() as client:
            if not partition:
                return table in client.get_all_tables(database)
            else:
                partition_str = self.partition_spec(partition)
                # -1 is max_parts, the # of partition names to return (-1 = unlimited)
                return partition_str in client.get_partition_names(database, table, -1)

    def table_schema(self, table, database='default'):
        with HiveThriftContext() as client:
            return [(field_schema.name, field_schema.type) for field_schema in client.get_schema(database, table)]

    def partition_spec(self, partition):
        return "/".join("%s=%s" % (k, v) for (k, v) in partition.items())


class HiveThriftContext(object):
    '''
    Context manager for hive metastore client
    '''
    def __enter__(self):
        try:
            from thrift import Thrift
            from thrift.transport import TSocket
            from thrift.transport import TTransport
            from thrift.protocol import TBinaryProtocol
            from hive_metastore import ThriftHiveMetastore
            config = luigi.interface.get_config()
            host = config.get('hive', 'metastore_host')
            port = config.getint('hive', 'metastore_port')
            transport = TSocket.TSocket(host, port)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            transport.open()
            self.transport = transport
            return ThriftHiveMetastore.Client(protocol)
        except ImportError, e:
            raise Exception('Could not import Hive thrift library:' + str(e))

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.transport.close()


client = HiveCommandClient()
table_location = client.table_location
table_exists = client.table_exists
table_schema = client.table_schema
partition_spec = client.partition_spec


class HiveQueryTask(luigi.hadoop.BaseHadoopJobTask):
    ''' Task to run a hive query
    '''
    # by default, we let hive figure these out.
    n_reduce_tasks = None
    bytes_per_reducer = None
    reducers_max = None

    @abc.abstractmethod
    def query(self):
        ''' Text of query to run in hive
        '''
        raise RuntimeError("Must implement query!")

    def hiverc(self):
        ''' Location of an rc file to run before the query
        '''
        return None

    def hiveconfs(self):
        '''
        Returns an dict of key=value settings to be passed along
        to the hive command line via --hiveconf. By default, sets
        mapred.job.name to task_id and if not None, sets:
        * mapred.reduce.tasks (n_reduce_tasks)
        * mapred.fairscheduler.pool (pool)
        * hive.exec.reducers.bytes.per.reducer (bytes_per_reducer)
        * hive.exec.reducers.max (reducers_max)
        '''
        jcs = {}
        jcs['mapred.job.name'] = self.task_id
        if self.n_reduce_tasks is not None:
            jcs['mapred.reduce.tasks'] = self.n_reduce_tasks
        if self.pool is not None:
            jcs['mapred.fairscheduler.pool'] = self.pool
        if self.bytes_per_reducer is not None:
            jcs['hive.exec.reducers.bytes.per.reducer'] = self.bytes_per_reducer
        if self.reducers_max is not None:
            jcs['hive.exec.reducers.max'] = self.reducers_max
        return jcs

    def job_runner(self):
        return HiveQueryRunner()


class HiveQueryRunner(luigi.hadoop.JobRunner):
    ''' Runs a HiveQueryTask by shelling out to hive
    '''

    def prepare_outputs(self, job):
        """ Called before job is started

        If output is a `FileSystemTarget`, create parent directories so the hive command won't fail
        """
        outputs = flatten(job.output())
        for o in outputs:
            if isinstance(o, FileSystemTarget):
                parent_dir = os.path.dirname(o.path)
                if not o.fs.exists(parent_dir):
                    logger.info("Creating parent directory %r" % (parent_dir,))
                    try:
                        # there is a possible race condition
                        # which needs to be handled here
                        o.fs.mkdir(parent_dir)
                    except FileAlreadyExists:
                        pass

    def run_job(self, job):
        self.prepare_outputs(job)
        with tempfile.NamedTemporaryFile() as f:
            f.write(job.query())
            f.flush()
            arglist = [load_hive_cmd(), '-f', f.name]
            if job.hiverc():
                arglist += ['-i', job.hiverc()]
            if job.hiveconfs():
                for k, v in job.hiveconfs().iteritems():
                    arglist += ['--hiveconf', '{0}={1}'.format(k, v)]

            logger.info(arglist)
            luigi.hadoop.run_and_track_hadoop_job(arglist)


class HiveTableTarget(luigi.Target):
    ''' exists returns true if the table exists
    '''

    def __init__(self, table, database='default'):
        self.database = database
        self.table = table
        self.hive_cmd = load_hive_cmd()

    def exists(self):
        logger.debug("Checking Hive table '{d}.{t}' exists".format(d=self.database, t=self.table))
        return table_exists(self.table, self.database)

    @property
    def path(self):
        """Returns the path to this table in HDFS"""
        location = table_location(self.table, self.database)
        if not location:
            raise Exception("Couldn't find location for table: {0}".format(str(self)))
        return location

    def open(self, mode):
        return NotImplementedError("open() is not supported for HiveTableTarget")


class HivePartitionTarget(luigi.Target):
    ''' exists returns true if the table's partition exists
    '''

    def __init__(self, table, partition, database='default', fail_missing_table=True):
        self.database = database
        self.table = table
        self.partition = partition

        self.fail_missing_table = fail_missing_table

    def exists(self):
        try:
            logger.debug("Checking Hive table '{d}.{t}' for partition {p}".format(d=self.database, t=self.table, p=str(self.partition)))
            return table_exists(self.table, self.database, self.partition)
        except HiveCommandError, e:
            if self.fail_missing_table:
                raise
            else:
                if table_exists(self.table, self.database):
                    # a real error occured
                    raise
                else:
                    # oh the table just doesn't exist
                    return False

    @property
    def path(self):
        """Returns the path for this HiveTablePartitionTarget's data"""
        location = table_location(self.table, self.database, self.partition)
        if not location:
            raise Exception("Couldn't find location for table: {0}".format(str(self)))
        return location

    def open(self, mode):
        return NotImplementedError("open() is not supported for HivePartitionTarget")


class ExternalHiveTask(luigi.ExternalTask):
    ''' External task that depends on a Hive table/partition.
    '''

    database = luigi.Parameter(default='default')
    table = luigi.Parameter()
    # since this is an external task and will never be initialized from the CLI, partition can be any python object, in this case a dictionary
    partition = luigi.Parameter(default=None, description='Python dictionary specifying the target partition e.g. {"date": "2013-01-25"}')

    def output(self):
        if self.partition is not None:
            assert self.partition, "partition required"
            return HivePartitionTarget(database=self.database,
                                       table=self.table,
                                       partition=self.partition)
        else:
            return HiveTableTarget(self.database, self.table)
