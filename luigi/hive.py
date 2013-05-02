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
import os
import subprocess
import tempfile

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


def table_location(db, table, partition_spec=None):
    """
    Returns the location of a Hive table/partitions, or None.
    partition_spec should be a string returned by partition_spec()
    """
    cmd = "use {0}; describe formatted {1}".format(db, table)
    if partition_spec:
        cmd += " PARTITION ({0})".format(partition_spec)

    stdout = run_hive_cmd(cmd)

    for line in stdout.split("\n"):
        if "Location:" in line:
            return line.split("\t")[1]


def partition_spec(partition):
    """
    Turns a dict into the a Hive partition specification string
    """
    return ','.join(["{0}='{1}'".format(k, v) for (k, v) in partition.items()])


class HiveQueryTask(luigi.hadoop.BaseHadoopJobTask):
    ''' Task to run a hive query
    '''

    @abc.abstractmethod
    def query(self):
        ''' Text of query to run in hive
        '''
        raise RuntimeError("Must implement query!")

    def hiverc(self):
        ''' Location of an rc file to run before the query
        '''
        return None

    def job_runner(self):
        return HiveQueryRunner()


class HiveQueryRunner(luigi.hadoop.JobRunner):
    ''' Runs a HiveQueryTask by shelling out to hive
    '''

    def run_job(self, job):
        with tempfile.NamedTemporaryFile() as f:
            f.write(job.query())
            f.flush()
            arglist = [load_hive_cmd(), '-f', f.name]
            if job.hiverc():
                arglist += ['-i', job.hiverc()]

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
        stdout = run_hive_cmd('use {0}; describe {1}'.format(self.database, self.table))

        return not "does not exist" in stdout

    @property
    def path(self):
        """Returns the path to this table in HDFS"""
        location = table_location(self.database, self.table)
        if not location:
            raise Exception("Couldn't find location for table: {0}".format(str(self)))
        return location

    def open(self, mode):
        return NotImplementedError("open() is not supported for HiveTableTarget")


class HivePartitionTarget(luigi.Target):
    ''' exists returns true if the table's partition exists
    '''

    def __init__(self, table, partition, database='default'):
        self.database = database
        self.table = table
        # change partitions to the way hive expects them
        self.partition_str = partition_spec(partition)

    def exists(self):
        stdout = run_hive_cmd("""use {self.database}; show partitions {self.table} partition
                            ({self.partition_str})""".format(self=self))

        if stdout:
            return True
        else:
            return False

    @property
    def path(self):
        """Returns the path for this HiveTablePartitionTarget's data"""
        location = table_location(self.database, self.table, self.partition_str)
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
