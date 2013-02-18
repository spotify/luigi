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

import logging
import luigi
import luigi.interface
import subprocess
import tempfile

from luigi.hadoop import JobRunner, HadoopJobRunner, BaseHadoopJobTask

logger = logging.getLogger('luigi-interface')


def load_hive_cmd():
  return luigi.interface.get_config().get('hive', 'command', 'hive')


def run_hive_cmd(hivecmd):
  cmd = [load_hive_cmd(), '-e', hivecmd]
  p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  stdout, _ = p.communicate()
  return stdout


class HiveQueryTask(BaseHadoopJobTask):
  ''' Task to run a hive query
  '''

  def query(self):
    ''' Text of query to run in hive
    '''
    raise RuntimeError("Must implement query!")

  def hiverc(self):
    ''' Location of an rc file to run before the query
    '''
    None

  def job_runner(self):
    return LocalHiveQueryRunner()


class LocalHiveQueryRunner(JobRunner):
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
      HadoopJobRunner.run_and_track_hadoop_job(arglist)


class HiveTableTarget(luigi.Target):
  ''' exists returns true if the table exists
  '''

  def __init__(self, db, table):
    self.db = db
    self.table = table
    self.hive_cmd = load_hive_cmd()

  def exists(self):
    stdout = run_hive_cmd('use {0}; describe {1}'.format(self.db, self.table))

    return not "does not exist" in stdout

  def path(self):
    """Returns the path to this table in HDFS"""
    stdout = run_hive_cmd("use {self.db}; describe formatted {self.table}".format(self=self))

    for line in stdout.split("\n"):
      if "Location:" in line:
        return line.split("\t")[1]

    raise Exception("Couldn't find location for table: {0}".format(str(self)))


class HiveTablePartitionTarget(luigi.Target):
  ''' exists returns true if the table's partition exists
  '''

  def __init__(self, db, table, partitions):
    self.db = db
    self.table = table
    # change partitions to the way hive expects them
    self.partition_str = ','.join(["{0}='{1}'".format(k, v) for (k, v) in partitions.items()])

  def exists(self):
    stdout = run_hive_cmd("""use {self.db}; show partitions {self.table} partition
({self.partition_str})""".format(self=self))

    if stdout:
      return True
    else:
      return False

  def path(self):
    """Returns the path for this HiveTablePartitionTarget's data"""
    stdout = run_hive_cmd("use {self.db}; describe formatted {self.table} PARTITION ({self.partition_str})".format(self=self))

    for line in stdout.split("\n"):
      if "Location:" in line:
        return line.split("\t")[1]

    raise Exception("Couldn't find location for table: {0}".format(str(self)))


class HiveTableTask(luigi.ExternalTask):
  ''' External task that depends on a Hive table/partition.
  '''

  db = luigi.Parameter()
  table = luigi.Parameter()
  partitions = luigi.Parameter(default=None, description='Key value list of partitions: key1=value1;key2=value2;...')
  partition_name = luigi.Parameter(default=None, description='Single partition name. Cannot be used with partitions')
  partition = luigi.Parameter(default=None, description='Single partition value')

  def output(self):
    if self.partition_name:
      assert self.partition, "partition required"
      assert not self.partitions, 'cannot have partitions and partition options'
      return HiveTablePartitionTarget(self.db, self.table, {self.partition_name: self.partition})
    elif self.partitions:
      splitByEquals = lambda a: a.split('=')
      arr = map(splitByEquals, self.partitions.split(';'))
      partitions = dict((a[0], a[1]) for a in arr)
      return HiveTablePartitionTarget(self.db, self.table, partitions)
    else:
      return HiveTableTarget(self.db, self.table)
