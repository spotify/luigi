Configuration
=============

All configuration can be done by adding a configuration file named
client.cfg to your current working directory or /etc/luigi (although
this is further configurable). The config file is broken into sections,
each controlling a different part of the config. Example
/etc/luigi/client.cfg:

::

    [hadoop]
    version: cdh4
    streaming-jar: /usr/lib/hadoop-xyz/hadoop-streaming-xyz-123.jar

    [core]
    default-scheduler-host: luigi-host.mycompany.foo
    error-email: foo@bar.baz

By default, all parameters will be overridden by matching values in the
configuration file. For instance if you have a Task definition:

.. code:: python

    class DailyReport(luigi.hadoop.JobTask):
        date = luigi.DateParameter(default=datetime.date.today())
        # ...

Then you can override the default value for date by providing it in the
configuration:

::

    [DailyReport]
    date: 2012-01-01

You can also use ``config_path`` as an argument to the ``Parameter`` if
you want to use a specific section in the config.


Configurable options
--------------------

Luigi comes with a lot of configurable options. Below, we describe each
section and the parameters available within it.


[core]
------

These parameters control core luigi behavior, such as error e-mails and
interactions between the worker and scheduler.

default-scheduler-host
  Hostname of the machine running the scheduler. Defaults to localhost.

default-scheduler-port
  Port of the remote scheduler api process. Defaults to 8082.

email-prefix
  Optional prefix to add to the subject line of all e-mails. For
  example, setting this to "[LUIGI]" would change the subject line of an
  e-mail from "Luigi: Framework error" to "[LUIGI] Luigi: Framework
  error"

email-sender
  User name in from field of error e-mails.
  Default value: luigi-client@<server_name>

email-type
  Type of e-mail to send. Valid values are "plain" and "html". When set
  to html, tracebacks are wrapped in <pre> tags to get fixed-width font.
  Default value is plain.

error-email
  Recipient of all error e-mails. If this is not set, no error e-mails
  are sent when luigi crashes. If luigi is run from the command line, no
  e-mails will be sent unless output is redirected to a file.

hdfs-tmp-dir
  Base directory in which to store temporary files on hdfs. Defaults to
  tempfile.gettempdir()

history-filename
  If set, specifies a filename for Luigi to write stuff (currently just
  job id) to in mapreduce job's output directory. Useful in a
  configuration where no history is stored in the output directory by
  Hadoop.

logging_conf_file
  Location of the logging configuration file.

max-reschedules
  The maximum number of times that a job can be automatically
  rescheduled by a worker before it will stop trying. Workers will
  reschedule a job if it is found to not be done when attempting to run
  a dependent job. This defaults to 1.

max-shown-tasks
  .. versionadded:: 1.0.20

  The maximum number of tasks returned in a task_list api call. This
  will restrict the number of tasks shown in any section in the
  visualiser. Small values can alleviate frozen browsers when there are
  too many done tasks. This defaults to 100000 (one hundred thousand).

no_configure_logging
  If true, logging is not configured. Defaults to false.

parallel-scheduling
  If true, the scheduler will compute complete functions of tasks in
  parallel using multiprocessing. This can significantly speed up
  scheduling, but requires that all tasks can be pickled.

retry-external-tasks
  If true, incomplete external tasks (i.e. tasks where the `run()` method is
  NotImplemented) will be retested for completion while Luigi is running.
  This means that if external dependencies are satisfied after a workflow has
  started, any tasks dependent on that resource will be eligible for running.
  Note: Every time the task remains incomplete, it will count as FAILED, so
  normal retry logic applies (see: `disable-num-failures` and `retry-delay`).
  This setting works best with `worker-keep-alive: true`.
  If false, external tasks will only be evaluated when Luigi is first invoked.
  In this case, Luigi will not check whether external dependencies are
  satisfied  while a workflow is in progress, so dependent tasks will remain
  PENDING until the workflow is reinvoked.
  Defaults to false for backwards compatibility.

rpc-connect-timeout
  Number of seconds to wait before timing out when making an API call.
  Defaults to 10.0

smtp_host
  Hostname for sending mail throug smtp. Defaults to localhost.

smtp_local_hostname
  If specified, overrides the FQDN of localhost in the HELO/EHLO
  command.

smtp_login
  Username to log in to your smtp server, if necessary.

smtp_password
  Password to log in to your smtp server. Must be specified for
  smtp_login to have an effect.

smtp_port
  Port number for smtp on smtp_host. Defaults to 0.

smtp_ssl
  If true, connects to smtp through SSL. Defaults to false.

smtp_timeout
  Optionally sets the number of seconds after which smtp attempts should
  time out.

tmp-dir
  DEPRECATED - use hdfs-tmp-dir instead

worker-count-uniques
  If true, workers will only count unique pending jobs when deciding
  whether to stay alive. So if a worker can't get a job to run and other
  workers are waiting on all of its pending jobs, the worker will die.
  worker-keep-alive must be true for this to have any effect. Defaults
  to false.

worker-keep-alive
  If true, workers will stay alive when they run out of jobs to run, as
  long as they have some pending job waiting to be run. Defaults to
  false.

worker-ping-interval
  Number of seconds to wait between pinging scheduler to let it know
  that the worker is still alive. Defaults to 1.0.

worker-task-limit
  .. versionadded:: 1.0.25

  Maximum number of tasks to schedule per invocation. Upon exceeding it,
  the worker will issue a warning and proceed with the workflow obtained
  thus far. Prevents incidents due to spamming of the scheduler, usually
  accidental. Default: no limit.

worker-timeout
  .. versionadded:: 1.0.20

  Number of seconds after which to kill a task which has been running
  for too long. This provides a default value for all tasks, which can
  be overridden by setting the worker-timeout property in any task. This
  only works when using multiple workers, as the timeout is implemented
  by killing worker subprocesses. Default value is 0, meaning no
  timeout.

worker-wait-interval
  Number of seconds for the worker to wait before asking the scheduler
  for another job after the scheduler has said that it does not have any
  available jobs.


[elasticsearch]
---------------

These parameters control use of elasticsearch

marker-index
  Defaults to "update_log".

marker-doc-type
  Defaults to "entry".


[email]
-------

These parameters control sending error e-mails through Amazon SES.

AWS_ACCESS_KEY
  Your AWS access key

AWS_SECRET_KEY
  Your AWS secret key

region
  Your AWS region. Defaults to us-east-1.

type
  If set to "ses", error e-mails will be send through Amazon SES.
  Otherwise, e-mails are sent via smtp.


[hadoop]
--------

Parameters controlling basic hadoop tasks

command
  Name of command for running hadoop from the command line. Defaults to
  "hadoop"

python-executable
  Name of command for running python from the command line. Defaults to
  "python"

scheduler
  Type of scheduler to use when scheduling hadoop jobs. Can be "fair" or
  "capacity". Defaults to "fair".

streaming-jar
  Path to your streaming jar. Must be specified to run streaming jobs.

version
  Version of hadoop used in your cluster. Can be "cdh3", "chd4", or
  "apache1". Defaults to "cdh4".


[hdfs]
------

Parameters controlling the use of snakebite to speed up hdfs queries.

client
  Client to use for most hadoop commands. Options are "snakebite",
  "snakebite_with_hadoopcli_fallback", and "hadoopcli". Snakebite is
  much faster, so use of it is encouraged. Using snakebite requires it
  to be installed separately on the machine. Defaults to "hadoopcli".

client_version
  Optionally specifies hadoop client version for snakebite.

effective_user
  Optionally specifies the effective user for snakebite.

namenode_host
  The hostname of the namenode. Needed for snakebite if
  snakebite_autoconfig is not set.

namenode_port
  The port used by snakebite on the namenode. Needed for snakebite if
  snakebite_autoconfig is not set.

snakebite_autoconfig
  If true, attempts to automatically detect the host and port of the
  namenode for snakebite queries. Defaults to false.


[hive]
------

Parameters controlling hive tasks

command
  Name of the command used to run hive on the command line. Defaults to
  "hive".

hiverc-location
  Optional path to hive rc file.

metastore_host
  Hostname for metastore.

metastore_port
  Port for hive to connect to metastore host.

release
  If set to "apache", uses a hive client that better handles apache
  hive output. All other values use the standard client Defaults to
  "cdh4".


[mysql]
-------

Parameters controlling use of MySQL targets

marker-table
  Table in which to store status of table updates. This table will be
  created if it doesn't already exist. Defaults to "table_updates".


[postgres]
----------

Parameters controlling the use of Postgres targets

local-tmp-dir
  Directory in which to temporarily store data before writing to
  postgres. Uses system default if not specified.

marker-table
  Table in which to store status of table updates. This table will be
  created if it doesn't already exist. Defaults to "table_updates".


[redshift]
----------

Parameters controlling the use of Redshift targets

marker-table
  Table in which to store status of table updates. This table will be
  created if it doesn't already exist. Defaults to "table_updates".


[resources]
-----------

This section can contain arbitrary keys. Each of these specifies the
amount of a global resource that the scheduler can allow workers to use.
The scheduler will prevent running jobs with resources specified from
exceeding the counts in this section. Unspecified resources are assumed
to have limit 1. Example resources section for a configuration with 2
hive resources and 1 mysql resource:

::

  [resources]
  hive: 2
  mysql: 1

Note that it was not necessary to specify the 1 for mysql here, but it
is good practice to do so when you have a fixed set of resources.


[scalding]
----------

Parameters controlling running of scalding jobs

scala-home
  Home directory for scala on your machine. Defaults to either
  SCALA_HOME or /usr/share/scala if SCALA_HOME is unset.

scalding-home
  Home directory for scalding on your machine. Defaults to either
  SCALDING_HOME or /usr/share/scalding if SCALDING_HOME is unset.

scalding-provided
  Provided directory for scalding on your machine. Defaults to either
  SCALDING_HOME/provided or /usr/share/scalding/provided

scalding-libjars
  Libjars directory for scalding on your machine. Defaults to either
  SCALDING_HOME/libjars or /usr/share/scalding/libjars


.. _scheduler-config:

[scheduler]
-----------

Parameters controlling scheduler behavior

disable-num-failures
  Number of times a task can fail within disable-window-seconds before
  the scheduler will automatically disable it. If not set, the scheduler
  will not automatically disable jobs.

disable-persist-seconds
  Number of seconds for which an automatic scheduler disable lasts.
  Defaults to 86400 (1 day).

disable-window-seconds
  Number of seconds during which disable-num-failures failures must
  occur in order for an automatic disable by the scheduler. The
  scheduler forgets about disables that have occurred longer ago than
  this amount of time. Defaults to 3600 (1 hour).

record_task_history
  If true, stores task history in a database. Defaults to false.

remove-delay
  Number of seconds to wait before removing a task that has no
  stakeholders. Defaults to 600 (10 minutes).

retry-delay
  Number of seconds to wait after a task failure to mark it pending
  again. Defaults to 900 (15 minutes).

state-path
  Path in which to store the luigi scheduler's state. When the scheduler
  is shut down, its state is stored in this path. The scheduler must be
  shut down cleanly for this to work, usually with a kill command. If
  the kill command includes the -9 flag, the scheduler will not be able
  to save its state. When the scheduler is started, it will load the
  state from this path if it exists. This will restore all scheduled
  jobs and other state from when the scheduler last shut down.

  Sometimes this path must be deleted when restarting the scheduler
  after upgrading luigi, as old state files can become incompatible
  with the new scheduler. When this happens, all workers should be
  restarted after the scheduler both to become compatible with the
  updated code and to reschedule the jobs that the scheduler has now
  forgotten about.

  This defaults to /var/lib/luigi-server/state.pickle

worker-disconnect-delay
  Number of seconds to wait after a worker has stopped pinging the
  scheduler before removing it and marking all of its running tasks as
  failed. Defaults to 60.


[spark]
-------

Parameters controlling the default execution of :py:class:`~luigi.contrib.spark.SparkSubmitTask` and :py:class:`~luigi.contrib.spark.PySparkTask`:

.. deprecated:: 1.1.1
   :py:class:`~luigi.contrib.spark.SparkJob`, :py:class:`~luigi.contrib.spark.Spark1xJob` and :py:class:`~luigi.contrib.spark.PySpark1xJob`
    are deprecated. Please use :py:class:`~luigi.contrib.spark.SparkSubmitTask` or :py:class:`~luigi.contrib.spark.PySparkTask`.

spark-submit
  Command to run in order to submit spark jobs. Default: spark-submit

master
  Master url to use for spark-submit. Example: local[*], spark://masterhost:7077. Default: Spark default (Prior to 1.1.1: yarn-client)

deploy-mode
    Whether to launch the driver programs locally ("client") or on one of the worker machines inside the cluster ("cluster"). Default: Spark default

jars
    Comma-separated list of local jars to include on the driver and executor classpaths. Default: Spark default

py-files
    Comma-separated list of .zip, .egg, or .py files to place on the PYTHONPATH for Python apps. Default: Spark default

files
    Comma-separated list of files to be placed in the working directory of each executor. Default: Spark default

conf:
    Arbitrary Spark configuration property in the form Prop=Value|Prop2=Value2. Default: Spark default

properties-file
    Path to a file from which to load extra properties. Default: Spark default

driver-memory
    Memory for driver (e.g. 1000M, 2G). Default: Spark default

driver-java-options
    Extra Java options to pass to the driver. Default: Spark default

driver-library-path
    Extra library path entries to pass to the driver. Default: Spark default

driver-class-path
    Extra class path entries to pass to the driver. Default: Spark default

executor-memory
    Memory per executor (e.g. 1000M, 2G). Default: Spark default

*Configuration for Spark submit jobs on Spark standalone with cluster deploy mode only:*

driver-cores
    Cores for driver. Default: Spark default

supervise
    If given, restarts the driver on failure. Default: Spark default

*Configuration for Spark submit jobs on Spark standalone and Mesos only:*

total-executor-cores
    Total cores for all executors. Default: Spark default

*Configuration for Spark submit jobs on YARN only:*

executor-cores
    Number of cores per executor. Default: Spark default

queue
    The YARN queue to submit to. Default: Spark default

num-executors
    Number of executors to launch. Default: Spark default

archives
    Comma separated list of archives to be extracted into the working directory of each executor. Default: Spark default

hadoop-conf-dir
  Location of the hadoop conf dir. Sets HADOOP_CONF_DIR environment variable
  when running spark. Example: /etc/hadoop/conf

*Extra configuration for PySparkTask jobs:*

py-packages
    Comma-separated list of local packages (in your python path) to be distributed to the cluster.

*Parameters controlling the execution of SparkJob jobs (deprecated):*

spark-jar
  Location of the spark jar. Sets SPARK_JAR environment variable when
  running spark. Example:
  /usr/share/spark/jars/spark-assembly-0.8.1-incubating-hadoop2.2.0.jar

spark-class
  Location of script to invoke. Example: /usr/share/spark/spark-class


[task_history]
--------------

Parameters controlling storage of task history in a database

db_connection
  Connection string for connecting to the task history db using
  sqlalchemy.
