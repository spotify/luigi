Configuration
=============

All configuration can be done by adding configuration files.

Supported config parsers:

* ``cfg`` (default), based on Python's standard ConfigParser_. Values may refer to environment variables using ``${ENVVAR}`` syntax.
* ``toml``

.. _ConfigParser: https://docs.python.org/3/library/configparser.html

You can choose right parser via ``LUIGI_CONFIG_PARSER`` environment variable. For example, ``LUIGI_CONFIG_PARSER=toml``.

Default (cfg) parser are looked for in:

* ``/etc/luigi/client.cfg`` (deprecated)
* ``/etc/luigi/luigi.cfg``
* ``client.cfg`` (deprecated)
* ``luigi.cfg``
* ``LUIGI_CONFIG_PATH`` environment variable

`TOML <https://github.com/toml-lang/toml>`_ parser are looked for in:

* ``/etc/luigi/luigi.toml``
* ``luigi.toml``
* ``LUIGI_CONFIG_PATH`` environment variable

Both config lists increase in priority (from low to high). The order only
matters in case of key conflicts (see docs for ConfigParser.read_).
These files are meant for both the client and ``luigid``.
If you decide to specify your own configuration you should make sure
that both the client and ``luigid`` load it properly.

.. _ConfigParser.read: https://docs.python.org/3.6/library/configparser.html#configparser.ConfigParser.read

The config file is broken into sections, each controlling a different part of the config.

Example cfg config:

.. code:: ini

    [hadoop]
    version=cdh4
    streaming_jar=/usr/lib/hadoop-xyz/hadoop-streaming-xyz-123.jar

    [core]
    scheduler_host=luigi-host.mycompany.foo

Example toml config:

.. code:: python

    [hadoop]
    version = "cdh4"
    streaming_jar = "/usr/lib/hadoop-xyz/hadoop-streaming-xyz-123.jar"

    [core]
    scheduler_host = "luigi-host.mycompany.foo"

Also see `examples/config.toml
<https://github.com/spotify/luigi/blob/master/examples/config.toml>`_
for more complex example.

.. _ParamConfigIngestion:

Parameters from config Ingestion
--------------------------------

All parameters can be overridden from configuration files. For instance if you
have a Task definition:

.. code:: python

    class DailyReport(luigi.contrib.hadoop.JobTask):
        date = luigi.DateParameter(default=datetime.date.today())
        # ...

Then you can override the default value for ``DailyReport().date`` by providing
it in the configuration:

.. code:: ini

    [DailyReport]
    date=2012-01-01

.. _ConfigClasses:

Configuration classes
*********************

Using the :ref:`ParamConfigIngestion` method, we derive the
conventional way to do global configuration. Imagine this configuration.

.. code:: ini

    [mysection]
    option=hello
    intoption=123


We can create a :py:class:`~luigi.Config` class:

.. code:: python

    import luigi

    # Config classes should be camel cased
    class mysection(luigi.Config):
        option = luigi.Parameter(default='world')
        intoption = luigi.IntParameter(default=555)

    mysection().option
    mysection().intoption


Configurable options
--------------------

Luigi comes with a lot of configurable options. Below, we describe each
section and the parameters available within it.


[core]
------

These parameters control core Luigi behavior, such as error e-mails and
interactions between the worker and scheduler.

autoload_range
  .. versionadded:: 2.8.11

  If false, prevents range tasks from autoloading. They can still be loaded
  using ``--module luigi.tools.range``. Defaults to true. Setting this to true
  explicitly disables the deprecation warning.

default_scheduler_host
  Hostname of the machine running the scheduler. Defaults to localhost.

default_scheduler_port
  Port of the remote scheduler api process. Defaults to 8082.

default_scheduler_url
  Full path to remote scheduler. Defaults to ``http://localhost:8082/``.
  For TLS support use the URL scheme: ``https``,
  example: ``https://luigi.example.com:443/``
  (Note: you will have to terminate TLS using an HTTP proxy)
  You can also use this to connect to a local Unix socket using the
  non-standard URI scheme: ``http+unix``
  example: ``http+unix://%2Fvar%2Frun%2Fluigid%2Fluigid.sock/``

hdfs_tmp_dir
  Base directory in which to store temporary files on hdfs. Defaults to
  tempfile.gettempdir()

history_filename
  If set, specifies a filename for Luigi to write stuff (currently just
  job id) to in mapreduce job's output directory. Useful in a
  configuration where no history is stored in the output directory by
  Hadoop.

log_level
  The default log level to use when no logging_conf_file is set. Must be
  a valid name of a `Python log level
  <https://docs.python.org/2/library/logging.html#logging-levels>`_.
  Default is ``DEBUG``.

logging_conf_file
  Location of the logging configuration file.

max_shown_tasks
  .. versionadded:: 1.0.20

  The maximum number of tasks returned in a task_list api call. This
  will restrict the number of tasks shown in task lists in the
  visualiser. Small values can alleviate frozen browsers when there are
  too many done tasks. This defaults to 100000 (one hundred thousand).

max_graph_nodes
  .. versionadded:: 2.0.0

  The maximum number of nodes returned by a dep_graph or
  inverse_dep_graph api call. Small values can greatly speed up graph
  display in the visualiser by limiting the number of nodes shown. Some
  of the nodes that are not sent to the visualiser will still show up as
  dependencies of nodes that were sent. These nodes are given TRUNCATED
  status.

no_configure_logging
  If true, logging is not configured. Defaults to false.

parallel_scheduling
  If true, the scheduler will compute complete functions of tasks in
  parallel using multiprocessing. This can significantly speed up
  scheduling, but requires that all tasks can be pickled.
  Defaults to false.

parallel_scheduling_processes
  The number of processes to use for parallel scheduling. If not specified
  the default number of processes will be the total number of CPUs available.

rpc_connect_timeout
  Number of seconds to wait before timing out when making an API call.
  Defaults to 10.0

rpc_retry_attempts
  The maximum number of retries to connect the central scheduler before giving up.
  Defaults to 3

rpc_retry_wait
  Number of seconds to wait before the next attempt will be started to
  connect to the central scheduler between two retry attempts.
  Defaults to 30


[cors]
------

.. versionadded:: 2.8.0

These parameters control ``/api/<method>`` ``CORS`` behaviour (see: `W3C Cross-Origin Resource Sharing
<http://www.w3.org/TR/cors/>`_).

enabled
  Enables CORS support.
  Defaults to false.

allowed_origins
  A list of allowed origins. Used only if ``allow_any_origin`` is false.
  Configure in JSON array format, e.g. ["foo", "bar"].
  Defaults to empty.

allow_any_origin
  Accepts requests from any origin.
  Defaults to false.

allow_null_origin
  Allows the request to set ``null`` value of the ``Origin`` header.
  Defaults to false.

max_age
  Content of ``Access-Control-Max-Age``.
  Defaults to 86400 (24 hours).

allowed_methods
  Content of ``Access-Control-Allow-Methods``.
  Defaults to ``GET, OPTIONS``.

allowed_headers
  Content of ``Access-Control-Allow-Headers``.
  Defaults to ``Accept, Content-Type, Origin``.

exposed_headers
  Content of ``Access-Control-Expose-Headers``.
  Defaults to empty string (will NOT be sent as a response header).

allow_credentials
  Indicates that the actual request can include user credentials.
  Defaults to false.

.. _worker-config:

[worker]
--------

These parameters control Luigi worker behavior.

count_uniques
  If true, workers will only count unique pending jobs when deciding
  whether to stay alive. So if a worker can't get a job to run and other
  workers are waiting on all of its pending jobs, the worker will die.
  ``worker_keep_alive`` must be ``true`` for this to have any effect. Defaults
  to false.

keep_alive
  If true, workers will stay alive when they run out of jobs to run, as
  long as they have some pending job waiting to be run. Defaults to
  false.

ping_interval
  Number of seconds to wait between pinging scheduler to let it know
  that the worker is still alive. Defaults to 1.0.

task_limit
  .. versionadded:: 1.0.25

  Maximum number of tasks to schedule per invocation. Upon exceeding it,
  the worker will issue a warning and proceed with the workflow obtained
  thus far. Prevents incidents due to spamming of the scheduler, usually
  accidental. Default: no limit.

timeout
  .. versionadded:: 1.0.20

  Number of seconds after which to kill a task which has been running
  for too long. This provides a default value for all tasks, which can
  be overridden by setting the ``worker_timeout`` property in any task.
  Default value is 0, meaning no timeout.

wait_interval
  Number of seconds for the worker to wait before asking the scheduler
  for another job after the scheduler has said that it does not have any
  available jobs.

wait_jitter
  Size of jitter to add to the worker wait interval such that the multiple
  workers do not ask the scheduler for another job at the same time.
  Default: 5.0

max_keep_alive_idle_duration
  .. versionadded:: 2.8.4

  Maximum duration to keep worker alive while in idle state.
  Default: 0 (Indefinitely)

max_reschedules
  The maximum number of times that a job can be automatically
  rescheduled by a worker before it will stop trying. Workers will
  reschedule a job if it is found to not be done when attempting to run
  a dependent job. This defaults to 1.

retry_external_tasks
  If true, incomplete external tasks (i.e. tasks where the ``run()`` method is
  NotImplemented) will be retested for completion while Luigi is running.
  This means that if external dependencies are satisfied after a workflow has
  started, any tasks dependent on that resource will be eligible for running.
  Note: Every time the task remains incomplete, it will count as FAILED, so
  normal retry logic applies (see: ``retry_count`` and ``retry_delay``).
  This setting works best with ``worker_keep_alive: true``.
  If false, external tasks will only be evaluated when Luigi is first invoked.
  In this case, Luigi will not check whether external dependencies are
  satisfied  while a workflow is in progress, so dependent tasks will remain
  PENDING until the workflow is reinvoked.
  Defaults to false for backwards compatibility.

no_install_shutdown_handler
  By default, workers will stop requesting new work and finish running
  pending tasks after receiving a ``SIGUSR1`` signal. This provides a hook
  for gracefully shutting down workers that are in the process of running
  (potentially expensive) tasks. If set to true, Luigi will NOT install
  this shutdown hook on workers. Note this hook does not work on Windows
  operating systems, or when jobs are launched outside the main execution
  thread.
  Defaults to false.

send_failure_email
  Controls whether the worker will send e-mails on task and scheduling
  failures. If set to false, workers will only send e-mails on
  framework errors during scheduling and all other e-mail must be
  handled by the scheduler.
  Defaults to true.

check_unfulfilled_deps
  If true, the worker checks for completeness of dependencies before running a
  task. In case unfulfilled dependencies are detected, an exception is raised
  and the task will not run. This mechanism is useful to detect situations
  where tasks do not create their outputs properly, or when targets were
  removed after the dependency tree was built. It is recommended to disable
  this feature only when the completeness checks are known to be bottlenecks,
  e.g. when the ``exists()`` calls of the dependencies' outputs are
  resource-intensive.
  Defaults to true.

force_multiprocessing
  By default, luigi uses multiprocessing when *more than one* worker process is
  requested. When set to true, multiprocessing is used independent of the
  number of workers.
  Defaults to false.

check_complete_on_run
  By default, luigi tasks are marked as 'done' when they finish running without
  raising an error. When set to true, tasks will also verify that their outputs
  exist when they finish running, and will fail immediately if the outputs are
  missing.
  Defaults to false.


[elasticsearch]
---------------

These parameters control use of elasticsearch

marker_index
  Defaults to "update_log".

marker_doc_type
  Defaults to "entry".


[email]
-------

General parameters

force_send
  If true, e-mails are sent in all run configurations (even if stdout is
  connected to a tty device).  Defaults to False.

format
  Type of e-mail to send. Valid values are "plain", "html" and "none".
  When set to html, tracebacks are wrapped in <pre> tags to get fixed-
  width font. When set to none, no e-mails will be sent.

  Default value is plain.

method
  Valid values are "smtp", "sendgrid", "ses" and "sns". SES and SNS are
  services of Amazon web services. SendGrid is an email delivery service.
  The default value is "smtp".

  In order to send messages through Amazon SNS or SES set up your AWS
  config files or run Luigi on an EC2 instance with proper instance
  profile.

  In order to use sendgrid, fill in your sendgrid API key in the
  `[sendgrid]`_ section.

  In order to use smtp, fill in the appropriate fields in the `[smtp]`_
  section.

prefix
  Optional prefix to add to the subject line of all e-mails. For
  example, setting this to "[LUIGI]" would change the subject line of an
  e-mail from "Luigi: Framework error" to "[LUIGI] Luigi: Framework
  error"

receiver
  Recipient of all error e-mails. If this is not set, no error e-mails
  are sent when Luigi crashes unless the crashed job has owners set. If
  Luigi is run from the command line, no e-mails will be sent unless
  output is redirected to a file.

  Set it to SNS Topic ARN if you want to receive notifications through
  Amazon SNS. Make sure to set method to sns in this case too.

sender
  User name in from field of error e-mails.
  Default value: luigi-client@<server_name>


[batch_notifier]
----------------

Parameters controlling the contents of batch notifications sent from the
scheduler

email_interval
  Number of minutes between e-mail sends. Making this larger results in
  fewer, bigger e-mails.
  Defaults to 60.

batch_mode
  Controls how tasks are grouped together in the e-mail. Suppose we have
  the following sequence of failures:

  1. TaskA(a=1, b=1)
  2. TaskA(a=1, b=1)
  3. TaskA(a=2, b=1)
  4. TaskA(a=1, b=2)
  5. TaskB(a=1, b=1)

  For any setting of batch_mode, the batch e-mail will record 5 failures
  and mention them in the subject. The difference is in how they will
  be displayed in the body. Here are example bodies with error_messages
  set to 0.

  "all" only groups together failures for the exact same task:

  - TaskA(a=1, b=1) (2 failures)
  - TaskA(a=1, b=2) (1 failure)
  - TaskA(a=2, b=1) (1 failure)
  - TaskB(a=1, b=1) (1 failure)

  "family" groups together failures for tasks of the same family:

  - TaskA (4 failures)
  - TaskB (1 failure)

  "unbatched_params" groups together tasks that look the same after
  removing batched parameters. So if TaskA has a batch_method set for
  parameter a, we get the following:

  - TaskA(b=1) (3 failures)
  - TaskA(b=2) (1 failure)
  - TaskB(a=1, b=2) (1 failure)

  Defaults to "unbatched_params", which is identical to "all" if you are
  not using batched parameters.

error_lines
  Number of lines to include from each error message in the batch
  e-mail. This can be used to keep e-mails shorter while preserving the
  more useful information usually found near the bottom of stack traces.
  This can be set to 0 to include all lines. If you don't wish to see
  error messages, instead set ``error_messages`` to 0.
  Defaults to 20.

error_messages
  Number of messages to preserve for each task group. As most tasks that
  fail repeatedly do so for similar reasons each time, it's not usually
  necessary to keep every message. This controls how many messages are
  kept for each task or task group. The most recent error messages are
  kept. Set to 0 to not include error messages in the e-mails.
  Defaults to 1.

group_by_error_messages
  Quite often, a system or cluster failure will cause many disparate
  task types to fail for the same reason. This can cause a lot of noise
  in the batch e-mails. This cuts down on the noise by listing items
  with identical error messages together. Error messages are compared
  after limiting by ``error_lines``.
  Defaults to true.


[hadoop]
--------

Parameters controlling basic hadoop tasks

command
  Name of command for running hadoop from the command line. Defaults to
  "hadoop"

python_executable
  Name of command for running python from the command line. Defaults to
  "python"

scheduler
  Type of scheduler to use when scheduling hadoop jobs. Can be "fair" or
  "capacity". Defaults to "fair".

streaming_jar
  Path to your streaming jar. Must be specified to run streaming jobs.

version
  Version of hadoop used in your cluster. Can be "cdh3", "chd4", or
  "apache1". Defaults to "cdh4".


[hdfs]
------

Parameters controlling the use of snakebite to speed up hdfs queries.

client
  Client to use for most hadoop commands. Options are "snakebite",
  "snakebite_with_hadoopcli_fallback", "webhdfs" and "hadoopcli". Snakebite is
  much faster, so use of it is encouraged. webhdfs is fast and works with
  Python 3 as well, but has not been used that much in the wild.
  Both snakebite and webhdfs requires you to install it separately on
  the machine. Defaults to "hadoopcli".

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

tmp_dir
  Path to where Luigi will put temporary files on hdfs


[hive]
------

Parameters controlling hive tasks

command
  Name of the command used to run hive on the command line. Defaults to
  "hive".

hiverc_location
  Optional path to hive rc file.

metastore_host
  Hostname for metastore.

metastore_port
  Port for hive to connect to metastore host.

release
  If set to "apache", uses a hive client that better handles apache
  hive output. All other values use the standard client Defaults to
  "cdh4".


[kubernetes]
------------

Parameters controlling Kubernetes Job Tasks

auth_method
  Authorization method to access the cluster.
  Options are "kubeconfig_" or "service-account_"

kubeconfig_path
  Path to kubeconfig file, for cluster authentication.
  It defaults to ``~/.kube/config``, which is the default location when
  using minikube_.
  When auth_method is "service-account" this property is ignored.

max_retrials
  Maximum number of retrials in case of job failure.

.. _service-account: http://kubernetes.io/docs/user-guide/kubeconfig-file
.. _kubeconfig: http://kubernetes.io/docs/user-guide/service-accounts
.. _minikube: http://kubernetes.io/docs/getting-started-guides/minikube


[mysql]
-------

Parameters controlling use of MySQL targets

marker_table
  Table in which to store status of table updates. This table will be
  created if it doesn't already exist. Defaults to "table_updates".


[postgres]
----------

Parameters controlling the use of Postgres targets

local_tmp_dir
  Directory in which to temporarily store data before writing to
  postgres. Uses system default if not specified.

marker_table
  Table in which to store status of table updates. This table will be
  created if it doesn't already exist. Defaults to "table_updates".


[redshift]
----------

Parameters controlling the use of Redshift targets

marker_table
  Table in which to store status of table updates. This table will be
  created if it doesn't already exist. Defaults to "table_updates".

.. _resources-config:

[resources]
-----------

This section can contain arbitrary keys. Each of these specifies the
amount of a global resource that the scheduler can allow workers to use.
The scheduler will prevent running jobs with resources specified from
exceeding the counts in this section. Unspecified resources are assumed
to have limit 1. Example resources section for a configuration with 2
hive resources and 1 mysql resource:

.. code:: ini

  [resources]
  hive=2
  mysql=1

Note that it was not necessary to specify the 1 for mysql here, but it
is good practice to do so when you have a fixed set of resources.

.. _retcode-config:

[retcode]
---------

Configure return codes for the Luigi binary. In the case of multiple return
codes that could apply, for example a failing task and missing data, the
*numerically greatest* return code is returned.

We recommend that you copy this set of exit codes to your ``luigi.cfg`` file:

.. code:: ini

  [retcode]
  # The following return codes are the recommended exit codes for Luigi
  # They are in increasing level of severity (for most applications)
  already_running=10
  missing_data=20
  not_run=25
  task_failed=30
  scheduling_error=35
  unhandled_exception=40

already_running
  This can happen in two different cases. Either the local lock file was taken
  at the time the invocation starts up. Or, the central scheduler have reported
  that some tasks could not have been run, because other workers are already
  running the tasks.
missing_data
  For when an :py:class:`~luigi.task.ExternalTask` is not complete, and this
  caused the worker to give up.  As an alternative to fiddling with this, see
  the [worker] keep_alive option.
not_run
  For when a task is not granted run permission by the scheduler. Typically
  because of lack of resources, because the task has been already run by
  another worker or because the attempted task is in DISABLED state.
  Connectivity issues with the central scheduler might also cause this.
  This does not include the cases for which a run is not allowed due to missing
  dependencies (missing_data) or due to the fact that another worker is currently
  running the task (already_running).
task_failed
  For signaling that there were last known to have failed. Typically because
  some exception have been raised.
scheduling_error
  For when a task's ``complete()`` or ``requires()`` method fails with an
  exception, or when the limit number of tasks is reached.
unhandled_exception
  For internal Luigi errors.  Defaults to 4, since this type of error
  probably will not recover over time.

If you customize return codes, prefer to set them in range 128 to 255 to avoid
conflicts. Return codes in range 0 to 127 are reserved for possible future use
by Luigi contributors.

[scalding]
----------

Parameters controlling running of scalding jobs

scala_home
  Home directory for scala on your machine. Defaults to either
  SCALA_HOME or /usr/share/scala if SCALA_HOME is unset.

scalding_home
  Home directory for scalding on your machine. Defaults to either
  SCALDING_HOME or /usr/share/scalding if SCALDING_HOME is unset.

scalding_provided
  Provided directory for scalding on your machine. Defaults to either
  SCALDING_HOME/provided or /usr/share/scalding/provided

scalding_libjars
  Libjars directory for scalding on your machine. Defaults to either
  SCALDING_HOME/libjars or /usr/share/scalding/libjars


.. _scheduler-config:

[scheduler]
-----------

Parameters controlling scheduler behavior

batch_emails
  Whether to send batch e-mails for failures and disables rather than
  sending immediate disable e-mails and just relying on workers to send
  immediate batch e-mails.
  Defaults to false.

disable_hard_timeout
  Hard time limit after which tasks will be disabled by the server if
  they fail again, in seconds. It will disable the task if it fails
  **again** after this amount of time. E.g. if this was set to 600
  (i.e. 10 minutes), and the task first failed at 10:00am, the task would
  be disabled if it failed again any time after 10:10am. Note: This setting
  does not consider the values of the ``retry_count`` or
  ``disable_window`` settings.

retry_count
  Number of times a task can fail within ``disable_window`` before
  the scheduler will automatically disable it. If not set, the scheduler
  will not automatically disable jobs.

disable_persist
  Number of seconds for which an automatic scheduler disable lasts.
  Defaults to 86400 (1 day).

disable_window
  Number of seconds during which ``retry_count`` failures must
  occur in order for an automatic disable by the scheduler. The
  scheduler forgets about disables that have occurred longer ago than
  this amount of time. Defaults to 3600 (1 hour).

record_task_history
  If true, stores task history in a database. Defaults to false.

remove_delay
  Number of seconds to wait before removing a task that has no
  stakeholders. Defaults to 600 (10 minutes).

retry_delay
  Number of seconds to wait after a task failure to mark it pending
  again. Defaults to 900 (15 minutes).

state_path
  Path in which to store the Luigi scheduler's state. When the scheduler
  is shut down, its state is stored in this path. The scheduler must be
  shut down cleanly for this to work, usually with a kill command. If
  the kill command includes the -9 flag, the scheduler will not be able
  to save its state. When the scheduler is started, it will load the
  state from this path if it exists. This will restore all scheduled
  jobs and other state from when the scheduler last shut down.

  Sometimes this path must be deleted when restarting the scheduler
  after upgrading Luigi, as old state files can become incompatible
  with the new scheduler. When this happens, all workers should be
  restarted after the scheduler both to become compatible with the
  updated code and to reschedule the jobs that the scheduler has now
  forgotten about.

  This defaults to /var/lib/luigi-server/state.pickle

worker_disconnect_delay
  Number of seconds to wait after a worker has stopped pinging the
  scheduler before removing it and marking all of its running tasks as
  failed. Defaults to 60.

pause_enabled
  If false, disables pause/unpause operations and hides the pause toggle from
  the visualiser.

send_messages
  When true, the scheduler is allowed to send messages to running tasks and
  the central scheduler provides a simple prompt per task to send messages.
  Defaults to true.

metrics_collector
  Optional setting allowing Luigi to use a contribution to collect metrics
  about the pipeline to a third-party. By default this uses the default metric
  collector that acts as a shell and does nothing. The currently available
  options are "datadog" and "prometheus".


[sendgrid]
----------

These parameters control sending error e-mails through SendGrid.

apikey
  API key of the SendGrid account.


[smtp]
------

These parameters control the smtp server setup.

host
  Hostname for sending mail through smtp. Defaults to localhost.

local_hostname
  If specified, overrides the FQDN of localhost in the HELO/EHLO
  command.

no_tls
  If true, connects to smtp without TLS. Defaults to false.

password
  Password to log in to your smtp server. Must be specified for
  username to have an effect.

port
  Port number for smtp on smtp_host. Defaults to 0.

ssl
  If true, connects to smtp through SSL. Defaults to false.

timeout
  Sets the number of seconds after which smtp attempts should time out.
  Defaults to 10.

username
  Username to log in to your smtp server, if necessary.


[spark]
-------

Parameters controlling the default execution of :py:class:`~luigi.contrib.spark.SparkSubmitTask` and :py:class:`~luigi.contrib.spark.PySparkTask`:

.. deprecated:: 1.1.1
   :py:class:`~luigi.contrib.spark.SparkJob`, :py:class:`~luigi.contrib.spark.Spark1xJob` and :py:class:`~luigi.contrib.spark.PySpark1xJob`
    are deprecated. Please use :py:class:`~luigi.contrib.spark.SparkSubmitTask` or :py:class:`~luigi.contrib.spark.PySparkTask`.

spark_submit
  Command to run in order to submit spark jobs. Default: ``"spark-submit"``

master
  Master url to use for ``spark_submit``. Example: local[*], spark://masterhost:7077. Default: Spark default (Prior to 1.1.1: yarn-client)

deploy_mode
    Whether to launch the driver programs locally ("client") or on one of the worker machines inside the cluster ("cluster"). Default: Spark default

jars
    Comma-separated list of local jars to include on the driver and executor classpaths. Default: Spark default

packages
    Comma-separated list of packages to link to on the driver and executors

py_files
    Comma-separated list of .zip, .egg, or .py files to place on the ``PYTHONPATH`` for Python apps. Default: Spark default

files
    Comma-separated list of files to be placed in the working directory of each executor. Default: Spark default

conf:
    Arbitrary Spark configuration property in the form Prop=Value|Prop2=Value2. Default: Spark default

properties_file
    Path to a file from which to load extra properties. Default: Spark default

driver_memory
    Memory for driver (e.g. 1000M, 2G). Default: Spark default

driver_java_options
    Extra Java options to pass to the driver. Default: Spark default

driver_library_path
    Extra library path entries to pass to the driver. Default: Spark default

driver_class_path
    Extra class path entries to pass to the driver. Default: Spark default

executor_memory
    Memory per executor (e.g. 1000M, 2G). Default: Spark default

*Configuration for Spark submit jobs on Spark standalone with cluster deploy mode only:*

driver_cores
    Cores for driver. Default: Spark default

supervise
    If given, restarts the driver on failure. Default: Spark default

*Configuration for Spark submit jobs on Spark standalone and Mesos only:*

total_executor_cores
    Total cores for all executors. Default: Spark default

*Configuration for Spark submit jobs on YARN only:*

executor_cores
    Number of cores per executor. Default: Spark default

queue
    The YARN queue to submit to. Default: Spark default

num_executors
    Number of executors to launch. Default: Spark default

archives
    Comma separated list of archives to be extracted into the working directory of each executor. Default: Spark default

hadoop_conf_dir
  Location of the hadoop conf dir. Sets HADOOP_CONF_DIR environment variable
  when running spark. Example: /etc/hadoop/conf

*Extra configuration for PySparkTask jobs:*

py_packages
    Comma-separated list of local packages (in your python path) to be distributed to the cluster.

*Parameters controlling the execution of SparkJob jobs (deprecated):*


[task_history]
--------------

Parameters controlling storage of task history in a database

db_connection
  Connection string for connecting to the task history db using
  sqlalchemy.


[execution_summary]
-------------------

Parameters controlling execution summary of a worker

summary_length
  Maximum number of tasks to show in an execution summary.  If the value is 0,
  then all tasks will be displayed.  Default value is 5.


[webhdfs]
---------

port
  The port to use for webhdfs. The normal namenode port is probably on a
  different port from this one.

user
  Perform file system operations as the specified user instead of $USER.  Since
  this parameter is not honored by any of the other hdfs clients, you should
  think twice before setting this parameter.

client_type
  The type of client to use. Default is the "insecure" client that requires no
  authentication. The other option is the "kerberos" client that uses kerberos
  authentication.

[datadog]
---------

api_key
  The api key found in the account settings of Datadog under the API
  sections.
app_key
  The application key found in the account settings of Datadog under the API
  sections.
default_tags
  Optional settings that adds the tag to all the metrics and events sent to
  Datadog. Default value is "application:luigi".
environment
  Allows you to tweak multiple environment to differentiate between production,
  staging or development metrics within Datadog. Default value is "development".
statsd_host
  The host that has the statsd instance to allow Datadog to send statsd metric. Default value is "localhost".
statsd_port
  The port on the host that allows connection to the statsd host. Defaults value is 8125.
metric_namespace
  Optional prefix to add to the beginning of every metric sent to Datadog.
  Default value is "luigi".

Per Task Retry-Policy
---------------------

Luigi also supports defining ``retry_policy`` per task.

.. code-block:: python

    class GenerateWordsFromHdfs(luigi.Task):

       retry_count = 2

        ...

    class GenerateWordsFromRDBM(luigi.Task):

       retry_count = 5

        ...

    class CountLetters(luigi.Task):

        def requires(self):
            return [GenerateWordsFromHdfs()]

        def run():
            yield GenerateWordsFromRDBM()

        ...

If none of retry-policy fields is defined per task, the field value will be **default** value which is defined in luigi config file.

To make luigi sticks to the given retry-policy, be sure you run luigi worker with ``keep_alive`` config. Please check ``keep_alive`` config in :ref:`worker-config` section.

Retry-Policy Fields
-------------------

The fields below are in retry-policy and they can be defined per task.

* ``retry_count``
* ``disable_hard_timeout``
* ``disable_window``
