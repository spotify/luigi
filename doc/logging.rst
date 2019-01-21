Configure logging
-----------------


Config options:
~~~~~~~~~~~~~~~

Some config options for config [core] section

log_level
    The default log level to use when no logging_conf_file is set. Must be
    a valid name of a `Python log level
    <https://docs.python.org/3/library/logging.html#logging-levels>`_.
    Default is ``DEBUG``.
logging_conf_file
      Location of the logging configuration file.
no_configure_logging
    If true, logging is not configured. Defaults to false.


Config section
~~~~~~~~~~~~~~

If you're use TOML for configuration file, you can configure logging
via ``logging`` section in this file. See `example
<https://github.com/spotify/luigi/blob/master/examples/config.toml>`_
for more details.

Luigid CLI options:
~~~~~~~~~~~~~~~~~~~

``--background``
    Run daemon in background mode. Disable logging setup
    and set up log level to INFO for root logger.
``--logdir``
    set logging with INFO level and output in ``$logdir/luigi-server.log`` file


Worker CLI options:
~~~~~~~~~~~~~~~~~~~

``--logging-conf-file``
    Configuration file for logging.
``--log-level``
    Default log level.
    Available values: NOTSET, DEBUG, INFO, WARNING, ERROR, CRITICAL.
    Default DEBUG. See `Python documentation
    <https://docs.python.org/3/library/logging.html#logging-levels>`_
    For information about levels difference.


Configuration options resolution order:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. no_configure_logging option
2. ``--background``
3. ``--logdir``
4. ``--logging-conf-file``
5. logging_conf_file option
6. ``logging`` section
7. ``--log-level``
8. log_level option
