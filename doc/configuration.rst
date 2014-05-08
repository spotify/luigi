Configuration
-------------

All configuration can be done by adding a configuration file named
client.cfg to your current working directory or /etc/luigi (although
this is further configurable).

-  *default-scheduler-host* defaults the scheduler to some hostname so
   that you don't have to provide it as an argument
-  *error-email* makes sure every time things crash, you will get an
   email (unless it was run on the command line)
-  *luigi-history*, if set, specifies a filename for Luigi to write
   stuff (currently just job id) to in mapreduce job's output directory.
   Useful in a configuration where no history is stored in the output
   directory by Hadoop.
-  If you want to run Hadoop mapreduce jobs in Python, you should also a
   path to your streaming jar
-  By default, Luigi is configured to work with the CDH4 release of
   Hadoop. There are some minor differences with regards to the HDFS CLI
   in CDH3, CDH4 and the Apache releases of Hadoop. If you want to use a
   release other than CDH4, you need to specify which version you are
   using.

Example /etc/luigi/client.cfg
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    [hadoop]
    version: cdh4
    jar: /usr/lib/hadoop-xyz/hadoop-streaming-xyz-123.jar

    [core]
    default-scheduler-host: luigi-host.mycompany.foo
    error-email: foo@bar.baz

All sections are optional based on what parts of Luigi you are actually
using. By default, Luigi will not send error emails when running through
a tty terminal. If using the Apache release of Hive, there are slight
differences when compared to the CDH release, so specify this
configuration setting accordingly.