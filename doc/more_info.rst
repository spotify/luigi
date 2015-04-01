
More Info
---------

Luigi is the successor to a couple of attempts that we weren't fully happy with.
We learned a lot from our mistakes and some design decisions include:

-  Straightforward command line integration.
-  As little boilerplate as possible.
-  Focus on job scheduling and dependency resolution, not a particular platform.
   In particular this means no limitation to Hadoop.
   Though Hadoop/HDFS support is built-in and is easy to use,
   this is just one of many types of things you can run.
-  A file system abstraction where code doesn't have to care about where files are located.
-  Atomic file system operations through this abstraction.
   If a task crashes it won't lead to a broken state.
-  The dependencies are decentralized.
   No big config file in XML.
   Each task just specifies which inputs it needs and cross-module dependencies are trivial.
-  A web server that renders the dependency graph and does locking etc for free.
-  Trivial to extend with new file systems, file formats and job types.
   You can easily write jobs that inserts a Tokyo Cabinet into Cassandra.
   Adding support for new systems is generally not very hard.
   (Feel free to send us a patch when you're done!)
-  Date algebra included.
-  Lots of unit tests of the most basic stuff

It wouldn't be fair not to mention some limitations with the current design:

-  Its focus is on batch processing so
   it's probably less useful for near real-time pipelines or continuously running processes.
-  The assumption is that a each task is a sizable chunk of work.
   While you can probably schedule a few thousand jobs,
   it's not meant to scale beyond tens of thousands.
-  Luigi does not support distribution of execution.
   When you have workers running thousands of jobs daily, this starts to matter,
   because the worker nodes get overloaded.
   There are some ways to mitigate this (trigger from many nodes, use resources),
   but none of them is ideal
-  Luigi does not come with built-in triggering, and you still need to rely on something like
   crontab to trigger workflows periodically.


Also it should be mentioned that Luigi is named after the world's second most famous plumber.

Want to Contribute?
~~~~~~~~~~~~~~~~~~~

Awesome! Let us know if you have any ideas. Feel free to contact x@y.com
where x = luigi and y = spotify.

Running Unit Tests
~~~~~~~~~~~~~~~~~~

You can see in ``.travis.yml`` how Travis CI runs the tests. Essentially, what
you do is first ``pip install tox``, then you can run any of these examples and
change them to your needs.


.. code-block:: bash

    # Run all nonhdfs tests
    export TOX_ENV=nonhdfs; export PYTHONPATH=''; tox -e $TOX_ENV test

    # Run specific nonhdfs tests
    export TOX_ENV=nonhdfs; export PYTHONPATH=''; tox -e $TOX_ENV test/test_ssh.py

    # Run specific hdp tests with hdp hadoop distrubtion
    export TOX_ENV=hdp; export PYTHONPATH=''; JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-amd64 tox -e $TOX_ENV test/snakebite_test.py

Future Ideas
~~~~~~~~~~~~

-  S3/EC2 - We have some old ugly code based on Boto that could be
   integrated in a day or two.
-  Built in support for Pig/Hive.
-  Better visualization tool - the layout gets pretty messy as the
   number of tasks grows.
-  Integration with existing Hadoop frameworks like mrjob would be cool
   and probably pretty easy.
-  Better support (without much boilerplate) for unittesting specific
   Tasks
