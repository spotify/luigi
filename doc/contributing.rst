Want to Contribute?
-------------------

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
