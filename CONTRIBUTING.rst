Running the tests
-----------------

We are always happy to recieve Pull Requests. When you open a PR, it will
automatically build on Travis. So you're not strictly required to test the
patch locally before submitting it.

If you do want to run the tests locally you'll need to ``pip install tox`` and
then run one of the tox commands below.

.. code:: bash

    # These commands are pretty fast and will tell if you if you've
    # broken something major:
    tox -e flake8
    tox -e py27-nonhdfs
    
    # You can also test particular files for even faster iterations
    tox -e py27-nonhdfs test/rpc_test.py

    # And some of the others involve downloading and running Hadoop:
    tox -e py33-cdh
    tox -e py34-hdp

Where ``flake8`` is the lint checking, ``py27`` is obviously Python 2.7.
``nonhdfs`` are tests not running on the Hadoop minicluster and ``cdh`` and
``hdp`` are two different hadoop distributions. For most local development it's
usually enough to run the lint checking and a python version for ``nonhdfs``
and let Travis run for the whole matrix.

For `cdh` and `hdp`, tox will download the hadoop distribution for you. You
however have to have Java installed and the `JAVA_HOME` environment variable
set.

For more details, check out the ``.travis.yml`` and ``tox.ini`` files.
