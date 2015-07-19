Running the tests
-----------------

We are always happy to recieve Pull Requests. When you open a PR, it will automatically build on Travis. So you're not strictly required to test the patch locally before submitting it.

Despite of this, it's good to test it locally first to speed up the iteration cycle. Here are four example commands:

.. code:: bash

    tox -e pep8
    tox -e py27-nonhdfs
    tox -e py33-cdh
    tox -e py34-hdp

Where ``pep8`` is the lint checking, ``py27`` is obviously Python 2.7. ``nonhdfs`` are tests not running on the Hadoop minicluster and ``cdh`` and ``hdp`` are two different hadoop distributions. For most local development it's usually enough to run the lint checking and a python version for ``nonhdfs`` and let Travis run for the whole matrix.

For `cdh` and `hdp`, tox will download the hadoop distribution for you. You however have to have Java installed and the `JAVA_HOME` environment variable set.

For more details, check out the ``.travis.yml`` and ``tox.ini`` files.
