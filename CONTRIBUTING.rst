Code of conduct
---------------

This project adheres to the `Open Code of Conduct 
<https://github.com/spotify/code-of-conduct/blob/master/code-of-conduct.md>`_.  By 
participating, you are expected to honor this code.

Running the tests
-----------------


We are always happy to receive Pull Requests. When you open a PR, it will
automatically build on Travis. So you're not strictly required to test the
patch locally before submitting it.

If you do want to run the tests locally you'll need to run the commands below
.. code:: bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   uv tool install tox --with tox-uv

You will need a ``tox --version`` of at least 4.22.

.. code:: bash

    # These commands are pretty fast and will tell if you've
    # broken something major:
    tox run -e flake8
    tox run -e py38-core

    # You can also test particular files for even faster iterations
    tox run -e py38-core -- test/rpc_test.py

    # The visualiser tests require phantomjs to be installed on your path
    tox run -e visualiser

    # And some of the others involve downloading and running Hadoop:
    tox run -e py38-cdh
    tox run -e py39-hdp

Where ``flake8`` is the lint checking, ``py38`` is obviously Python 3.8.
``core`` are tests that do not require external components and ``cdh`` and
``hdp`` are two different hadoop distributions. For most local development it's
usually enough to run the lint checking and a python version for ``core``
and let Travis run for the whole matrix.

For `cdh` and `hdp`, tox will download the hadoop distribution for you. You
however have to have Java installed and the `JAVA_HOME` environment variable
set.

For more details, check out the ``.github/workflows/pythonbuild.yml`` and ``tox.ini`` files.

Writing documentation
=====================

All documentation for Luigi is written in `reStructuredText/Sphinx markup
<http://sphinx-doc.org/domains.html#the-python-domain>`_ and are both in the
code as docstrings and in `.rst`. Pull requests should come with documentation
when appropriate.

You verify that your documentation code compiles by running

.. code:: bash

    tox run -e docs

After that, you can check how it renders locally with your browser

.. code:: bash

    firefox doc/_build/html/index.html
