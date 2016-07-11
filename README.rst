.. figure:: https://raw.githubusercontent.com/spotify/luigi/master/doc/luigi.png
   :alt: Luigi Logo
   :align: center

.. image:: https://img.shields.io/travis/spotify/luigi/master.svg?style=flat
    :target: https://travis-ci.org/spotify/luigi

.. image:: https://img.shields.io/codecov/c/github/spotify/luigi/master.svg?style=flat
    :target: https://codecov.io/gh/spotify/luigi?branch=master

.. image:: https://landscape.io/github/spotify/luigi/master/landscape.svg?style=flat
   :target: https://landscape.io/github/spotify/luigi/master

.. image:: https://img.shields.io/pypi/dm/luigi.svg?style=flat
   :target: https://pypi.python.org/pypi/luigi

.. image:: https://img.shields.io/pypi/l/luigi.svg?style=flat
   :target: https://pypi.python.org/pypi/luigi

Luigi is a Python (2.7, 3.3, 3.4, 3.5) package that helps you build complex pipelines of batch
jobs. It handles dependency resolution, workflow management, visualization,
handling failures, command line integration, and much more.

Getting Started
---------------

Run ``pip install luigi`` to install the latest stable version from `PyPI
<https://pypi.python.org/pypi/luigi>`_. Documentation for the latest release is
hosted `here <https://luigi.readthedocs.io/en/stable/>`__.

For the bleeding edge code, ``pip install
git+https://github.com/spotify/luigi.git``. Bleeding edge documentation can be
found `here <https://luigi.readthedocs.io/en/latest/>`__.

Background
----------

The purpose of Luigi is to address all the plumbing typically associated
with long-running batch processes. You want to chain many tasks,
automate them, and failures *will* happen. These tasks can be anything,
but are typically long running things like
`Hadoop <http://hadoop.apache.org/>`_ jobs, dumping data to/from
databases, running machine learning algorithms, or anything else.

There are other software packages that focus on lower level aspects of
data processing, like `Hive <http://hive.apache.org/>`__,
`Pig <http://pig.apache.org/>`_, or
`Cascading <http://www.cascading.org/>`_. Luigi is not a framework to
replace these. Instead it helps you stitch many tasks together, where
each task can be a `Hive query <https://luigi.readthedocs.io/en/latest/api/luigi.contrib.hive.html>`__,
a `Hadoop job in Java <https://luigi.readthedocs.io/en/latest/api/luigi.contrib.hadoop_jar.html>`_,
a  `Spark job in Scala or Python <https://luigi.readthedocs.io/en/latest/api/luigi.contrib.spark.html>`_
a Python snippet,
`dumping a table <https://luigi.readthedocs.io/en/latest/api/luigi.contrib.sqla.html>`_
from a database, or anything else. It's easy to build up
long-running pipelines that comprise thousands of tasks and take days or
weeks to complete. Luigi takes care of a lot of the workflow management
so that you can focus on the tasks themselves and their dependencies.

You can build pretty much any task you want, but Luigi also comes with a
*toolbox* of several common task templates that you use. It includes
support for running
`Python mapreduce jobs <https://luigi.readthedocs.io/en/latest/api/luigi.contrib.hadoop.html>`_
in Hadoop, as well as
`Hive <https://luigi.readthedocs.io/en/latest/api/luigi.contrib.hive.html>`__,
and `Pig <https://luigi.readthedocs.io/en/latest/api/luigi.contrib.pig.html>`__,
jobs. It also comes with
`file system abstractions for HDFS <https://luigi.readthedocs.io/en/latest/api/luigi.hdfs.html>`_,
and local files that ensures all file system operations are atomic. This
is important because it means your data pipeline will not crash in a
state containing partial data.

Visualiser page
---------------

The Luigi server comes with a web interface too, so you can search and filter
among all your tasks.

.. figure:: https://raw.githubusercontent.com/spotify/luigi/master/doc/visualiser_front_page.png
   :alt: Visualiser page

Dependency graph example
------------------------

Just to give you an idea of what Luigi does, this is a screen shot from
something we are running in production. Using Luigi's visualiser, we get
a nice visual overview of the dependency graph of the workflow. Each
node represents a task which has to be run. Green tasks are already
completed whereas yellow tasks are yet to be run. Most of these tasks
are Hadoop jobs, but there are also some things that run locally and
build up data files.

.. figure:: https://raw.githubusercontent.com/spotify/luigi/master/doc/user_recs.png
   :alt: Dependency graph

Philosophy
----------

Conceptually, Luigi is similar to `GNU
Make <http://www.gnu.org/software/make/>`_ where you have certain tasks
and these tasks in turn may have dependencies on other tasks. There are
also some similarities to `Oozie <http://oozie.apache.org/>`_
and `Azkaban <http://data.linkedin.com/opensource/azkaban>`_. One major
difference is that Luigi is not just built specifically for Hadoop, and
it's easy to extend it with other kinds of tasks.

Everything in Luigi is in Python. Instead of XML configuration or
similar external data files, the dependency graph is specified *within
Python*. This makes it easy to build up complex dependency graphs of
tasks, where the dependencies can involve date algebra or recursive
references to other versions of the same task. However, the workflow can
trigger things not in Python, such as running
`Pig scripts <https://luigi.readthedocs.io/en/latest/api/luigi.contrib.pig.html>`_
or `scp'ing files <https://luigi.readthedocs.io/en/latest/api/luigi.contrib.ssh.html>`_.

Who uses Luigi?
---------------

We use Luigi internally at `Spotify <https://www.spotify.com/us/>`_ to run
thousands of tasks every day, organized in complex dependency graphs.
Most of these tasks are Hadoop jobs. Luigi provides an infrastructure
that powers all kinds of stuff including recommendations, toplists, A/B
test analysis, external reports, internal dashboards, etc.

Since Luigi is open source and without any registration walls, the exact number
of Luigi users is unknown. But based on the number of unique contributors, we
expect hundreds of enterprises to use it. Some users have written blog posts
or held presentations about Luigi:

* `Spotify (NYC Data Science) <http://www.slideshare.net/erikbern/luigi-presentation-nyc-data-science>`_
* `Foursquare <http://www.slideshare.net/OpenAnayticsMeetup/luigi-presentation-17-23199897>`_
* `Mortar Data <http://help.mortardata.com/technologies/luigi>`_
* `Stripe <http://www.slideshare.net/PyData/python-as-part-of-a-production-machine-learning-stack-by-michael-manapat-pydata-sv-2014>`_
* `Asana <https://eng.asana.com/2014/11/stable-accessible-data-infrastructure-startup/>`_
* `Buffer <https://overflow.bufferapp.com/2014/10/31/buffers-new-data-architecture/>`_
* `SeatGeek <http://chairnerd.seatgeek.com/building-out-the-seatgeek-data-pipeline/>`_
* `Treasure Data <http://blog.treasuredata.com/blog/2015/02/25/managing-the-data-pipeline-with-git-luigi/>`_
* `Growth Intelligence <http://www.slideshare.net/growthintel/a-beginners-guide-to-building-data-pipelines-with-luigi>`_
* `AdRoll <http://tech.adroll.com/blog/data/2015/09/22/data-pipelines-docker.html>`_
* `Schibsted <http://www.schibsted.com/>`_
* `17zuoye <https://speakerdeck.com/mvj3/luiti-an-offline-task-management-framework>`_
* `enbrite.ly <http://enbrite.ly/>`_
* `Dow Jones / The Wall Street Journal <http://wsj.com>`_
* `Hotels.com <https://hotels.com>`_
* `Custobar (Metrics Monday Helsinki) <http://www.slideshare.net/teemukurppa/managing-data-workflows-with-luigi>`_

We're more than happy to have your company added here. Just send a PR on GitHub.

External links
--------------

* `Mailing List <https://groups.google.com/d/forum/luigi-user/>`_ for discussions and asking questions. (Google Groups)
* `Releases <https://pypi.python.org/pypi/luigi>`_ (PyPI)
* `Source code <https://github.com/spotify/luigi>`_ (Github)
* `Hubot Integration <https://github.com/houzz/hubot-luigi>`_ plugin for Slack, Hipchat, etc (Github)

Authors
-------

Luigi was built at `Spotify <https://www.spotify.com/us/>`_, mainly by
`Erik Bernhardsson <https://github.com/erikbern>`_ and
`Elias Freider <https://github.com/freider>`_.
`Many other people <https://github.com/spotify/luigi/graphs/contributors>`_
have contributed since open sourcing in late 2012.
`Arash Rouhani <https://github.com/tarrasch>`_ is currently the chief
maintainer of Luigi.
