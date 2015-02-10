.. figure:: https://raw.githubusercontent.com/spotify/luigi/master/doc/luigi.png
   :alt: Luigi Logo
   :align: center

About Luigi
-----------

.. image:: https://img.shields.io/travis/spotify/luigi/master.svg?style=flat
    :target: https://travis-ci.org/spotify/luigi

.. image:: https://img.shields.io/coveralls/spotify/luigi/master.svg?style=flat
    :target: https://coveralls.io/r/spotify/luigi?branch=master

.. image:: https://landscape.io/github/spotify/luigi/master/landscape.svg?style=flat
   :target: https://landscape.io/github/spotify/luigi/master

.. image:: https://img.shields.io/pypi/dm/luigi.svg?style=flat
   :target: https://pypi.python.org/pypi/luigi

.. image:: https://img.shields.io/pypi/l/luigi.svg?style=flat
   :target: https://pypi.python.org/pypi/luigi

Luigi is a Python package that helps you build complex pipelines of
batch jobs. It handles dependency resolution, workflow management,
visualization, handling failures, command line integration, and much
more.

The purpose of Luigi is to address all the plumbing typically associated
with long-running batch processes. You want to chain many tasks,
automate them, and failures *will* happen. These tasks can be anything,
but are typically long running things like
`Hadoop <http://hadoop.apache.org/>`_ jobs, dumping data to/from
databases, running machine learning algorithms, or anything else.

There are other software packages that focus on lower level aspects of
data processing, like `Hive <http://hive.apache.org/>`_,
`Pig <http://pig.apache.org/>`_, or
`Cascading <http://www.cascading.org/>`_. Luigi is not a framework to
replace these. Instead it helps you stitch many tasks together, where
each task can be a Hive query, a Hadoop job in Java, a Python snippet,
dumping a table from a database, or anything else. It's easy to build up
long-running pipelines that comprise thousands of tasks and take days or
weeks to complete. Luigi takes care of a lot of the workflow management
so that you can focus on the tasks themselves and their dependencies.

You can build pretty much any task you want, but Luigi also comes with a
*toolbox* of several common task templates that you use. It includes
native Python support for running mapreduce jobs in Hadoop, as well as
Hive and Jar jobs. It also comes with filesystem abstractions for HDFS
and local files that ensures all file system operations are atomic. This
is important because it means your data pipeline will not crash in a
state containing partial data.

Dependency graph example
------------------------

Just to give you an idea of what Luigi does, this is a screen shot from
something we are running in production. Using Luigi's visualizer, we get
a nice visual overview of the dependency graph of the workflow. Each
node represents a task which has to be run. Green tasks are already
completed whereas yellow tasks are yet to be run. Most of these tasks
are Hadoop jobs, but there are also some things that run locally and
build up data files.

.. figure:: https://raw.githubusercontent.com/spotify/luigi/master/doc/user_recs.png
   :alt: Dependency graph

Background
----------

We use Luigi internally at `Spotify <https://www.spotify.com/us/>`_ to run
thousands of tasks every day, organized in complex dependency graphs.
Most of these tasks are Hadoop jobs. Luigi provides an infrastructure
that powers all kinds of stuff including recommendations, toplists, A/B
test analysis, external reports, internal dashboards, etc. Luigi grew
out of the realization that powerful abstractions for batch processing
can help programmers focus on the most important bits and leave the rest
(the boilerplate) to the framework.

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
trigger things not in Python, such as running Pig scripts or scp'ing
files.

Installing
----------

Downloading and running ``python setup.py install`` should be enough. Note
that you probably want `Tornado <http://www.tornadoweb.org/>`_. See
`Configuration <http://luigi.readthedocs.org/en/latest/configuration.html>`_ for how to configure Luigi.

Getting Started
---------------

Take a look at the `Example workflow
<http://luigi.readthedocs.org/en/latest/example_top_artists.html>`_ and the `API overview
<http://luigi.readthedocs.org/en/latest/api_overview.html>`_ which explains some of
the most important concepts.

Who uses Luigi?
---------------

Several companies have written blog posts or presentation about Luigi:

* `Spotify : NYC Data Science <http://www.slideshare.net/erikbern/luigi-presentation-nyc-data-science>`_
* `Foursquare <http://www.slideshare.net/OpenAnayticsMeetup/luigi-presentation-17-23199897>`_
* `Mortar Data <https://help.mortardata.com/technologies/luigi>`_
* `Stripe <http://www.slideshare.net/PyData/python-as-part-of-a-production-machine-learning-stack-by-michael-manapat-pydata-sv-2014>`_
* `Asana <https://eng.asana.com/2014/11/stable-accessible-data-infrastructure-startup/>`_
* `Buffer <https://overflow.bufferapp.com/2014/10/31/buffers-new-data-architecture/>`_
* `SeatGeek <http://chairnerd.seatgeek.com/building-out-the-seatgeek-data-pipeline/>`_

Please let us know if your company wants to be featured on this list!

Getting Help
------------

* Find us on `#luigi` on freenode.
* Subscribe to the `luigi-user <http://groups.google.com/group/luigi-user/>`_
  group and ask a question.

External links
--------------

* `Documentation <http://luigi.readthedocs.org/>`_, including the `Luigi package documentation <http://luigi.readthedocs.org/en/latest/api/luigi.html>`_ (Read the Docs)
* `Mailing List <https://groups.google.com/d/forum/luigi-user/>`_ (Google Groups)
* `Releases <https://pypi.python.org/pypi/luigi>`_ (PyPi)
* `Source code <https://github.com/spotify/luigi>`_ (Github)

Authors
-------

Luigi was built at `Spotify <https://www.spotify.com/us/>`_, mainly by
`Erik Bernhardsson <https://github.com/erikbern>`_ and `Elias
Freider <https://github.com/freider>`_, but many other people have
contributed.

