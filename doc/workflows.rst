Building workflows
------------------

There are two fundamental building blocks of Luigi -
the :class:`~luigi.task.Task` class and the :class:`~luigi.target.Target` class.
Both are abstract classes and expect a few methods to be implemented.
In addition to those two concepts,
the :class:`~luigi.parameter.Parameter` class is an important concept that governs how a Task is run.

Target
~~~~~~

The :py:class:`~luigi.target.Target` class corresponds to a file on a disk,
a file on HDFS or some kind of a checkpoint, like an entry in a database.
Actually, the only method that Targets have to implement is the *exists*
method which returns True if and only if the Target exists.

In practice, implementing Target subclasses is rarely needed.
Luigi comes with a toolbox of several useful Targets.
In particular, :class:`~luigi.file.LocalTarget` and :class:`~luigi.contrib.hdfs.target.HdfsTarget`,
but there is also support for other file systems:
:class:`luigi.contrib.s3.S3Target`,
:class:`luigi.contrib.ssh.RemoteTarget`,
:class:`luigi.contrib.ftp.RemoteTarget`,
:class:`luigi.contrib.mysqldb.MySqlTarget`,
:class:`luigi.contrib.redshift.RedshiftTarget`, and several more.

Most of these targets, are file system-like.
For instance, :class:`~luigi.file.LocalTarget` and :class:`~luigi.contrib.hdfs.target.HdfsTarget` map to a file on the local drive or a file in HDFS.
In addition these also wrap the underlying operations to make them atomic.
They both implement the :func:`~luigi.file.LocalTarget.open` method which returns a stream object that
could be read (``mode='r'``) from or written to (``mode='w'``).

Luigi comes with Gzip support by providing ``format=format.Gzip``.
Adding support for other formats is pretty simple.

Task
~~~~

The :class:`~luigi.task.Task` class is a bit more conceptually interesting because this is
where computation is done.
There are a few methods that can be implemented to alter its behavior,
most notably :func:`~luigi.task.Task.run`, :func:`~luigi.task.Task.output` and :func:`~luigi.task.Task.requires`.

Tasks consume Targets that were created by some other task. They usually also output targets:

    .. figure:: task_with_targets.png
       :alt: Task and targets

You can define dependencies between *Tasks* using the :py:meth:`~luigi.task.Task.requires` method. See :doc:`/tasks` for more info.

    .. figure:: tasks_with_dependencies.png
       :alt: Tasks and dependencies

Each task defines its outputs using the :py:meth:`~luigi.task.Task.output` method.
Additionally, there is a helper method :py:meth:`~luigi.task.Task.input` that returns the corresponding Target classes for each Task dependency.

    .. figure:: tasks_input_output_requires.png
       :alt: Tasks and methods

.. _Parameter:

Parameter
~~~~~~~~~

The Task class corresponds to some type of job that is run, but in
general you want to allow some form of parameterization of it.
For instance, if your Task class runs a Hadoop job to create a report every night,
you probably want to make the date a parameter of the class.
See :doc:`/parameters` for more info.

    .. figure:: task_parameters.png
       :alt: Tasks with parameters

Dependencies
~~~~~~~~~~~~

Using tasks, targets, and parameters, Luigi lets you express arbitrary dependencies in *code*, rather than using some kind of awkward config DSL.
This is really useful because in the real world, dependencies are often very messy.
For instance, some examples of the dependencies you might encounter:

    .. figure:: parameters_date_algebra.png
       :alt: Dependencies with date algebra

    .. figure:: parameters_recursion.png
       :alt: Dependencies with recursion

    .. figure:: parameters_enum.png
       :alt: Dependencies with enums

(These diagrams are from a `Luigi presentation in late 2014 at NYC Data Science meetup <http://www.slideshare.net/erikbern/luigi-presentation-nyc-data-science>`_)
