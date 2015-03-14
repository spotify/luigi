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
You can probably get pretty far with the :class:`~luigi.file.LocalTarget` and :class:`~luigi.hdfs.HdfsTarget`
classes that are available out of the box.
These directly map to a file on the local drive or a file in HDFS, respectively.
In addition these also wrap the underlying operations to make them atomic.
They both implement the :func:`~luigi.file.LocalTarget.open` method which returns a stream object that
could be read (``mode='r'``) from or written to (``mode='w'``).
Both :class:`~luigi.file.LocalTarget` and :class:`~luigi.hdfs.HdfsTarget` also optionally take a format parameter.
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
general you want to allow some form of parametrization of it.
For instance, if your Task class runs a Hadoop job to create a report every night,
you probably want to make the date a parameter of the class.

    .. figure:: task_parameters.png
       :alt: Tasks with parameters
