=============
API reference
=============

.. py:currentmodule:: luigi

This chapter contains detailed API documentation for Luigi.

The key parts of the Luigi API are as follows:

:py:class:`~luigi.task.Task`:
  A :py:class:`~luigi.task.Task` is the mechanism for describing a unit of work in
  Luigi. Luigi includes a number of useful subclasses of the Task class, such
  as :py:class:`~luigi.hadoop.JobTask`, :py:class:`~luigi.hadoop_jar.HadoopJarJobTask`,
  :py:class:`~luigi.postgres.CopyToTable`, :py:class:`~luigi.scalding.ScaldingJobTask`, and
  more.

:py:class:`~luigi.parameter.Parameter`:
  A :py:class:`~luigi.parameter.Parameter` describes a (optionally typed) runtime argument
  to a Task. Luigi auto-exposes Parameters on the command line, and
  automatically parses Parameters to their type. Examples include:
  :py:class:`~luigi.parameter.IntParameter`, :py:class:`~luigi.parameter.DateParameter`,
  :py:class:`~luigi.parameter.DateInterval`, :py:class:`~luigi.parameter.FloatParameter`, and more.


Tasks
=====

.. automodule:: luigi.task
   :members:


Parameters
==========

.. automodule:: luigi.parameter
   :members:


.. vim: set spell spelllang=en: