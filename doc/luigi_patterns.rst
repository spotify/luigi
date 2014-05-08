Luigi Patterns
--------------

Code Reuse
~~~~~~~~~~

One nice thing about Luigi is that it's super easy to depend on tasks
defined in other repos. It's also trivial to have "forks" in the
execution path, where the output of one task may become the input of
many other tasks.

Currently no semantics for "intermediate" output is supported, meaning
that all output will be persisted indefinitely. The upside of that is
that if you try to run X -> Y, and Y crashes, you can resume with the
previously built X. The downside is that you will have a lot of
intermediate results on your file system. A useful pattern is to put
these files in a special directory and have some kind of periodical
garbage collection clean it up.

Triggering Many Tasks
~~~~~~~~~~~~~~~~~~~~~

A common use case is to make sure some daily Hadoop job (or something
else) is run every night. Sometimes for various reasons things will
crash for more than a day though. A useful pattern is to have a dummy
Task at the end just declaring dependencies on the past few days of
tasks you want to run.

.. code:: python

    class AllReports(luigi.Task):
        date = luigi.DateParameter(default=datetime.date.today())
        lookback = luigi.IntParameter(default=14)
        def requires(self):
            for i in xrange(self.lookback):
               date = self.date - datetime.timedelta(i + 1)
               yield SomeReport(date), SomeOtherReport(date), CropReport(date), TPSReport(date), FooBarBazReport(date)    

This simple task will not do anything itself, but will invoke a bunch of
other tasks.