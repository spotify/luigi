Luigi Patterns
--------------

Code Reuse
~~~~~~~~~~

One nice thing about Luigi is that it's super easy to depend on tasks defined in other repos.
It's also trivial to have "forks" in the execution path,
where the output of one task may become the input of many other tasks.

Currently no semantics for "intermediate" output is supported,
meaning that all output will be persisted indefinitely.
The upside of that is that if you try to run X -> Y, and Y crashes,
you can resume with the previously built X.
The downside is that you will have a lot of intermediate results on your file system.
A useful pattern is to put these files in a special directory and
have some kind of periodical garbage collection clean it up.

Triggering Many Tasks
~~~~~~~~~~~~~~~~~~~~~

A convenient pattern is to have a dummy Task at the end of several
dependency chains, so you can trigger a multitude of pipelines by
specifying just one task in command line, similarly to how e.g. `make <http://www.gnu.org/software/make/>`_
works.

.. code:: python

    class AllReports(luigi.Task):
        date = luigi.DateParameter(default=datetime.date.today())
        def requires(self):
            yield SomeReport(self.date)
            yield SomeOtherReport(self.date)
            yield CropReport(self.date)
            yield TPSReport(self.date)
            yield FooBarBazReport(self.date)

This simple task will not do anything itself, but will invoke a bunch of
other tasks. Per each invocation Luigi will perform as many of the pending
jobs as possible (those which have all their dependencies present).

Triggering recurring tasks
~~~~~~~~~~~~~~~~~~~~~~~~~~

A common requirement is to have a daily report (or something else)
produced every night. Sometimes for various reasons tasks will keep
crashing or lacking their required dependencies for more than a day
though, which would lead to a missing deliverable for some date. Oops.

To ensure that the above AllReports task is eventually completed for
every day (value of date parameter), one could e.g. add a loop in
requires method to yield dependencies on the past few days preceding
self.date. Then, so long as Luigi keeps being invoked, the backlog of
jobs would catch up nicely after fixing intermittent problems.

Luigi actually comes with a reusable tool for achieving this, called
RangeDailyBase (resp. RangeHourlyBase). Simply putting

.. code:: console

	luigi --module all_reports RangeDailyBase --of AllReports --start 2015-01-01

in your crontab will easily keep gaps from occurring from 2015-01-01
onwards. NB - it will not always loop over everything from 2015-01-01
till current time though, but rather a maximum of 3 months ago by
default - see RangeDailyBase documentation for this and more knobs
for tweaking behavior. See also Monitoring below.

Efficiently triggering recurring tasks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

RangeDailyBase, described above, is named like that because a more
efficient subclass exists, RangeDaily (resp. RangeHourly), tailored for
hundreds of task classes scheduled concurrently with contiguousness
requirements spanning years (which would incur redundant completeness
checks and scheduler overload using the naive looping approach.) Usage:

.. code:: console

	luigi --module all_reports RangeDaily --of AllReports --start 2015-01-01

It has the same knobs as RangeDailyBase, with some added requirements.
Namely the task must implement an efficient bulk_complete method, or
must be writing output to file system Target with date parameter value
consistently represented in the file path.

Backfilling tasks
~~~~~~~~~~~~~~~~~

Also a common use case, sometimes you have tweaked existing recurring
task code and you want to schedule recomputation of it over an interval
of dates for that or another reason. Most conveniently it is achieved
with the above described range tools, just with both start (inclusive)
and stop (exclusive) parameters specified:

.. code:: console

	luigi --module all_reports RangeDaily --of AllReportsV2 --start 2014-10-31 --stop 2014-12-25

Monitoring task pipelines
~~~~~~~~~~~~~~~~~~~~~~~~~

Set error-email in :doc:`../configuration` to receive notifications whenever
tasks crash. (This can get noisy with growing numbers of tasks and
intermittent failures.)

The above mentioned range tools for recurring tasks not only implement
reliable scheduling for you, but also emit events which you can use to
set up delay monitoring. That way you can implement alerts for when
jobs are stuck for prolonged periods lacking input data or otherwise
requiring attention.

