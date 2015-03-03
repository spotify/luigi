API Overview
------------

There are two fundamental building blocks of Luigi -
the :class:`~luigi.task.Task` class and the :class:`~luigi.target.Target` class.
Both are abstract classes and expect a few methods to be implemented.
In addition to those two concepts,
the :class:`~luigi.parameter.Parameter` class is an important concept that governs how a Task is run.

Target
~~~~~~

Broadly speaking,
the Target class corresponds to a file on a disk,
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

.. _Parameter:

Parameter
~~~~~~~~~

The Task class corresponds to some type of job that is run, but in
general you want to allow some form of parametrization of it.
For instance, if your Task class runs a Hadoop job to create a report every night,
you probably want to make the date a parameter of the class.

In Python this is generally done by adding arguments to the constructor,
but Luigi requires you to declare these parameters instantiating
:class:`~luigi.parameter.Parameter` objects on the class scope:

.. code:: python

    class DailyReport(luigi.hadoop.JobTask):
        date = luigi.DateParameter(default=datetime.date.today())
        # ...

By doing this, Luigi can do take care of all the boilerplate code that
would normally be needed in the constructor.
Internally, the DailyReport object can now be constructed by running
``DailyReport(datetime.date(2012, 5, 10))`` or just ``DailyReport()``.
Luigi also creates a command line parser that automatically handles the
conversion from strings to Python types.
This way you can invoke the job on the command line eg. by passing ``--date 2012-15-10``.

The parameters are all set to their values on the Task object instance,
i.e.

.. code:: python

    d = DailyReport(datetime.date(2012, 5, 10))
    print d.date

will return the same date that the object was constructed with.
Same goes if you invoke Luigi on the command line.

Tasks are uniquely identified by their class name and values of their
parameters.
In fact, within the same worker, two tasks of the same class with
parameters of the same values are not just equal, but the same instance:

.. code:: python

    >>> import luigi
    >>> import datetime
    >>> class DateTask(luigi.Task):
    ...   date = luigi.DateParameter()
    ... 
    >>> a = datetime.date(2014, 1, 21)
    >>> b = datetime.date(2014, 1, 21)
    >>> a is b
    False
    >>> c = DateTask(date=a)
    >>> d = DateTask(date=b)
    >>> c
    DateTask(date=2014-01-21)
    >>> d
    DateTask(date=2014-01-21)
    >>> c is d
    True

However, if a parameter is created with ``significant=False``,
it is ignored as far as the Task signature is concerned.
Tasks created with only insignificant parameters differing have the same signature but
are not the same instance:

.. code:: python

    >>> class DateTask2(DateTask):
    ...   other = luigi.Parameter(significant=False)
    ... 
    >>> c = DateTask2(date=a, other="foo")
    >>> d = DateTask2(date=b, other="bar")
    >>> c
    DateTask2(date=2014-01-21)
    >>> d
    DateTask2(date=2014-01-21)
    >>> c.other
    'foo'
    >>> d.other
    'bar'
    >>> c is d
    False
    >>> hash(c) == hash(d)
    True

Python is not a strongly typed language and you don't have to specify the types
of any of your parameters.
You can simply use the base class :class:`~luigi.parameter.Parameter` if you don't care.
In fact, the reason :class:`~luigi.parameter.DateParameter` et al exist is just in order to
support command line interaction and make sure to convert the input to
the corresponding type (i.e. datetime.date instead of a string).

Setting parameter value for other classes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

All parameters are also exposed on a class level on the command line interface.
For instance, say you have classes TaskA and TaskB:

.. code:: python

    class TaskA(luigi.Task):
        x = luigi.Parameter()

    class TaskB(luigi.Task):
        y = luigi.Parameter()


You can run ``TaskB`` on the command line: ``python script.py TaskB --y 42``.
But you can also set the class value of ``TaskA`` by running
``python script.py TaskB --y 42 --TaskA-x 43``.
This sets the value of ``TaskA.x`` to 43 on a *class* level.
It is still possible to override it inside Python if you instantiate ``TaskA(x=44)``.

All parameters can also be set from the configuration file.
For instance, you can put this in the config:

.. code:: console

    [TaskA]
    x: 45


Just as in the previous case, this will set the value of ``TaskA.x`` to 45 on the *class* level.
And likewise, it is still possible to override it inside Python if you instantiate ``TaskA(x=44)``.

Parameters are resolved in the following order of decreasing priority:

1. Any value passed to the constructor, or task level value set on the command line (applies on an instance level)
2. Any value set on the command line (applies on a class level)
3. Any configuration option (applies on a class level)
4. Any default value provided to the parameter (applies on a class level)

See the :class:`~luigi.parameter.Parameter` class for more information.

.. _Task.requires:

Task.requires
~~~~~~~~~~~~~

The :func:`~luigi.task.Task.requires` method is used to specify dependencies on other Task object,
which might even be of the same class.
For instance, an example implementation could be

.. code:: python

    def requires(self):
        return OtherTask(self.date), DailyReport(self.date - datetime.timedelta(1))

In this case, the DailyReport task depends on two inputs created earlier,
one of which is the same class.
requires can return other Tasks in any way wrapped up within dicts/lists/tuples/etc.

Requiring another Task
~~~~~~~~~~~~~~~~~~~~~~

Note that :func:`~luigi.task.Task.requires` can *not* return a :class:`~luigi.target.Target` object.
If you have a simple Target object that is created externally
you can wrap it in a Task class like this:

.. code:: python

    class LogFiles(luigi.Task):
        def output(self):
            return luigi.hdfs.HdfsTarget('/log')

This also makes it easier to add parameters:

.. code:: python

    class LogFiles(luigi.Task):
        date = luigi.DateParameter()
        def output(self):
            return luigi.hdfs.HdfsTarget(self.date.strftime('/log/%Y-%m-%d'))

.. _Task.output:

Task.output
~~~~~~~~~~~

The :func:`~luigi.task.Task.output` method returns one or more :class:`~luigi.target.Target` objects.
Similarly to requires, can return wrap them up in any way that's convenient for you.
However we recommend that any :class:`~luigi.task.Task` only return one single :class:`~luigi.target.Target` in output.
If multiple outputs are returned,
atomicity will be lost unless the :class:`~luigi.task.Task` itself can ensure that each :class:`~luigi.target.Target` is atomically created.
(If atomicity is not of concern, then it is safe to return multiple :class:`~luigi.target.Target` objects.)

.. code:: python

    class DailyReport(luigi.Task):
        date = luigi.DateParameter()
        def output(self):
            return luigi.hdfs.HdfsTarget(self.date.strftime('/reports/%Y-%m-%d'))
        # ...

.. _Task.run:

Task.run
~~~~~~~~

The :func:`~luigi.task.Task.run` method now contains the actual code that is run.
When you are using Task.requires_ and Task.run_ Luigi breaks down everything into two stages.
First it figures out all dependencies between tasks,
then it runs everything.
The :func:`~luigi.task.Task.input` method is an internal helper method that just replaces all Task objects in requires
with their corresponding output.
An example:

.. code:: python

    class TaskA(luigi.Task):
        def output(self):
            return luigi.LocalTarget('xyz')

    class FlipLinesBackwards(luigi.Task):
        def requires(self):
            return TaskA()

        def output(self):
            return luigi.LocalTarget('abc')

        def run(self):
            f = self.input().open('r') # this will return a file stream that reads from "xyz"
            g = self.output().open('w')
            for line in f:
                g.write('%s\n', ''.join(reversed(line.strip().split()))
            g.close() # needed because files are atomic

.. _Task.input:

Task.input
~~~~~~~~~~

As seen in the example above, :class:`~luigi.task.Task` is a wrapper around Task.requires_ that
returns the corresponding Target objects instead of Task objects.
Anything returned by Task.requires_ will be transformed, including lists,
nested dicts, etc.
This can be useful if you have many dependencies:

.. code:: python

    class TaskWithManyInputs(luigi.Task):
        def requires(self):
            return {'a': TaskA(), 'b': [TaskB(i) for i in xrange(100)]}

        def run(self):
            f = self.input()['a'].open('r')
            g = [y.open('r') for y in self.input()['b']]


Dynamic dependencies
~~~~~~~~~~~~~~~~~~~~

Sometimes you might not know exactly what other tasks to depend on until runtime.
In that case, Luigi provides a mechanism to specify dynamic dependencies.
If you yield another :class:`~luigi.task.Task` in the Task.run_ method,
the current task will be suspended and the other task will be run.
You can also return a list of tasks.

.. code:: python

    class MyTask(luigi.Task):
        def run(self):
            other_target = yield OtherTask()

	    # dynamic dependencies resolve into targets
	    f = other_target.open('r')


This mechanism is an alternative to Task.requires_ in case
you are not able to build up the full dependency graph before running the task.
It does come with some constraints:
the Task.run_ method will resume from scratch each time a new task is yielded.
In other words, you should make sure your Task.run_ method is idempotent.
(This is good practice for all Tasks in Luigi, but especially so for tasks with dynamic dependencies).

For an example of a workflow using dynamic dependencies, see
`examples/dynamic_requirements.py <https://github.com/spotify/luigi/blob/master/examples/dynamic_requirements.py>`_.

.. _Events:

Events and callbacks
~~~~~~~~~~~~~~~~~~~~

Luigi has a built-in event system that
allows you to register callbacks to events and trigger them from your own tasks.
You can both hook into some pre-defined events and create your own.
Each event handle is tied to a Task class and
will be triggered only from that class or
a subclass of it.
This allows you to effortlessly subscribe to events only from a specific class (e.g. for hadoop jobs).

.. code:: python

    @luigi.Task.event_handler(luigi.Event.SUCCESS)
    def celebrate_success(task):
        """Will be called directly after a successful execution
           of `run` on any Task subclass (i.e. all luigi Tasks)
        """
        ...

    @luigi.hadoop.JobTask.event_handler(luigi.Event.FAILURE)
    def mourn_failure(task, exception):
        """Will be called directly after a failed execution
           of `run` on any JobTask subclass
        """
        ...

    luigi.run()


But I just want to run a Hadoop job?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Hadoop code is integrated in the rest of the Luigi code because
we really believe almost all Hadoop jobs benefit from being part of some sort of workflow.
However, in theory, nothing stops you from using the :class:`~luigi.hadoop.JobTask` class (and also :class:`~luigi.hdfs.HdfsTarget`)
without using the rest of Luigi.
You can simply run it manually using

.. code:: python

    MyJobTask('abc', 123).run()

You can use the hdfs.HdfsTarget class anywhere by just instantiating it:

.. code:: python

    t = luigi.hdfs.HdfsTarget('/tmp/test.gz', format=format.Gzip)
    f = t.open('w')
    # ...
    f.close() # needed

.. _Task.priority:

Task priority
~~~~~~~~~~~~~

The scheduler decides which task to run next from
the set of all task that have all their dependencies met.
By default, this choice is pretty arbitrary,
which is fine for most workflows and situations.

If you want to have some control on the order of execution of available tasks,
you can set the ``priority`` property of a task,
for example as follows:

.. code:: python

    # A static priority value as a class constant:
    class MyTask(luigi.Task):
        priority = 100
        # ...

    # A dynamic priority value with a "@property" decorated method:
    class OtherTask(luigi.Task):
        @property
        def priority(self):
            if self.date > some_threshold:
                return 80
            else:
                return 40
        # ...

Tasks with a higher priority value will be picked before tasks with a lower priority value.
There is no predefined range of priorities,
you can choose whatever (int or float) values you want to use.
The default value is 0.

Warning: task execution order in Luigi is influenced by both dependencies and priorities, but
in Luigi dependencies come first.
For example:
if there is a task A with priority 1000 but still with unmet dependencies and
a task B with priority 1 without any pending dependencies,
task B will be picked first.


Instance caching
~~~~~~~~~~~~~~~~

In addition to the stuff mentioned above,
Luigi also does some metaclass logic so that
if e.g. ``DailyReport(datetime.date(2012, 5, 10))`` is instantiated twice in the code,
it will in fact result in the same object.
