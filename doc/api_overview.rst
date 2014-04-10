API Overview
------------

There are two fundamental building blocks of Luigi - the *Task* class
and the *Target* class. Both are abstract classes and expect a few
methods to be implemented. In addition to those two concepts, the
*Parameter* class is an important concept that governs how a Task is
run.

Target
~~~~~~

Broadly speaking, the Target class corresponds to a file on a disk. Or a
file on HDFS. Or some kind of a checkpoint, like an entry in a database.
Actually, the only method that Targets have to implement is the *exists*
method which returns True if and only if the Target exists.

In practice, implementing Target subclasses is rarely needed. You can
probably get pretty far with the *LocalTarget* and *hdfs.HdfsTarget*
classes that are available out of the box. These directly map to a file
on the local drive, or a file in HDFS, respectively. In addition these
also wrap the underlying operations to make them atomic. They both
implement the *open(flag)* method which returns a stream object that
could be read (flag = 'r') from or written to (flag = 'w'). Both
LocalTarget and hdfs.HdfsTarget also optionally take a format parameter.
Luigi comes with Gzip support by providing *format=format.Gzip* . Adding
support for other formats is pretty simple.

Task
~~~~

The *Task* class is a bit more conceptually interesting because this is
where computation is done. There are a few methods that can be
implemented to alter its behavior, most notably *run*, *output* and
*requires*.

The Task class corresponds to some type of job that is run, but in
general you want to allow some form of parametrization of it. For
instance, if your Task class runs a Hadoop job to create a report every
night, you probably want to make the date a parameter of the class.

Parameter
^^^^^^^^^

In Python this is generally done by adding arguments to the constructor,
but Luigi requires you to declare these parameters instantiating
Parameter objects on the class scope:

.. code:: python

    class DailyReport(luigi.hadoop.JobTask):
        date = luigi.DateParameter(default=datetime.date.today())
        # ...

By doing this, Luigi can do take care of all the boilerplate code that
would normally be needed in the constructor. Internally, the DailyReport
object can now be constructed by running
*DailyReport(datetime.date(2012, 5, 10))* or just *DailyReport()*. Luigi
also creates a command line parser that automatically handles the
conversion from strings to Python types. This way you can invoke the job
on the command line eg. by passing *--date 2012-15-10*.

The parameters are all set to their values on the Task object instance,
i.e.

.. code:: python

    d = DailyReport(datetime.date(2012, 5, 10))
    print d.date

will return the same date that the object was constructed with. Same
goes if you invoke Luigi on the command line.

Tasks are uniquely identified by their class name and values of their
parameters. In fact, within the same worker, two tasks of the same class
with parameters of the same values are not just equal, but the same
instance:

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

However, if a parameter is created with *significant=False*, it is
ignored as far as the Task signature is concerned. Tasks created with
only insignificant parameters differing have the same signature, but are
not the same instance:

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

Python is not a typed language and you don't have to specify the types
of any of your parameters. You can simply use *luigi.Parameter* if you
don't care. In fact, the reason DateParameter et al exist is just in
order to support command line interaction and make sure to convert the
input to the corresponding type (i.e. datetime.date instead of a
string).

Task.requires
^^^^^^^^^^^^^

The *requires* method is used to specify dependencies on other Task
object, which might even be of the same class. For instance, an example
implementation could be

.. code:: python

    def requires(self):
        return OtherTask(self.date), DailyReport(self.date - datetime.timedelta(1))

In this case, the DailyReport task depends on two inputs created
earlier, one of which is the same class. requires can return other Tasks
in any way wrapped up within dicts/lists/tuples/etc.

Task.output
^^^^^^^^^^^

The *output* method returns one or more Target objects. Similarly to
requires, can return wrap them up in any way that's convenient for you.
However we recommend that any Task only return one single Target in
output. If multiple outputs are returned, atomicity will be lost unless
the Task itself can ensure that the Targets are atomically created. (If
atomicity is not of concern, then it is safe to return multiple Target
objects.)

.. code:: python

    class DailyReport(luigi.Task):
        date = luigi.DateParameter()
        def output(self):
            return luigi.hdfs.HdfsTarget(self.date.strftime('/reports/%Y-%m-%d'))
        # ...

Task.run
^^^^^^^^

The *run* method now contains the actual code that is run. Note that
Luigi breaks down everything into two stages. First it figures out all
dependencies between tasks, then it runs everything. The *input()*
method is an internal helper method that just replaces all Task objects
in requires with their corresponding output. For instance, in this
example

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

Events and callbacks
^^^^^^^^^^^^^^^^^^^^

Luigi has a built-in event system that allows you to register callbacks
to events and trigger them from your own tasks. You can both hook into
some pre-defined events and create your own. Each event handle is tied
to a Task class, and will be triggered only from that class or a
subclass of it. This allows you to effortlessly subscribe to events only
from a specific class (e.g. for hadoop jobs).

.. code:: python

    @luigi.Task.event_handler(luigi.Event.SUCCESS):
    def celebrate_success(self, task):
        """Will be called directly after a successful execution
           of `run` on any Task subclass (i.e. all luigi Tasks)
        """
        ...

    @luigi.hadoop.JobTask.event_handler(luigi.Event.FAILURE):
    def mourn_failure(self, task, exception):
        """Will be called directly after a failed execution
           of `run` on any JobTask subclass
        """
        ...

    luigi.run()


But I just want to run a Hadoop job?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Hadoop code is integrated in the rest of the Luigi code because we
really believe almost all Hadoop jobs benefit from being part of some
sort of workflow. However, in theory, nothing stops you from using the
hadoop.JobTask class (and also hdfs.HdfsTarget) without using the rest
of Luigi. You can simply run it manually using

.. code:: python

    MyJobTask('abc', 123).run()

You can use the hdfs.HdfsTarget class anywhere by just instantiating it:

.. code:: python

    t = luigi.hdfs.HdfsTarget('/tmp/test.gz', format=format.Gzip)
    f = t.open('w')
    # ...
    f.close() # needed

Instance caching
^^^^^^^^^^^^^^^^

In addition to the stuff mentioned above, Luigi also does some metaclass
logic so that if eg. *DailyReport(datetime.date(2012, 5, 10))* is
instantiated twice in the code, it will in fact result in the same
object. This is needed so that each Task is run only once.