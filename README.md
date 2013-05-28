![Luigi](https://raw.github.com/spotify/luigi/master/doc/luigi.png)

Luigi is a Python package that helps you build complex pipelines of batch jobs. It handles dependency resolution, workflow management, visualization, handling failures, command line integration, and much more.

The purpose of Luigi is to address all the plumbing typically associated with long-running batch processes. You want to chain many tasks, automate them, and failures *will* happen. These tasks can be anything, but typically long running things like [Hadoop](http://hadoop.apache.org/) jobs, dumping data to/from databases, running machine learning algorithms, or anything else. 

There are other software packages that focus on lower level aspects of data processing, like [Hive](http://hive.apache.org/), [Pig](http://pig.apache.org/), [Cascading](http://www.cascading.org/). Luigi is not a framework to replace these. Instead it helps you stitch many tasks together into long-running pipelines that can comprise thousands of tasks and take weeks to complete. Luigi takes care of a lot of the workflow management so that you can focus on the concrete tasks and their dependencies.

Luigi comes with a toolbox of several common tasks that you use. It includes native Python support for running mapreduce jobs in Hadoop. It also comes with file system abstractions for HDFS and local files that also ensures all file system operations are atomic. This is important because it means your data pipeline will not crash in a state containing partial data.

## Dependency graph example

Just to give you an idea of what Luigi does, this is a screen shot from something we are running in production. Using Luigi's visualizer, we get a nice visual overview of the dependency graph of the workflow. At the top of the graph are two data sets containing external data dumps. Each node represents a task which has to be run. Green tasks are already completed whereas white tasks are yet to be run. Most of these tasks are Hadoop job, but at the end of the graph is a task ingesting data from HDFS into Cassandra.

![Dependency graph](https://raw.github.com/erikbern/luigi/master/doc/user_recs_graph.png)

## Background

We use Luigi internally at [Spotify](http://www.spotify.com/) to run 1000s of tasks every day, organized in complex dependency graphs. Most of these tasks are Hadoop job. Luigi provides an infrastructure that powers all kinds of stuff including recommendations, toplists, A/B test analysis, external reports, internal dashboards, etc. Luigi grew out of the realization that powerful abstractions for the batch processing can help programmers focus on the most important bits and leave the rest (the boilerplate) to the framework.

Conceptually, Luigi similar to [GNU Make](http://www.gnu.org/software/make/) where you have certain tasks and these tasks in turn may have dependencies on other tasks. There are also some similarities to [Oozie](http://incubator.apache.org/oozie/) and [Azkaban](http://data.linkedin.com/opensource/azkaban). One major difference is that Luigi is not just built specifically for Hadoop, and it's easy to extend it with other kinds of tasks.

Everything in Luigi is in Python. Instead of XML configuration or similar external data files, the dependency graph is specified *within Python*. This makes it easy to build up complex dependency graphs of tasks, where the dependencies can involve date algebra or recursive references to other versions of the same task.

## Installing

Downloading and running *python setup.py install* should be enough. Note that you probably want [Tornado](http://www.tornadoweb.org/). Also [Mechanize](http://wwwsearch.sourceforge.net/mechanize/) is optional if you want to run Hadoop jobs since it makes debugging easier. If you want to run Hadoop you should also make sure to edit /etc/luigi/client.cfg and add something like this

    [hadoop]
    jar: /usr/lib/hadoop-xyz/hadoop-streaming-xyz-123.jar

## Examples

Let's begin with the classic WordCount example. We'll show a non-Hadoop version then later show how it can be implemented as a Hadoop job. The examples are all available in the examples/ directory.

### Example 1 - Simple wordcount

Assume you have a bunch of text files dumped onto disk every night by some external process. These text files contain English text, and we want to monitor the top few thousand words over arbitrary date periods. Hopefully this doesn't sound too contrived - you could imagine mapping "words" to "artists" to get an idea of a real application at Spotify.

```python
import luigi
import datetime

class InputText(luigi.ExternalTask):
    ''' This class represents something that was created elsewhere by an external process, so all we want to do is to implement the output method
    '''
    date = luigi.DateParameter()

    def output(self):
        return luigi.LocalTarget(self.date.strftime('/var/tmp/text/%Y-%m-%d.txt'))

class WordCount(luigi.Task):
    date_interval = luigi.DateIntervalParameter()

    def requires(self):
        return [InputText(date) for date in self.date_interval.dates()]

    def output(self):
        return luigi.LocalTarget('/var/tmp/text-count/%s' % (self.date_interval))

    def run(self):
        count = {}
        for file in self.input(): # The input() method is a wrapper around requires() that returns Target objects
            for line in file.open('r'): # Target objects are a file system/format abstraction and this will return a file stream object
                for word in line.strip().split():
                    count[word] = count.get(word, 0) + 1

        # output data
        f = self.output().open('w')
        for word, count in count.iteritems():
            f.write("%s\t%d\n" % (word, count))
        f.close() # Note that this is essential because file system operations are atomic

if __name__ == '__main__':
    luigi.run()
```

Now, provided you have a bunch of input files in /var/tmp/text/ (you can generate them using examples/generate\_input.py), try running this using eg

    $ python wordcount.py WordCount --local-scheduler --date 2012-08-01

You can also try to view the manual using --help which will give you an overview of the options:

    usage: wordcount.py [-h] [--local-scheduler] [--scheduler-host SCHEDULER_HOST]
                        [--lock] [--lock-pid-dir LOCK_PID_DIR] [--workers WORKERS]
                        [--date-interval DATE_INTERVAL]

    optional arguments:
      -h, --help            show this help message and exit
      --local-scheduler     Use local scheduling
      --scheduler-host SCHEDULER_HOST
                            Hostname of machine running remote scheduler [default:
                            localhost]
      --lock                Do not run if the task is already running
      --lock-pid-dir LOCK_PID_DIR
                            Directory to store the pid file [default:
                            /var/tmp/luigi]
      --workers WORKERS     Maximum number of parallel tasks to run [default: 1]
      --date-interval DATE_INTERVAL
                            WordCount.date_interval

Running the command again will do nothing because the output file is already created. Note that unlike Makefile, the output will not be recreated when any of the input files is modified. You need to delete the output file manually.

### Using the central planner

The --local-scheduler flag tells Luigi not to connect to a central scheduler. This is recommended in order to get started and or for development purposes. At the point where you start putting things in production we strongly recommend running the central scheduler server. In addition to provide locking so the same task is not run by multiple processes at the same time, this server also provides a pretty nice visualization of your current work flow.

If you drop the *--local-scheduler* flag, your script will try to connect to the central planner, by default at localhost port 8081. If you run

    PYTHONPATH=. python bin/luigid

in the background and then run

    $ python wordcount.py --date 2012-W03

then in fact your script will now do the scheduling through a centralized server. You need [Tornado](http://www.tornadoweb.org/) for this to work. It is available in Debian as *python-tornado*.

Launching *http://localhost:8081* should show something like this:

![Wordcount](https://raw.github.com/spotify/luigi/master/doc/wordcount.png)

The green boxes mean that the job is already done. If you keep invoking the script with a bunch of different date intervals it might look like this after a while:

![Wordcount](https://raw.github.com/spotify/luigi/master/doc/wordcount_more.png)

You can drag and scroll to re-center and zoom. The visualizer will automatically prune all done tasks after a while.

### Example 2 - Hadoop WordCount

Luigi also provides support for Hadoop jobs straight out of the box. The interface is similar to mrjob but each job class is now a Luigi Task that can also define their dependencies and output files.

EC2 is unfortunately not supported at this point. We have some old code for this (using Python [boto](http://github.com/boto/boto)) and would love to help anyone interested in getting it running.

```python
import luigi, luigi.hadoop, luigi.hdfs
import datetime

# To make this run, you probably want to edit /etc/luigi/client.cfg and add something like:
#
# [hadoop]
# jar: /usr/lib/hadoop-xyz/hadoop-streaming-xyz-123.jar

class InputText(luigi.ExternalTask):
    date = luigi.DateParameter()
    def output(self):
        return luigi.hdfs.HdfsTarget(self.date.strftime('/tmp/text/%Y-%m-%d.txt'))

class WordCount(luigi.hadoop.JobTask):
    date_interval = luigi.DateIntervalParameter()

    def requires(self):
        return [InputText(date) for date in self.date_interval.dates()]

    def output(self):
        return luigi.hdfs.HdfsTarget('/tmp/text-count/%s' % self.date_interval)

    def mapper(self, line):
        for word in line.strip().split():
            yield word, 1

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    luigi.run()
```

Luigi also has support for combiners and counters. If you want to bundle modules with your job, you can use that either by overriding the *extra_packages* method, or by invoking the *luigi.hadoop.attach* function anywhere in your code.

Run the example using

    $ python wordcount_hadoop.py WordCount --date 2012-W03

This will yield a familiar overview

![Wordcount](https://raw.github.com/spotify/luigi/master/doc/wordcount_hadoop.png)

The blue box means that the job is currently running. If it fails, it will become red:

![Wordcount](https://raw.github.com/spotify/luigi/master/doc/wordcount_hadoop_failed.png)

In case your job crashes remotely due to any Python exception, Luigi will try to fetch the traceback and print it on standard output. You need [Mechanize](http://wwwsearch.sourceforge.net/mechanize/) for it to work and you also need connectivity to your tasktrackers.

## Conceptual overview

There are two fundamental building blocks of Luigi - the *Task* class and the *Target* class. Both are abstract classes and expect a few methods to be implemented. In addition to those two concepts, the *Parameter* class is an important concept that governs how a Task is run.

### Target

Broadly speaking, the Target class corresponds to a file on a disk. Or a file on HDFS. Or some kind of a checkpoint, like an entry in a database. Actually, the only method that Targets have to implement is the *exists* method which returns True if and only if the Target exists.

In practice, implementing Target subclasses is rarely needed. You can probably get pretty far with the *LocalTarget* and *hdfs.HdfsTarget* classes that are available out of the box. These directly map to a file on the local drive, or a file in HDFS, respectively. In addition these also wrap the underlying operations to make them atomic. They both implement the *open(flag)* method which returns a stream object that could be read (flag = 'r') from or written to (flag = 'w'). Both LocalTarget and hdfs.HdfsTarget also optionally take a format parameter. Luigi comes with Gzip support by providing *format=format.Gzip* . Adding support for other formats is pretty simple.

### Task

The *Task* class is a bit more conceptually interesting because this is where computation is done. There is a few methods that can be implemented to alter its behavior, most notably *run*, *output* and *requires*.

The Task class corresponds to some type of job that is run, but in general you want to allow some form of parametrization of it. For instance, if your Task class runs a Hadoop job to create a report every night, you probably want to make the date a parameter of the class.

#### Parameter

Now, in Python this is generally done by adding arguments to the constructor. Luigi requires you to declare these parameters instantiating Parameter objects on the class scope:

```python
class DailyReport(luigi.hadoop.JobTask):
    date = luigi.DateParameter(default=datetime.date.today())
    # ...
```

By doing this, Luigi can do take care of all the boiler plate code that would normally be needed in the constructor. Internally, the DailyReport object can now be constructed by running *DailyReport(datetime.date(2012, 5, 10))* or just *DailyReport()*. Luigi also creates a command line parser that automatically handles the conversion from strings to Python types. This way you can invoke the job on the command line eg. by passing *--date 2012-15-10*.

The parameters are all set to their values on the Task object instance, i.e.

```python
d = DailyReport(datetime.date(2012, 5, 10))
print d.date
```

will return the same date that the object was constructed with. Same goes if you invoke Luigi on the command line.

Python is not a typed language and you don't have to specify the types of any of your parameters. You can simply use *luigi.Parameter* if you don't care. In fact, the reason DateParameter et al exist is just in order to support command line interaction and make sure to convert the input to the corresponding type (i.e. datetime.date instead of a string).

#### Task.requires

The *requires* method is used to specify dependencies on other Task object, which might even be of the same class. For instance, an example implementation could be

```python
def requires(self):
    return OtherTask(self.date), DailyReport(self.date - datetime.timedelta(1))
```

In this case, the DailyReport task depends on two inputs created earlier, one of which is the same class. requires can return other Tasks in any way wrapped up within dicts/lists/tuples etc

#### Task.output

The *output* method returns one or more Target objects. Similarly to requires, can return wrap them up in any way that's convenient for you. However we strongly recommend that any Task only returns one single Target in output.

```python
class DailyReport(luigi.Task):
    date = luigi.DateParameter()
    def output(self):
        return luigi.hdfs.HdfsTarget(self.date.strftime('/reports/%Y-%m-%d'))
    # ...
```


#### Task.run

The *run* method now contains the actual code that is run. Note that Luigi breaks down everything into two stages. First it figures out all dependencies between tasks, then it runs everything. The *input()* method is an internal helper method that just replaces all Task objects in requires with their corresponding output. For instance, in this example

```python
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
```

#### Running from the command line

Any task can be instantiated and run from the command line

```python
class MyTask(luigi.Task):
    x = IntParameter()
    y = IntParameter(default=45)
    def run(self):
        print self.x + self.y

if __name__ == '__main__':
       luigi.run()
```

You can run this task from the command line like this:

    python my_task.py MyTask --x 123 --y 456

You can also pass *main_task_cls=MyTask* to luigi.run() and that way you can invoke it simply using

    python my_task.py --x 123 --y 456

#### Executing a Luigi workflow

As seen above, command line integration is achieved by simply adding

```python
if __name__ == '__main__':
    luigi.run()
```

This will read the args from the command line (using argparse) and invoke everything.

In case you just want to run a Luigi chain from a Python script, you can do that internally without the command line integration. The code will look something like

```python
task = MyTask(123, 'xyz')
sch = scheduler.CentralPlannerScheduler()
w = worker.Worker(scheduler=sch)
w.add(task)
w.run()
```

#### Instance caching

In addition to the stuff mentioned above, Luigi also does some metaclass logic so that if eg. *DailyReport(datetime.date(2012, 5, 10))* is instantiated twice in the code, it will in fact result in the same object. This is needed so that each Task is run only once.

#### But I just want to run a Hadoop job?

The Hadoop code is integrated in the rest of the Luigi code because we really believe almost all Hadoop jobs benefit from being part of some sort of workflow. However, in theory, nothing stops you from using the hadoop.JobTask class (and also hdfs.HdfsTarget) without using the rest of Luigi. You can simply run it manually using

```python
MyJobTask('abc', 123).run()
```

You can use the hdfs.HdfsTarget class anywhere by just instantiating it:

```python
t = luigi.hdfs.HdfsTarget('/tmp/test.gz', format=format.Gzip)
f = t.open('w')
# ...
f.close() # needed
```

## More graph porn

Running the *test/fib_test.py* with *--n 200* yields a complex graph (albeit slightly artificial):

![Wordcount](https://raw.github.com/spotify/luigi/master/doc/fib.png)

Actually the resemblance with a G-clef is coincidental. Scroll and drag to zoom in:

![Wordcount](https://raw.github.com/spotify/luigi/master/doc/fib_zoomed.png)

## More info

Luigi is the sucessor to a couple of attempts that we weren't fully happy with. We learned a lot from our mistakes and some design decisions include:

* Straightforward command line integration.
* As little boiler plate as possible.
* Focus on job scheduling and dependency resolution, not a particular platform. In particular this means no limitation to Hadoop. Though Hadoop/HDFS support is built-in and is easy to use, this is just one of many types of things you can run.
* A file system abstraction where code doesn't have to care about where files are located.
* Atomic file system operations through this abstraction. If a task crashes it won't lead to a broken state.
* The depencies are decentralized. No big config file in XML. Each task just specifies which inputs it needs and cross-module dependencies are trivial.
* A web server that renders the dependency graph and does locking etc for free.
* Trivial to extend with new file systems, file formats and job types. You can easily write jobs that inserts a Tokyo Cabinet into Cassandra. Adding broad support S3, MySQL or Hive should be a stroll in the park. (and feel free to send us a patch when you're done!)
* Date algebra included.
* Lots of unit tests of the most basic stuff

It wouldn't be fair not to mention some limitations with the current design:

* Its focus is on batch processing so it's probably less useful for near real-time pipelines or continuously running processes.
* The assumption is that a each task is a sizable chunk of work. While you can probably schedule a few thousand jobs, it's not meant to scale beyond tens of thousands.
* Luigi maintains a strict separation between scheduling tasks and running them. Dynamic for-loops and branches are non-trivial to implement. For instance, it's tricky to iterate a numerical computation task until it converges.

It should actually be noted that all these limitations are not fundamental in any way. However, it would take some major refactoring work.

Also it should be mentioned that Luigi is named after the pipeline-running friend of Super Mario.

## Future ideas

* S3/EC2 - We have some old ugly code based on Boto that could be integrated in a day or two.
* Built in support for Pig/Hive.
* Better visualization tool - the layout gets pretty messy as the number of tasks grows.
* Integration with existing Hadoop frameworks like mrjob would be cool and probably pretty easy.
* Better support (without much boiler plate) for unittesting specific Tasks

## Getting help

* Find us on #luigi on freenode.
* Subscribe to the [luigi-user](http://groups.google.com/group/luigi-user/) group and ask a question.

## Want to contribute?

Awesome! Let us know if you have any ideas. Feel free to contact x@y.com where x = luigi and y = spotify.

