![Luigi](https://raw.github.com/spotify/luigi/master/doc/luigi.png)

Luigi is a Python package that helps you build complex pipelines of batch jobs. It handles dependency resolution, workflow management, visualization, handling failures, command line integration, and much more.

The purpose of Luigi is to address all the plumbing typically associated with long-running batch processes. You want to chain many tasks, automate them, and failures *will* happen. These tasks can be anything, but are typically long running things like [Hadoop](http://hadoop.apache.org/) jobs, dumping data to/from databases, running machine learning algorithms, or anything else.

There are other software packages that focus on lower level aspects of data processing, like [Hive](http://hive.apache.org/), [Pig](http://pig.apache.org/), or [Cascading](http://www.cascading.org/). Luigi is not a framework to replace these. Instead it helps you stitch many tasks together, where each task can be a Hive query, a Hadoop job in Java, a Python snippet, dumping a table from a database, or anything else. It's easy to build up long-running pipelines that comprise thousands of tasks and take days or weeks to complete. Luigi takes care of a lot of the workflow management so that you can focus on the tasks themselves and their dependencies. 

You can build pretty much any task you want, but Luigi also comes with a *toolbox* of several common task templates that you use. It includes native Python support for running mapreduce jobs in Hadoop, as well as Pig and Jar jobs. It also comes with filesystem abstractions for HDFS and local files that ensures all file system operations are atomic. This is important because it means your data pipeline will not crash in a state containing partial data.

Luigi was built at [Spotify](http://www.spotify.com/), mainly by [Erik Bernhardsson](https://github.com/erikbern) and [Elias Freider](https://github.com/freider), but many other people have contributed.

## Dependency graph example

Just to give you an idea of what Luigi does, this is a screen shot from something we are running in production. Using Luigi's visualizer, we get a nice visual overview of the dependency graph of the workflow. Each node represents a task which has to be run. Green tasks are already completed whereas yellow tasks are yet to be run. Most of these tasks are Hadoop jobs, but there are also some things that run locally and build up data files.

![Dependency graph](https://raw.github.com/erikbern/luigi/new-doc/doc/user_recs.png)

## Background

We use Luigi internally at [Spotify](http://www.spotify.com/) to run thousands of tasks every day, organized in complex dependency graphs. Most of these tasks are Hadoop jobs. Luigi provides an infrastructure that powers all kinds of stuff including recommendations, toplists, A/B test analysis, external reports, internal dashboards, etc. Luigi grew out of the realization that powerful abstractions for batch processing can help programmers focus on the most important bits and leave the rest (the boilerplate) to the framework.

Conceptually, Luigi is similar to [GNU Make](http://www.gnu.org/software/make/) where you have certain tasks and these tasks in turn may have dependencies on other tasks. There are also some similarities to [Oozie](http://incubator.apache.org/oozie/) and [Azkaban](http://data.linkedin.com/opensource/azkaban). One major difference is that Luigi is not just built specifically for Hadoop, and it's easy to extend it with other kinds of tasks.

Everything in Luigi is in Python. Instead of XML configuration or similar external data files, the dependency graph is specified *within Python*. This makes it easy to build up complex dependency graphs of tasks, where the dependencies can involve date algebra or recursive references to other versions of the same task. However, the workflow can trigger things not in Python, such as running Pig scripts or scp'ing files.

## Installing

Downloading and running *python setup.py install* should be enough. Note that you probably want [Tornado](http://www.tornadoweb.org/). Also [Mechanize](http://wwwsearch.sourceforge.net/mechanize/) is optional if you want to run Hadoop jobs since it makes debugging easier. See [Configuration](#configuration) for how to configure Luigi.

## Example workflow – top artists

This is a very simplified case of something we do at Spotify a lot. All user actions are logged to HDFS where we run a bunch of Hadoop jobs to transform the data. At some point we might end up with a smaller data set that we can bulk ingest into Cassandra, Postgres, or some other format.

For the purpose of this excercise, we want to aggregate all streams, and find the top 10 artists. We will then put it into Postgres.

This example is also available in *examples/top_artists.py*

### Step 1 - Aggregate artist streams

```python
class AggregateArtists(luigi.Task):
    date_interval = luigi.DateIntervalParameter()

    def output(self):
        return luigi.LocalTarget("data/artist_streams_%s.tsv" % self.date_interval)

    def requires(self):
        return [Streams(date) for date in self.date_interval]

    def run(self):
        artist_count = defaultdict(int)

        for input in self.input():
            with input.open('r') as in_file:
                for line in in_file:
                    timestamp, artist, track = line.strip().split()
                    artist_count[artist] += 1

        with self.output().open('w') as out_file:
            for artist, count in artist_count.iteritems():
                print >> out_file, artist, count
```

There are several pieces of this snippet that deserve more explanation.

* Any *Task* may be customized by instantiating one or more *Parameter* objects on the class level.
* The *output* method tells Luigi where the result of running the task will end up. The path can be some function of the parameters.
* The *requires* tasks specifies other tasks that we need to perform this task. In this case it's an external dump named *Streams* which takes the date as the argument.
* For plain Tasks, the *run* method implements the task. This could be anything, including calling subprocesses, performing long running number crunching, etc. For some subclasses of *Task* you don't have to implement the *run* method. For instance, for the *HadoopJobTask* subclass you implement a *mapper* and *reducer* instead.
* *HdfsTarget* is a built in class that makes it easy to read/write from/to HDFS. It also makes all file operations atomic, which is nice in case your script crashes for any reason.

### Running this locally

Try running this using eg.

    $ python examples/top_artists.py AggregateArtists --local-scheduler --date-interval 2012-06

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
                            AggregateArtists.date_interval

Running the command again will do nothing because the output file is already created. In that sense, any task in Luigi is *idempotent* because running it many times gives the same outcome as running it once. Note that unlike Makefile, the output will not be recreated when any of the input files is modified. You need to delete the output file manually.

The *--local-scheduler* flag tells Luigi not to connect to a scheduler server. This is not recommended for other purpose than just testing things.

### Step 1b - running this in Hadoop

Luigi comes with native Python Hadoop mapreduce support built in, and here is how this could look like, instead of the class above.

```python
class AggregateArtistsHadoop(luigi.hadoop.JobTask):
    date_interval = luigi.DateIntervalParameter()

    def output(self):
        return luigi.HdfsTarget("data/artist_streams_%s.tsv" % self.date_interval)

    def requires(self):
        return [StreamsHdfs(date) for date in self.date_interval]

    def mapper(self, line):
        timestamp, artist, track = line.strip().split()
        yield artist, 1
        
    def reducer(self, key, values):
        yield key, sum(values)
```

Note that `luigi.hadoop.JobTask` doesn't require you to implement a `run` method. Instead, you typically implement a `mapper` and `reducer` method.

### Step 2 – Find the top artists

At this point, we've counted the number of streams for each artists, for the full time period. We are left with a large file that contains mappings of artist -> count data, and we want to find the top 10 artists. Since we only have a few hundred thousand artists, and calculating artists is nontrivial to parallelize, we choose to do this not as a Hadoop job, but just as a plain old for-loop in Python.

```python
class Top10Artists(luigi.Task):
    date_interval = luigi.DateIntervalParameter()
    use_hadoop = luigi.BooleanParameter()

    def requires(self):
        if self.use_hadoop:
            return AggregateArtistsHadoop(self.date_interval)
        else:
            return AggregateArtists(self.date_interval)

    def output(self):
        return luigi.LocalTarget("data/top_artists_%s.tsv" % self.date_interval)

    def run(self):
        top_10 = nlargest(10, self._input_iterator())
        with self.output().open('w') as out_file:
            for streams, artist in top_10:
                print >> out_file, self.date_interval.date_a, self.date_interval.date_b, artist, streams

    def _input_iterator(self):
        with self.input().open('r') as in_file:
            for line in in_file:
                artist, streams = line.strip().split()
                yield int(streams), int(artist)
```

The most interesting thing here is that this task (*Top10Artists*) defines a dependency on the previous task (*AggregateArtists*). This means that if the output of *AggregateArtists* does not exist, the task will run before *Top10Artists*.

    $ python examples/top_artists.py Top10Artists --local-scheduler --date-interval 2012-07

This will run both tasks.

### Step 3 - Insert into Postgres

This mainly serves as an example of a specific subclass *Task* that doesn't require any code to be written. It's also an example of how you can define task templates that you can reuse for a lot of different tasks.

```python
class ArtistToplistToDatabase(luigi.postgres.CopyToTable):
    date_interval = luigi.DateIntervalParameter()
    use_hadoop = luigi.BooleanParameter()

    host = "localhost"
    database = "toplists"
    user = "luigi"
    password = "abc123"  # ;)
    table = "top10"

    columns = [("date_from", "DATE"),
               ("date_to", "DATE"),
               ("artist", "TEXT"),
               ("streams", "INT")]

    def requires(self):
        return Top10Artists(self.date_interval, self.use_hadoop)
```

Just like previously, this defines a recursive dependency on the previous task. If you try to build the task, that will also trigger building all its upstream dependencies.

### Using the central planner

The *--local-scheduler* flag tells Luigi not to connect to a central scheduler. This is recommended in order to get started and or for development purposes. At the point where you start putting things in production we strongly recommend running the central scheduler server. In addition to providing locking so the same task is not run by multiple processes at the same time, this server also provides a pretty nice visualization of your current work flow.

If you drop the *--local-scheduler* flag, your script will try to connect to the central planner, by default at localhost port 8082. If you run

    PYTHONPATH=. python bin/luigid

in the background and then run

    $ python wordcount.py --date 2012-W03

then in fact your script will now do the scheduling through a centralized server. You need [Tornado](http://www.tornadoweb.org/) for this to work.

Launching *http://localhost:8082* should show something like this:

![Web server screenshot](https://raw.github.com/erikbern/luigi/new-doc/doc/web_server.png)

Looking at the dependency graph for any of the tasks yields something like this:

![Aggregate artists screenshot](https://raw.github.com/erikbern/luigi/new-doc/doc/aggregate_artists.png)

In case your job crashes remotely due to any Python exception, Luigi will try to fetch the traceback and print it on standard output. You need [Mechanize](http://wwwsearch.sourceforge.net/mechanize/) for it to work and you also need connectivity to your tasktrackers.

## Conceptual overview

There are two fundamental building blocks of Luigi - the *Task* class and the *Target* class. Both are abstract classes and expect a few methods to be implemented. In addition to those two concepts, the *Parameter* class is an important concept that governs how a Task is run.

### Target

Broadly speaking, the Target class corresponds to a file on a disk. Or a file on HDFS. Or some kind of a checkpoint, like an entry in a database. Actually, the only method that Targets have to implement is the *exists* method which returns True if and only if the Target exists.

In practice, implementing Target subclasses is rarely needed. You can probably get pretty far with the *LocalTarget* and *hdfs.HdfsTarget* classes that are available out of the box. These directly map to a file on the local drive, or a file in HDFS, respectively. In addition these also wrap the underlying operations to make them atomic. They both implement the *open(flag)* method which returns a stream object that could be read (flag = 'r') from or written to (flag = 'w'). Both LocalTarget and hdfs.HdfsTarget also optionally take a format parameter. Luigi comes with Gzip support by providing *format=format.Gzip* . Adding support for other formats is pretty simple.

### Task

The *Task* class is a bit more conceptually interesting because this is where computation is done. There are a few methods that can be implemented to alter its behavior, most notably *run*, *output* and *requires*.

The Task class corresponds to some type of job that is run, but in general you want to allow some form of parametrization of it. For instance, if your Task class runs a Hadoop job to create a report every night, you probably want to make the date a parameter of the class.

#### Parameter

In Python this is generally done by adding arguments to the constructor, but Luigi requires you to declare these parameters instantiating Parameter objects on the class scope:

```python
class DailyReport(luigi.hadoop.JobTask):
    date = luigi.DateParameter(default=datetime.date.today())
    # ...
```

By doing this, Luigi can do take care of all the boilerplate code that would normally be needed in the constructor. Internally, the DailyReport object can now be constructed by running *DailyReport(datetime.date(2012, 5, 10))* or just *DailyReport()*. Luigi also creates a command line parser that automatically handles the conversion from strings to Python types. This way you can invoke the job on the command line eg. by passing *--date 2012-15-10*.

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

In this case, the DailyReport task depends on two inputs created earlier, one of which is the same class. requires can return other Tasks in any way wrapped up within dicts/lists/tuples/etc.

#### Task.output

The *output* method returns one or more Target objects. Similarly to requires, can return wrap them up in any way that's convenient for you. However we strongly recommend that any Task only return one single Target in output.

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

#### Using the central scheduler

The central scheduler does not execute anything for you, or help you with job parallelization. The two purposes it serves are to

* Make sure two instances of the same task are not running simultaneously
* Provide visualization of everything that's going on.

For running tasks periodically, the easiest thing to do is to trigger a Python script from cron or from a continuously running process. There is no central process that automatically triggers job. This model may seem limited, but we believe that it makes things far more intuitive and easy to understand.

## Luigi patterns

### Code reuse

One nice thing about Luigi is that it's super easy to depend on tasks defined in other repos. It's also trivial to have "forks" in the execution path, where the output of one task may become the input of many other tasks.

Currently no semantics for "intermediate" output is supported, meaning that all output will be persisted indefinitely. The upside of that is that if you try to run X -> Y, and Y crashes, you can resume with the previously built X. The downside is that you will have a lot of intermediate results on your file system. A useful pattern is to put these files in a special directory and have some kind of periodical garbage collection clean it up.

### Triggering many tasks

A common use case is to make sure some daily Hadoop job (or something else) is run every night. Sometimes for various reasons things will crash for more than a day though. A useful pattern is to have a dummy Task at the end just declaring dependencies on the past few days of tasks you want to run.

```python
class AllReports(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today())
    lookback = luigi.IntParameter(default=14)
    def requires(self):
        for i in xrange(self.lookback):
           date = self.date - datetime.timedelta(i + 1)
           yield SomeReport(date), SomeOtherReport(date), CropReport(date), TPSReport(date), FooBarBazReport(date)    
```

This simple task will not do anything itself, but will invoke a bunch of other tasks.

## Configuration

All configuration can be done by adding a configuration file named client.cfg to your current working directory or /etc/luigi (although this is further configurable).

* *default-scheduler-host* defaults the scheduler to some hostname so that you don't have to provide it as an argument
* *error-email* makes sure every time things crash, you will get an email (unless it was run on the command line)
* *luigi-history*, if set, specifies a filename for Luigi to write stuff (currently just job id) to in mapreduce job's output directory. Useful in a configuration where no history is stored in the output directory by Hadoop.
* If you want to run Hadoop mapreduce jobs in Python, you should also a path to your streaming jar
* By default, Luigi is configured to work with the CDH4 release of Hadoop.  There are some minor differences with regards to the HDFS CLI in CDH3, CDH4 and the Apache releases of Hadoop.  If you want to use a release other than CDH4, you need to specify which version you are using.

### Example /etc/luigi/client.cfg

```
[hadoop]
version: cdh4
jar: /usr/lib/hadoop-xyz/hadoop-streaming-xyz-123.jar

[core]
default-scheduler-host: luigi-host.mycompany.foo
error-email: foo@bar.baz
```

All sections are optional based on what parts of Luigi you are actually using.  By default, Luigi will not send error emails when running through a tty terminal.  If using the Apache release of Hive, there are slight differences when compared to the CDH release, so specify this configuration setting accordingly.

## More info

Luigi is the sucessor to a couple of attempts that we weren't fully happy with. We learned a lot from our mistakes and some design decisions include:

* Straightforward command line integration.
* As little boilerplate as possible.
* Focus on job scheduling and dependency resolution, not a particular platform. In particular this means no limitation to Hadoop. Though Hadoop/HDFS support is built-in and is easy to use, this is just one of many types of things you can run.
* A file system abstraction where code doesn't have to care about where files are located.
* Atomic file system operations through this abstraction. If a task crashes it won't lead to a broken state.
* The depencies are decentralized. No big config file in XML. Each task just specifies which inputs it needs and cross-module dependencies are trivial.
* A web server that renders the dependency graph and does locking etc for free.
* Trivial to extend with new file systems, file formats and job types. You can easily write jobs that inserts a Tokyo Cabinet into Cassandra. Adding broad support S3, MySQL or Hive should be a stroll in the park. (Feel free to send us a patch when you're done!)
* Date algebra included.
* Lots of unit tests of the most basic stuff

It wouldn't be fair not to mention some limitations with the current design:

* Its focus is on batch processing so it's probably less useful for near real-time pipelines or continuously running processes.
* The assumption is that a each task is a sizable chunk of work. While you can probably schedule a few thousand jobs, it's not meant to scale beyond tens of thousands.
* Luigi maintains a strict separation between scheduling tasks and running them. Dynamic for-loops and branches are non-trivial to implement. For instance, it's tricky to iterate a numerical computation task until it converges.

It should actually be noted that all these limitations are not fundamental in any way. However, it would take some major refactoring work.

Also it should be mentioned that Luigi is named after the world's second most famous plumber.

## Future ideas

* S3/EC2 - We have some old ugly code based on Boto that could be integrated in a day or two.
* Built in support for Pig/Hive.
* Better visualization tool - the layout gets pretty messy as the number of tasks grows.
* Integration with existing Hadoop frameworks like mrjob would be cool and probably pretty easy.
* Better support (without much boilerplate) for unittesting specific Tasks

## Getting help

* Find us on #luigi on freenode.
* Subscribe to the [luigi-user](http://groups.google.com/group/luigi-user/) group and ask a question.

## Want to contribute?

Awesome! Let us know if you have any ideas. Feel free to contact x@y.com where x = luigi and y = spotify.

