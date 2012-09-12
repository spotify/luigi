# Luigi

![Luigi](luigi.png)

## What is it?

Luigi is a Python module that helps you build complex pipelines of batch jobs. It handles dependency resolution, workflow management, visualization etc. It also comes with [Hadoop](http://hadoop.apache.org) support built in.

Conceptually, it's similar to [GNU Make](http://www.gnu.org/software/make/) where you have certain tasks and these tasks in turn may have dependencies on other tasks. There are also some similarities to [Oozie](http://incubator.apache.org/oozie/), and maybe also [Azkaban](http://data.linkedin.com/opensource/azkaban).

Luigi was conceived and implemented at [Spotify](http://www.spotify.com) mostly by Erik Bernhardsson and Elias Freider.

You probably should check out Luigi if you use Python and:

* There is a bunch of tasks you have to run.
* This tasks are generally batch processing stuff.
* These tasks form have dependencies on other tasks or input coming from somewhere else.
* You want things to be triggered at specific points and/or by data availablity.
* You use Hadoop (though by no means it's necessary)
* Your stuff takes a sizeable amount of time to run (anything from minutes to months).
* You want to automate a complex pipeline of tasks.
* Occasional failures are expected. Retry later.

We use Luigi internally at Spotify to run 1000s of tasks every day, organized in complex dependency graphs. Most of these tasks are Hadoop job. Luigi provides an infrastructure that powers all kinds of stuff including recommendations, toplists, A/B test analysis, external reports, internal dashboards, etc.

A code example says more than 1000 bullet lists, so enough said - let's look at some examples. Though by all means feel free to scroll down and [read more](#readmore) about some design decisions Luigi.

## Examples

Let's begin with the classic WordCount example. We'll show a non-Hadoop version then later show how it can be implemented as a Hadoop job.

### Example 1 - Simple wordcount

TODO: verify this works

Assume you have a bunch of text files dumped onto disk every night by some external process. These text files contain English text, and we want to monitor the top few thousand words over rolling 7-day periods. Hopefully this doesn't sound too contrived - you could imagine mapping "words" to "artists" to get an idea of a real application at Spotify.

    import luigi
    
    class InputText(luigi.ExternalTask):
       ''' This class represents something that was created elsewhere by an external process, so all we want to do is to implement the output method.
       '''
       date = luigi.DateParameter()
       def output(self):
           return luigi.LocalTarget(self.date.strftime('/var/text/%Y-%m-%d.txt'))

    @luigi.expose_main
    class WordToplist(luigi.Task):
        date = luigi.DateParameter()
        lookback = luigi.IntParameter(default=7)
        size = luigi.IntParameter(default=1000)

        def requires(self):
            return [InputText(self.date - i) for i in xrange(self.lookback)]
        def output(self):
            return luigi.LocalTarget('/var/text-output/%s-%d' % (self.data.strftime('%Y-%m-%d'), self.lookback))
        def run(self):
            count = {}
            for file in self.input(): # The input() method is a wrapper around requires() that returns Target objects
                for line in file.open('r'): # Target objects are a file system/format abstraction and this will return a file stream object
                    for word in line.strip().split():
                        count[word] = count.get(word, 0) + 1
            # output data
            f = self.output().open('w')
            for word in sorted(count.keys(), key=count.get, reverse=True)[:self.size]:
                f.write("%s\t%d\n" % (word, count[word]))
            f.close() # Note that this is essential because file system operations are atomic

    if __name__ == '__main__':
        main()

Now, provided you have a bunch of input files in /var/text/, try running this using eg

    $ python wordcount.py --local-scheduler --date 2012-08-01

You can also try to view the manual using --help which will give you an overview of the options:

TODO: show output

Running the command again will do nothing because the output file is already created. Note that unlike Makefile, the output will not be recreated when any of the input files is modified. You need to delete the output file manually.

### Using the central planner

The --local-scheduler flag tells Luigi not to connect to a central scheduler. This is recommended in order to get started and or for development purposes. At the point where you start putting things in production we strongly recommend running the central scheduler server. In addition to provide locking so that no two tasks can be run at the same time, this server also provides a pretty nice visualization of your current work flow.

If you drop the *--local-scheduler* flag, your script will try to connect to the central planner, by default at localhost port 8000 (TODO: verify). If you run

    ./bin/spluigid
    
in the background and then run

    $ python wordcount.py --date 2012-08-01

then in fact your script will now do the scheduling through a centralized server. Launching *http://localhost:8000* should show something like this:

TODO: show graphviz visualization

### Example 2 - Hadoop WordCount

Luigi also provides support for Hadoop jobs straight out of the box. The interface is similar to mrjob but each job class is now a Luigi Task that can also define their dependencies and output files.

EC2 is unfortunately not supported at this point. We have some old code for this (using Python [boto](http://github.com/boto/boto) and would love to help anyone interested in getting it running.

TODO: verify this works

    import luigi, luigi.hadoop
    import heapq
    
    class InputText(luigi.ExternalTask):
       date = luigi.DateParameter()
       def output(self): return luigi.HdfsTarget(self.date.strftime('/text/%Y-%m-%d.txt'))

    @luigi.expose
    class AggregateWords(luigi.hadoop.Task):
        date = luigi.DateParameter()
        lookback = luigi.IntParameter()

        def requires(self):
            return [InputText(self.date - i) for i in xrange(self.lookback)]
        def output(self):
            return luigi.LocalTarget('/var/text-output/%s-%d' % (self.data.strftime('%Y-%m-%d'), self.lookback))
        def mapper(self, line):
            for word in line.strip().split():
                yield word, 1
        def reducer(self, key, values):
            yield key, sum(values)

    @luigi.expose_main
    class WordToplist(luigi.Task):
        date = luigi.DateParameter()
        lookback = luigi.IntParameter(default=7)
        size = luigi.IntParameter(default=1000)

        def requires(self):
            return AggregateWords(date, lookback)
        def output(self):
            return luigi.LocalTarget('/var/text-output/%s-%d' % (self.data.strftime('%Y-%m-%d'), self.lookback))
        def run(self):
            counts = []
            for line in self.input().open('r'):
                word, count = line.strip().split()
                heapq.heappush(counts, (int(count), word))
                if len(counts) > self.size:
                    heapq.heappop(counts)

            counts.sort(reverse=True)
            f = self.output().open('w')
            for count, word in counts:
                f.write('%s\t%d\n' % (word, count))
            f.close()

    if __name__ == '__main__':
        main()

### Example 3 - Another Hadoop WordCount

TODO: rewrite into a job that calculate accumulated word stats, then another job that just takes two such outputs and subtracts delta

## Conceptual overview

There are two fundamental building blocks of Luigi - the *Task* class and the *Target* class. Both are abstract classes and expect a few methods to be implemented. In addition to those two concepts, the *Parameter* class is an important concept that governs how a Task is run.

Broadly speaking, the Target class corresponds to a file on a disk. Or a file on HDFS. Or some kind of a checkpoint, like an entry in a database. Actually, the only method that Targets have to implement is the *complete* method which returns True if and only if the Target exists.

In practice, implementing Target subclasses is rarely needed. You can probably get pretty far with the *LocalTarget* and *HdfsTarget* classes that are available out of the box. These directly map to a file on the local drive, or a file in HDFS, respectively. In addition these also wrap the underlying operations to make them atomic. They both implement the *open(flag)* method which returns a stream object that could be read (flag = 'r') from or written to (flag = 'w').

The *Task* class is a bit more conceptually interesting because this is where computation is done. There is a few methods that can be implemented to alter its behavior, most notably *run*, *output* and *requires*.

The Task class corresponds to some type of job that is run, but in general you want to allow some form of parametrization of it. For instance, if your Task class runs a Hadoop job to create a report every night, you probably want to make the date a parameter of the class.

Now, in Python this is generally done by adding arguments to the constructor. Luigi requires you to declare these parameters instantiating Parameter objects on the class scope:

    class DailyReport(luigi.hadoop.JobTask):
        date = luigi.DateParameter(default=datetime.date.today())
        # ...

By doing this, Luigi can do take care of all the boiler plate code that would normally be needed in the constructor. Internally, the DailyReport object can now be constructed by running *DailyReport(datetime.date(2012, 5, 10))*. Luigi also creates a command line parser that automatically handles the conversion from strings to Python types. This way you can invoke the job on the command line eg. by passing *--date 2012-15-10*. The parameters are all set to their values on the Task object instance, i.e.

    d = DailyReport(datetime.date(2012, 5, 10))
    print d.date

will return the same date that the object was constructed with.

The *requires* method is used to specify dependencies on other Task object, which might even be of the same class. For instance, an example implementation could be

    def requires(self):
        return OtherTask(self.date), DailyReport(self.date - datetime.timedelta(1))

In this case, the DailyReport task depends on two inputs created earlier, one of which is the same class.

The *run* method now contains the actual code that is run. Note that Luigi breaks down everything into two stages. First it figures out all dependencies between tasks, then it runs everything.

In addition to the stuff mentioned above, Luigi also does some metaclass logic so that if eg. *DailyReport(datetime.date(2012, 5, 10))* is instantiated twice in the code, it will in fact result in the same object. This is needed so that each Task is run only once.

## More info

Some design decisions include:

* Straightforward command line integration.
* As little boiler plate as possible.
* Focus on job scheduling and dependency resolution, not a particular platform. In particular this means no limitation to Hadoop. Though Hadoop/HDFS support is built-in and is easy to use, this is just one of many types of things you can run.
* A file system abstraction where code doesn't have to care about where files are located.
* Atomic file system operations through this abstraction. If a task crashes it won't lead to a broken state.
* The depencies are decentralized. No big config file in XML. Each task just specifies which inputs it needs and cross-module dependencies are trivial.
* A web server that renders the dependency graph and does locking etc for free.
* Trivial to extend with new file systems, file formats and job types. You can easily write jobs that inserts a Tokyo Cabinet into Cassandra. Adding broad support S3, MySQL or Hive should be a stroll in the park. (and feel free to send us a patch when you're done!)
* Date algebra included.

It wouldn't be fair not to mention some limitations with the current design:

* Its focus is on batch processing so it's probably less useful for near real-time pipelines or continuously running processes.
* The assumption is that a each task is a sizable chunk of work. While you can probably schedule a few thousand jobs, it's not meant to scale beyond tens of thousands.
* Luigi maintains a strict separation between scheduling tasks and running them. Dynamic for-loops and branches are non-trivial to implement. For instance, it's tricky to iterate a numerical computation task until it converges.

It should actually be noted that all these limitations are not fundamental in any way. However, it would take some major refactoring work.

Also it should be mentioned that Luigi is named after the pipeline-running friend of Super Mario.

## Future ideas

* S3/EC2 - We have some old ugly code based on Boto that could be integrated in a day or two.
* The @luigi.expose decorator is probably a bit superfluous and should be the default mode.
* Better visualization tool - the layout gets pretty messy as the number of tasks grows.
* Integration with existing Hadoop frameworks like mrjob would be cool and probably pretty easy.
