# Luigi

## What is it?

Luigi is a Python module that helps you build complex pipelines of batch jobs. It's handles dependency resolution, workflow management, visualization etc. It also comes with *Hadoop* support built in.

Conceptually, it's similar to *GNU*Make* where you have certain tasks and these tasks in turn may have dependencies on other tasks. There are also some similarities to *Oozie*, although there are several crucial differences.

Luigi was conceived and implemented at *Spotify* mostly by Erik Bernhardsson and Elias Freider.

You probably should check out Luigi if:

* You use Hadoop (though by no means it's necessary)
* There is a bunch of tasks you have to run.
* This tasks are generally batch processing stuff.
* These tasks have dependencies on other tasks or input coming from somewhere else.
* You want things to be triggered at specific points and/or by data availablity.
* Your stuff takes a sizeable amount of time to run (anything from minutes to months).
* Tasks are run from cron.
* You are tired of running 50 manual steps for some complex process.
* Occasional failures are expected and aren't a big deal. Retry later.

[Read more](#readmore) about Luigi.

## Examples

Let's begin with the classic WordCount example. We'll show a non-Hadoop version then later extend it to Hadoop.

### Example 1 - Simple wordcount

TODO: verify this works

Assume you have a bunch of text files dumped onto disk every night by some external process. These text files contain English text, and we want to monitor the top few thousand words over rolling 7-day periods.

    import luigi
    
    class InputText(luigi.ExternalTask):
       date = luigi.DateParameter()
       def output(self): return luigi.LocalTarget(self.date.strftime('/var/text/%Y-%m-%d.txt'))

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

You can also try to view the manual using --help which will give you an overview of the options (in this case also --lookback and --size).

Running the command again will do nothing because the output file is already created. Note that unlike Makefile, the output will not be recreated when any of the input files is modified. You need to delete the output file manually.

The --local-scheduler flag tells Luigi not to connect to a central scheduler. This is recommended in order to get started and or for development purposes. At the point where you start putting things in production we strongly recommend running the central scheduler server.

TODO: show graphviz visualization

### Example 2 - Hadoop WordCount

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

## More info

Some design decisions include:

* Straightforward command line integration.
* As little boiler plate as possible.
* Focus on job scheduling and dependency resolution, not a particular platform. In particular this means no limitation to Hadoop. Though Hadoop/HDFS support is built-in and is easy to use, this is just one of many types of things you can run.
* A file system abstraction where code doesn't have to care about where files are located.
* Atomic file system operations through this abstraction. If a task crashes it won't lead to a broken state.
* The depencies are decentralized. No big config file. Each task just specifies which inputs it needs and cross-module dependencies are trivial.
* A web server that renders the dependency graph and does locking etc for free.
* Trivial to extend with new file systems, file formats and job types. You can easily write jobs that inserts a Tokyo Cabinet into Cassandra. Adding broad support S3, MySQL or Hive should be a stroll in the park. (and feel free to send \
a patch when you're done!)
* Date algebra included.

Some limitations are:

* Its focus is on batch processing so it's probably less useful for near real-time pipelines or continuously running processes.
* The assumption is that a each task is a sizable chunk of work. While you can probably schedule a few thousand jobs, it's not meant to scale beyond tens of thousands.
* Luigi maintains a strict separation between scheduling tasks and running them. Dynamic for-loops and branches are non-trivial to implement. For instance, it's tricky to iterate a numerical computation task until it converges.

## Future ideas

* S3/EC2 - We have some old ugly code that could be integrated in a day or two.
