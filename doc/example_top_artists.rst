Example Workflow – Top Artists
------------------------------

This is a very simplified case of something we do at Spotify a lot. All
user actions are logged to HDFS where we run a bunch of Hadoop jobs to
transform the data. At some point we might end up with a smaller data
set that we can bulk ingest into Cassandra, Postgres, or some other
format.

For the purpose of this exercise, we want to aggregate all streams, and
find the top 10 artists. We will then put it into Postgres.

This example is also available in ``examples/top_artists.py``

Step 1 - Aggregate Artist Streams
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

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

Note that this is just a portion of the file *examples/top\_artists.py*.
In particular, ``Streams`` is defined as a ``luigi.Task``, acting as a
dependency for ``AggregateArtists``. In addition, ``luigi.run()`` is
called if the script is executed directly, allowing it to be run from
the command line.

There are several pieces of this snippet that deserve more explanation.

-  Any *Task* may be customized by instantiating one or more *Parameter*
   objects on the class level.
-  The *output* method tells Luigi where the result of running the task
   will end up. The path can be some function of the parameters.
-  The *requires* tasks specifies other tasks that we need to perform
   this task. In this case it's an external dump named *Streams* which
   takes the date as the argument.
-  For plain Tasks, the *run* method implements the task. This could be
   anything, including calling subprocesses, performing long running
   number crunching, etc. For some subclasses of *Task* you don't have
   to implement the *run* method. For instance, for the *HadoopJobTask*
   subclass you implement a *mapper* and *reducer* instead.
-  *HdfsTarget* is a built in class that makes it easy to read/write
   from/to HDFS. It also makes all file operations atomic, which is nice
   in case your script crashes for any reason.

Running this Locally
~~~~~~~~~~~~~~~~~~~~

Try running this using eg.

::

    $ python examples/top_artists.py AggregateArtists --local-scheduler --date-interval 2012-06

You can also try to view the manual using --help which will give you an
overview of the options:

::

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

Running the command again will do nothing because the output file is
already created. In that sense, any task in Luigi is *idempotent*
because running it many times gives the same outcome as running it once.
Note that unlike Makefile, the output will not be recreated when any of
the input files is modified. You need to delete the output file
manually.

The *--local-scheduler* flag tells Luigi not to connect to a scheduler
server. This is not recommended for other purpose than just testing
things.

Step 1b - Running this in Hadoop
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Luigi comes with native Python Hadoop mapreduce support built in, and
here is how this could look like, instead of the class above.

.. code:: python

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

Note that ``luigi.hadoop.JobTask`` doesn't require you to implement a
``run`` method. Instead, you typically implement a ``mapper`` and
``reducer`` method.

Step 2 – Find the Top Artists
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

At this point, we've counted the number of streams for each artists, for
the full time period. We are left with a large file that contains
mappings of artist -> count data, and we want to find the top 10
artists. Since we only have a few hundred thousand artists, and
calculating artists is nontrivial to parallelize, we choose to do this
not as a Hadoop job, but just as a plain old for-loop in Python.

.. code:: python

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

The most interesting thing here is that this task (*Top10Artists*)
defines a dependency on the previous task (*AggregateArtists*). This
means that if the output of *AggregateArtists* does not exist, the task
will run before *Top10Artists*.

::

    $ python examples/top_artists.py Top10Artists --local-scheduler --date-interval 2012-07

This will run both tasks.

Step 3 - Insert into Postgres
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This mainly serves as an example of a specific subclass *Task* that
doesn't require any code to be written. It's also an example of how you
can define task templates that you can reuse for a lot of different
tasks.

.. code:: python

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

Just like previously, this defines a recursive dependency on the
previous task. If you try to build the task, that will also trigger
building all its upstream dependencies.

Using the Central Planner
~~~~~~~~~~~~~~~~~~~~~~~~~

The *--local-scheduler* flag tells Luigi not to connect to a central
scheduler. This is recommended in order to get started and or for
development purposes. At the point where you start putting things in
production we strongly recommend running the central scheduler server.
In addition to providing locking so the same task is not run by multiple
processes at the same time, this server also provides a pretty nice
visualization of your current work flow.

If you drop the *--local-scheduler* flag, your script will try to
connect to the central planner, by default at localhost port 8082. If
you run

::

    PYTHONPATH=. python bin/luigid

in the background and then run

::

    $ python wordcount.py --date 2012-W03

then in fact your script will now do the scheduling through a
centralized server. You need `Tornado <http://www.tornadoweb.org/>`__
for this to work.

Launching *http://localhost:8082* should show something like this:

.. figure:: web_server.png
   :alt: Web server screenshot

   Web server screenshot
Looking at the dependency graph for any of the tasks yields something
like this:

.. figure:: aggregate_artists.png
   :alt: Aggregate artists screenshot

   Aggregate artists screenshot
In case your job crashes remotely due to any Python exception, Luigi
will try to fetch the traceback and print it on standard output. You
need `Mechanize <http://wwwsearch.sourceforge.net/mechanize/>`__ for it
to work and you also need connectivity to your tasktrackers.

To run the server as a daemon run:

::

    PYTHONPATH=. python bin/luigid --background --pidfile <PATH_TO_PIDFILE> --logdir <PATH_TO_LOGDIR> --state-path <PATH_TO_STATEFILE>

Note that this requires python-daemon for this to work.
