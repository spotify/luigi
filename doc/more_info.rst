
More Info
---------

Luigi is the successor to a couple of attempts that we weren't fully happy with.
We learned a lot from our mistakes and some design decisions include:

-  Straightforward command line integration.
-  As little boilerplate as possible.
-  Focus on job scheduling and dependency resolution, not a particular platform.
   In particular this means no limitation to Hadoop.
   Though Hadoop/HDFS support is built-in and is easy to use,
   this is just one of many types of things you can run.
-  A file system abstraction where code doesn't have to care about where files are located.
-  Atomic file system operations through this abstraction.
   If a task crashes it won't lead to a broken state.
-  The dependencies are decentralized.
   No big config file in XML.
   Each task just specifies which inputs it needs and cross-module dependencies are trivial.
-  A web server that renders the dependency graph and does locking etc for free.
-  Trivial to extend with new file systems, file formats and job types.
   You can easily write jobs that inserts a Tokyo Cabinet into Cassandra.
   Adding broad support S3, MySQL or Hive should be a stroll in the park.
   (Feel free to send us a patch when you're done!)
-  Date algebra included.
-  Lots of unit tests of the most basic stuff

It wouldn't be fair not to mention some limitations with the current design:

-  Its focus is on batch processing so
   it's probably less useful for near real-time pipelines or continuously running processes.
-  The assumption is that a each task is a sizable chunk of work.
   While you can probably schedule a few thousand jobs,
   it's not meant to scale beyond tens of thousands.
-  Luigi maintains a strict separation between scheduling tasks and running them.
   Dynamic for-loops and branches are non-trivial to implement.
   For instance, it's tricky to iterate a numerical computation task until it converges.

It should actually be noted that all these limitations are not fundamental in any way.
However, it would take some major refactoring work.

Also it should be mentioned that Luigi is named after the world's second most famous plumber.
