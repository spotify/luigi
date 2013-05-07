# Scalding

Scalding is a Scala library that makes it easy to write MapReduce jobs in Hadoop. Instead of forcing you to write raw map and reduce functions, Scalding allows you to write code that looks like *natural* Scala. It's similar to other MapReduce platforms like Pig, but offers a more powerful level of abstraction due to its built-in integration with Scala and the JVM.

Scalding is built on top of [Cascading](http://www.cascading.org/), a Java library that abstracts away much of the complexity of Hadoop.

Current version: 0.8.1

## Word Count

Hadoop is a distributed system for counting words. Here is how it's done in Scalding.

```scala
package com.twitter.scalding.examples

import com.twitter.scalding._

class WordCountJob(args : Args) extends Job(args) {
  TextLine( args("input") )
    .flatMap('line -> 'word) { line : String => tokenize(line) }
    .groupBy('word) { _.size }
    .write( Tsv( args("output") ) )

  // Split a piece of text into individual words.
  def tokenize(text : String) : Array[String] = {
    // Lowercase each word and remove punctuation.
    text.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+")
  }
}
```

Notice that the `tokenize` function, which is standard Scala, integrates naturally with the rest of the MapReduce job. This is a very powerful feature of Scalding. (Compare it to the use of UDFs in Pig.)

You can find more example code under [examples/](https://github.com/twitter/scalding/tree/master/src/main/scala/com/twitter/scalding/examples). If you're interested in comparing Scalding to other languages, see the [Rosetta Code page](https://github.com/twitter/scalding/wiki/Rosetta-Code), which contains several MapReduce tasks translated from other frameworks (e.g., Pig and Hadoop Streaming) into Scalding.

## Getting Started

* Check out the [Getting Started](https://github.com/twitter/scalding/wiki/Getting-Started) page on the [wiki](https://github.com/twitter/scalding/wiki).
* Next, go through the [runnable tutorials](https://github.com/twitter/scalding/tree/master/tutorial) provided in the source.
* The [API Reference](https://github.com/twitter/scalding/wiki/API-Reference) contains general documentation, as well as many example Scalding snippets.
* The [Introduction to Matrix Library](https://github.com/twitter/scalding/wiki/Introduction-to-Matrix-Library) contains a short overview of the Matrix library and a small "Hello world!" example to get started 
* The [Matrix API Reference](https://github.com/twitter/scalding/wiki/Matrix-API-Reference) contains the Matrix Library API reference with examples
* The [Scalding Wiki](https://github.com/twitter/scalding/wiki) contains more useful information.

## Building
0. Install [sbt 0.11.3](http://scalasbt.artifactoryonline.com/scalasbt/sbt-native-packages/org/scala-sbt/sbt-launcher/0.11.3/) (sorry, but the assembly plugin is sbt version dependent).
1. ```sbt update``` (takes 2 minutes or more)
2. ```sbt test```
3. ```sbt assembly``` (needed to make the jar used by the scald.rb script)

We use [Travis CI](http://travis-ci.org/) to verify the build:
[![Build Status](https://secure.travis-ci.org/twitter/scalding.png)](http://travis-ci.org/twitter/scalding)

The current version is 0.8.1 and is available from maven central: org="com.twitter", artifact="scalding_2.9.2".

## Contact

Currently we are using the cascading-user mailing list for discussions:
<http://groups.google.com/group/cascading-user>

In the remote possibility that there exist bugs in this code, please report them to:
<https://github.com/twitter/scalding/issues>

Follow [@Scalding](http://twitter.com/scalding) on Twitter for updates.

## Authors:
* Avi Bryant <http://twitter.com/avibryant>
* Oscar Boykin <http://twitter.com/posco>
* Argyris Zymnis <http://twitter.com/argyris>

Thanks for assistance and contributions:

* Chris Wensel <http://twitter.com/cwensel>
* Ning Liang <http://twitter.com/ningliang>
* Dmitriy Ryaboy <http://twitter.com/squarecog>
* Dong Wang <http://twitter.com/dongwang218>
* Edwin Chen <http://twitter.com/edchedch>
* Sam Ritchie <http://twitter.com/sritchie09>
* Flavian Vasile <http://twitter.com/flavianv>

## License
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
