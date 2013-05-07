/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
import com.twitter.scalding._

/**
Scalding tutorial part 0.

This is the simplest possible scalding job: it reads from one data source and writes the data,
unchanged, to another.

To test it, from the science directory, first make sure you've built the target/scalding-assembly-0.2.0.jar:
from the base directory type:
  sbt assembly

Now use the scald.rb script in local mode to run this job:
  scripts/scald.rb --local tutorial/Tutorial0.scala

You can check the input:
  cat tutorial/data/hello.txt

And the output:
  cat tutorial/data/output0.txt

The output should look just like the input, but with line numbers.
More on this in part 1 of the tutorial.
**/


/**
All jobs in scalding are represented by a subclass of com.twitter.scalding.Job.
The constructor must take a single com.twitter.scalding.Args, even if, as here,
we don't use it.

For the scald.rb script to work, name the class to match the file,
and don't use a package.
**/
class Tutorial0(args : Args) extends Job(args) {

  /**
  Both input and output data sources are represented by instances of
  com.twitter.scalding.Source.

  Scalding comes with some basic source types like TextLine and Tsv.
  There are also many twitter-specific types like MergedAdRequestSource.
  **/
  val input = TextLine("tutorial/data/hello.txt")
  val output = TextLine("tutorial/data/output0.txt")

  /**
  This is the minimal pipeline. Source.read returns a cascading.pipe.Pipe, which represents
  a stream of data. We can transform this stream in many ways, but here we're simply
  asking it to write itself to the output source.
  **/
  input.read.write(output)

  /**
  By the way, if you look at the docs for Pipe, you won't find write there. That's
  because it's actually defined on com.twitter.scalding.RichPipe. Most of the methods
  we call on Pipes will actually be found on RichPipe; in typical scala style,
  the conversion between them is implicit.
  **/
}
