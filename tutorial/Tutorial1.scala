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
Scalding tutorial part 1.

In part 0, we made a copy of hello.txt, but it wasn't a perfect copy:
it was annotated with line numbers.

That's because the data stream coming out of a TextLine source actually
has two fields: one, called "line", has the actual line of text. The other,
called "num", has the line number in the file. When you write these
tuples to a TextLine, it naively outputs them both on each line.

We can ask scalding to select just the "line" field from the pipe, using the
project() method. When we refer to a data stream's fields, we use Scala symbols,
like this: 'line.

To run this job:
  scripts/scald.rb --local tutorial/Tutorial1.scala

Check the output:
  cat tutorial/data/output1.txt

**/

class Tutorial1(args : Args) extends Job(args) {

  val input = TextLine("tutorial/data/hello.txt")
  val output = TextLine("tutorial/data/output1.txt")

  /**
  We generally write each step of the pipeline on a separate line.
  **/
  input
    .read
    .project('line)
    .write(output)
}
