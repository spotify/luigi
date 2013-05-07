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
Scalding tutorial part 3.

So far, we've been hardcoding the input file. Let's make that an argument,
which changes how we run the job:

  scripts/scald.rb \
    --local tutorial/Tutorial3.scala \
    --input tutorial/data/hello.txt

We're also going to use a new transformation: flatMap.

Check the output:
  cat tutorial/data/output3.txt

You can also of course try this with other input parameters. For example:

  scripts/scald.rb \
    --local tutorial/Tutorial3.scala \
    --input tutorial/data/output2.txt

**/

class Tutorial3(args : Args) extends Job(args) {

  /**
  We can ask args for the --input argument from the command line.
  If it's missing, we'll get an error.
  **/
  val input = TextLine(args("input"))
  val output = TextLine("tutorial/data/output3.txt")

  input
    .read

    /**
    flatMap is like map, but instead of returning a single item from the
    function, we return a collection of items. Each of these items will create
    a new entry in the data stream; here, we'll end up with a new entry for each word.
    **/

    .flatMap('line -> 'word){ line : String => line.split("\\s")}

    /**
    We still want to project just the 'word field for our final output.
    For interest, though, let's stash a copy of the data before we do that.
    write() returns the pipe, so we can keep chaining our pipeline.
    **/

    .write(Tsv("tutorial/data/tmp3.tsv"))
    .project('word)
    .write(output)
}
