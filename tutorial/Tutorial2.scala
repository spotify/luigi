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
Scalding tutorial part 2.

In parts 0 and 1, we made copies of hello.txt. Now let's try to
modify the copies by reversing each line.

To run this job:
  scripts/scald.rb --local tutorial/Tutorial2.scala

Check the output:
  cat tutorial/data/output2.txt

**/

class Tutorial2(args : Args) extends Job(args) {

  val input = TextLine("tutorial/data/hello.txt")
  val output = TextLine("tutorial/data/output2.txt")

  input
    .read

    /**
    As with a scala collection, you can map over a pipe, where each
    item gets passed into an anonymous function, and we create a new
    pipe with the results.

    In scalding, we need to also annotate the call to map with the names of the
    fields it operates on. In this case, we want to take the 'line field
    as input, and we want to output a new field named 'reversed.

    Unlike with a normal scala map{}, we always need to specify the
    types of the arguments to the anonymous function.
    **/

    .map('line -> 'reversed){ line : String => line.reverse}

    /**
    The map transformation in scalding is additive: the 'offset and 'line
    fields haven't gone away, we've just added a new 'reversed field to each
    entry. If we only want to write the 'reversed version, we need to use
    project.
    **/

    .project('reversed)
    .write(output)
}
