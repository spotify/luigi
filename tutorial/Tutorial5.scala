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
Scalding tutorial part 5.

This example is a little bit contrived so that we can play with joins.

Let's define a metric for a line of text which is the sum of the rank
of each of its words in the /usr/share/dict/words file -
on my laptop, the word "aa" comes third (rank 2) whereas the
last word is Zyzzogeton, with a rank of 234935.

So, the line "Zyzzogeton aa Zyzzogeton" would have a total score
of 234935+2+234935 = 469872

We'll read in an input file, split it into words, join those words
with /usr/share/dict/words to get their individual ranks, then
group by line to get a total score and output each line/score pair.

Run:
  scripts/scald.rb \
    --local tutorial/Tutorial5.scala \
    --input tutorial/data/hello.txt \
    --output tutorial/data/output5.txt

Check the output:
  cat tutorial/data/output5.txt

Note that the line order may no longer be the same as the input file.
That's parallelism, man.

**/

class Tutorial5(args : Args) extends Job(args) {

  /**
  We'll start with the dict data source.

  When we join, we'll need unique field names, so we'll rename
  the 'num field to be 'score. Also, we want to normalize
  the words to be lowercase.
  **/

  val scores = TextLine("/usr/share/dict/words")
                .read
                .rename('num, 'score)
                .map('line -> 'dictWord){line : String => line.toLowerCase}
                .project('score, 'dictWord)

  TextLine(args("input"))
    .read

    //split and normalize to lowercase
    .flatMap('line -> 'word){ line : String => line.split("\\s").map{_.toLowerCase}}

    /**
    When we join, we need to specify which fields from each side of the join should match.
    This is like a SQL inner join: we end up with a new row that combines each possible
    matching pair, with all of the fields of both the left and right side.
    **/

    .joinWithLarger('word -> 'dictWord, scores)

    /**
    Now that we have a score for each word, we can group back to the original lines
    and sum up the word scores. Sum is another common aggregation that GroupBuilder
    provides; we just need to specify which field to sum by.
    **/

    .groupBy('line){group => group.sum('score)}
    .write(Tsv(args("output")))
}
