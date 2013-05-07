package com.twitter.scalding

import org.specs._

class WordCountTest extends Specification with TupleConversions {
  "A WordCount job" should {
    JobTest("com.twitter.scalding.examples.WordCountJob").
      arg("input", "inputFile").
      arg("output", "outputFile").
      source(TextLine("inputFile"), List("0" -> "hack hack hack and hack")).
      sink[(String,Int)](Tsv("outputFile")){ outputBuffer =>
        val outMap = outputBuffer.toMap
        "count words correctly" in {
          outMap("hack") must be_==(4)
          outMap("and") must be_==(1)
        }
      }.
      run.
      finish
  }
}
