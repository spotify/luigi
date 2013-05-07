package com.twitter.scalding

import org.specs._

class PageRankTest extends Specification with TupleConversions {
  "A PageRank job" should {
    JobTest("com.twitter.scalding.examples.PageRank").
      arg("input", "inputFile").
      arg("output", "outputFile").
      arg("errorOut", "error").
      arg("temp", "tempBuffer").
      //How many iterations to do each time:
      arg("iterations", "6").
      arg("convergence", "0.05").
      source(Tsv("inputFile"), List((1L,"2",1.0),(2L,"1,3",1.0),(3L,"2",1.0))).
      //Don't check the tempBuffer:
      sink[(Long,String,Double)](Tsv("tempBuffer")) { ob => () }.
      sink[Double](Tsv("error")) { ob =>
        "have low error" in {
          ob.head must be_<=(0.05)
        }
      }.
      sink[(Long,String,Double)](Tsv("outputFile")){ outputBuffer =>
        val pageRank = outputBuffer.map { res => (res._1,res._3) }.toMap
        "correctly compute pagerank" in {
          val d = 0.85
          val twoPR = ( 1.0 + 2*d ) / (1.0 + d)
          val otherPR = ( 1.0 + d / 2.0 ) / (1.0 + d)
          println(pageRank)
          (pageRank(1L) + pageRank(2L) + pageRank(3L)) must beCloseTo(3.0, 0.1)
          pageRank(1L) must beCloseTo(otherPR, 0.1)
          pageRank(2L) must beCloseTo(twoPR, 0.1)
          pageRank(3L) must beCloseTo(otherPR, 0.1)
        }
      }.
      run.
      finish
  }
}
