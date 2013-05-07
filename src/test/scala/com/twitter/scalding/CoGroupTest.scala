package com.twitter.scalding

import cascading.pipe.joiner._
import org.specs._

class StarJoinJob(args : Args) extends Job(args) {
  val in0 = Tsv("input0").read.mapTo((0,1) -> ('x0, 'a)) { input : (Int, Int) => input }
  val in1 = Tsv("input1").read.mapTo((0,1) -> ('x1, 'b)) { input : (Int, Int) => input }
  val in2 = Tsv("input2").read.mapTo((0,1) -> ('x2, 'c)) { input : (Int, Int) => input }
  val in3 = Tsv("input3").read.mapTo((0,1) -> ('x3, 'd)) { input : (Int, Int) => input }

  in0.coGroupBy('x0) {
    _.coGroup('x1, in1, OuterJoinMode)
      .coGroup('x2, in2, OuterJoinMode)
      .coGroup('x3, in3, OuterJoinMode)
  }
  .project('x0, 'a, 'b, 'c, 'd)
  .write(Tsv("output"))
}

class CoGroupTest extends Specification with TupleConversions {
  noDetailedDiffs()
  "A StarJoinJob" should {
    JobTest("com.twitter.scalding.StarJoinJob")
      .source(Tsv("input0"), List((0, 1), (1, 1), (2, 1), (3, 2)))
      .source(Tsv("input1"), List((0, 1), (2, 5), (3, 2)))
      .source(Tsv("input2"), List((1, 1), (2, 8)))
      .source(Tsv("input3"), List((0, 9), (2, 11)))
      .sink[(Int, Int, Int, Int, Int)](Tsv("output")) { outputBuf =>
        "be able to work" in {
          val out = outputBuf.toSet
          val expected = Set((0,1,1,0,9), (1,1,0,1,0), (2,1,5,8,11), (3,2,2,0,0))
          out must_== expected
        }
      }
      .run
      .finish
  }
}
