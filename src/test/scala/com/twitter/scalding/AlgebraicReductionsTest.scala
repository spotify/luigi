package com.twitter.scalding

import org.specs._
/**
  */
class AlgebraJob(args : Args) extends Job(args) {
  Tsv("input", ('x,'y,'z,'w))
    .map('w -> 'w) { w : Int => Set(w) }
    .groupBy('x) {
      _.plus[(Int,Int)](('y,'z) -> ('sy, 'sz))
       .plus[Set[Int]]('w -> 'setw)
       .times[(Int,Int)](('y, 'z) -> ('py, 'pz))
       .dot[Int]('y,'z,'ydotz)
    }
    .write(Tsv("output"))
}

class ComplicatedAlgebraJob(args : Args) extends Job(args) {
  Tsv("input", ('x,'y,'z,'w,'v))
    .map('w -> 'w) { w : Int => Set(w) }
    .groupBy('x) {
      _.plus[(Int,Int,Set[Int],Double)](('y,'z,'w,'v) -> ('sy,'sz,'sw,'sv))
    }
    .write(Tsv("output"))
}

class AlgebraJobTest extends Specification {
  noDetailedDiffs()
  import Dsl._
  val inputData = List((1,2,3,5),(1,4,5,7),(2,1,0,7))
  val correctOutput = List((1,6,8,Set(5,7), 8,15,(6 + 20)),(2,1,0,Set(7),1,0,0))
  "A AlgebraJob" should {
    JobTest("com.twitter.scalding.AlgebraJob")
      .source(Tsv("input",('x,'y,'z,'w)), inputData)
      .sink[(Int, Int, Int, Set[Int], Int, Int, Int)](Tsv("output")) { buf =>
        "correctly do algebra" in {
          buf.toList must be_==(correctOutput)
        }
      }
      .run
      .finish
  }

  val inputData2 = List((1,2,3,5,1.2),(1,4,5,7,0.1),(2,1,0,7,3.2))
  val correctOutput2 = List((1,6,8,Set(5,7),1.3),(2,1,0,Set(7),3.2))
  "A ComplicatedAlgebraJob" should {
    JobTest("com.twitter.scalding.ComplicatedAlgebraJob")
      .source(Tsv("input",('x,'y,'z,'w,'v)), inputData2)
      .sink[(Int, Int, Int, Set[Int], Double)](Tsv("output")) { buf =>
        "correctly do complex algebra" in {
          buf.toList must be_==(correctOutput2)
        }
      }
      .run
      .finish
  }
}
