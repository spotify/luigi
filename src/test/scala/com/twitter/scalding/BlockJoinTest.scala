package com.twitter.scalding

import org.specs._

import cascading.pipe.joiner._

import java.lang.reflect.InvocationTargetException

import scala.collection.mutable.Buffer

class InnerProductJob(args : Args) extends Job(args) {
  val l = args.getOrElse("left", "1").toInt
  val r = args.getOrElse("right", "1").toInt
  val j = args.getOrElse("joiner", "i") match {
    case "i" => new InnerJoin
    case "l" => new LeftJoin
    case "r" => new RightJoin
    case "o" => new OuterJoin
  }

  val in0 = Tsv("input0").read.mapTo((0,1,2) -> ('x1, 'y1, 's1)) { input : (Int, Int, Int) => input }
  val in1 = Tsv("input1").read.mapTo((0,1,2) -> ('x2, 'y2, 's2)) { input : (Int, Int, Int) => input }
  in0
    .blockJoinWithSmaller('y1 -> 'y2, in1, leftReplication = l, rightReplication = r, joiner = j)
    .map(('s1, 's2) -> 'score) { v : (Int, Int) =>
      v._1 * v._2
    }
    .groupBy('x1, 'x2) { _.sum('score) }
    .write(Tsv("output"))
}

class BlockJoinPipeTest extends Specification with TupleConversions {
  noDetailedDiffs()

  "An InnerProductJob" should {

    val in1 = List(("0", "0", "1"), ("0", "1", "1"), ("1", "0", "2"), ("2", "0", "4"))
    val in2 = List(("0", "1", "1"), ("1", "0", "2"), ("2", "4", "5"))
    val correctOutput = Set((0, 1, 2.0), (0, 0, 1.0), (1, 1, 4.0), (2, 1, 8.0))

    def runJobWithArguments(left : Int = 1, right : Int = 1, joiner : String = "i")
        (callback : Buffer[(Int,Int,Double)] => Unit ) {
      JobTest("com.twitter.scalding.InnerProductJob")
        .source(Tsv("input0"), in1)
        .source(Tsv("input1"), in2)
        .arg("left", left.toString)
        .arg("right", right.toString)
        .arg("joiner", joiner)
        .sink[(Int,Int,Double)](Tsv("output")) { outBuf =>
          callback(outBuf)
        }
        .run
        .finish
    }

    "correctly compute product with 1 left block and 1 right block" in {
      runJobWithArguments() { outBuf =>
        val unordered = outBuf.toSet
        unordered must_== correctOutput
      }
    }

    "correctly compute product with multiple left and right blocks" in {
      runJobWithArguments(left = 3, right = 7) { outBuf =>
        val unordered = outBuf.toSet
        unordered must_== correctOutput
      }
    }

    "correctly compute product with a valid LeftJoin" in {
      runJobWithArguments(right = 7, joiner = "l") { outBuf =>
        val unordered = outBuf.toSet
        unordered must_== correctOutput
      }
    }

    "throw an exception when used with OuterJoin" in {
      runJobWithArguments(joiner = "o") { g => g } must throwA[InvocationTargetException]
    }

    "throw an exception when used with an invalid LeftJoin" in {
      runJobWithArguments(joiner = "l", left = 2) { g => g } must throwA[InvocationTargetException]
    }

    "throw an exception when used with an invalid RightJoin" in {
      runJobWithArguments(joiner = "r", right = 2) { g => g } must throwA[InvocationTargetException]
    }
  }
}
