package com.twitter.scalding

import org.specs._

import TDsl._

object TUtil {
  def printStack( fn: => Unit ) {
    try { fn } catch { case e : Throwable => e.printStackTrace; throw e }
  }
}

class TypedPipeJob(args : Args) extends Job(args) {
  //Word count using TypedPipe
  TextLine("inputFile")
    .flatMap { _.split("\\s+") }
    .map { w => (w, 1L) }
    .forceToDisk
    .group
    .sum
    .write(Tsv("outputFile"))
}

class TypedPipeTest extends Specification {
  import Dsl._
  noDetailedDiffs() //Fixes an issue with scala 2.9
  "A TypedPipe" should {
    TUtil.printStack {
    JobTest("com.twitter.scalding.TypedPipeJob").
      source(TextLine("inputFile"), List("0" -> "hack hack hack and hack")).
      sink[(String,Int)](Tsv("outputFile")){ outputBuffer =>
        val outMap = outputBuffer.toMap
        "count words correctly" in {
          outMap("hack") must be_==(4)
          outMap("and") must be_==(1)
        }
      }.
      run.
      runHadoop.
      finish
    }
  }
}

class TypedPipeJoinJob(args : Args) extends Job(args) {
  (Tsv("inputFile0").read.toTypedPipe[(Int,Int)](0, 1)
    leftJoin TypedPipe.from[(Int,Int)](Tsv("inputFile1").read, (0, 1)))
    .toPipe('key, 'value)
    .write(Tsv("outputFile"))
}

class TypedPipeJoinTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  import Dsl._
  "A TypedPipeJoin" should {
    JobTest("com.twitter.scalding.TypedPipeJoinJob")
      .source(Tsv("inputFile0"), List((0,0), (1,1), (2,2), (3,3), (4,5)))
      .source(Tsv("inputFile1"), List((0,1), (1,2), (2,3), (3,4)))
      .sink[(Int,(Int,Option[Int]))](Tsv("outputFile")){ outputBuffer =>
        val outMap = outputBuffer.toMap
        "correctly join" in {
          outMap(0) must be_==((0,Some(1)))
          outMap(1) must be_==((1,Some(2)))
          outMap(2) must be_==((2,Some(3)))
          outMap(3) must be_==((3,Some(4)))
          outMap(4) must be_==((5,None))
          outMap.size must be_==(5)
        }
      }.
      run.
      finish
  }
}

class TypedPipeHashJoinJob(args : Args) extends Job(args) {
  (Tsv("inputFile0").read.toTypedPipe[(Int,Int)](0, 1)
    hashLeftJoin TypedPipe.from[(Int,Int)](Tsv("inputFile1").read, (0, 1)))
    .toPipe('key, 'value)
    .write(Tsv("outputFile"))
}

class TypedPipeHashJoinTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  import Dsl._
  "A TypedPipeJoin" should {
    JobTest("com.twitter.scalding.TypedPipeJoinJob")
      .source(Tsv("inputFile0"), List((0,0), (1,1), (2,2), (3,3), (4,5)))
      .source(Tsv("inputFile1"), List((0,1), (1,2), (2,3), (3,4)))
      .sink[(Int,(Int,Option[Int]))](Tsv("outputFile")){ outputBuffer =>
        val outMap = outputBuffer.toMap
        "correctly join" in {
          outMap(0) must be_==((0,Some(1)))
          outMap(1) must be_==((1,Some(2)))
          outMap(2) must be_==((2,Some(3)))
          outMap(3) must be_==((3,Some(4)))
          outMap(4) must be_==((5,None))
          outMap.size must be_==(5)
        }
      }.
      run.
      finish
  }
}

class TypedImplicitJob(args : Args) extends Job(args) {
  def revTup[K,V](in : (K,V)) : (V,K) = (in._2, in._1)
  TextLine("inputFile").read.typed(1 -> ('maxWord, 'maxCnt)) { tpipe : TypedPipe[String] =>
    tpipe.flatMap { _.split("\\s+") }
      .map { w => (w, 1L) }
      // groupby the key and sum the values:
      .sum
      .groupAll
      .mapValues { revTup _ }
      .max
      // Throw out the Unit key and reverse the value tuple
      .map { _._2 }
      .swap
  }.write(Tsv("outputFile"))
}

class TypedPipeTypedTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  import Dsl._
  "A TypedImplicitJob" should {
    JobTest("com.twitter.scalding.TypedImplicitJob")
      .source(TextLine("inputFile"), List("0" -> "hack hack hack and hack"))
      .sink[(String,Int)](Tsv("outputFile")){ outputBuffer =>
        val outMap = outputBuffer.toMap
        "find max word" in {
          outMap("hack") must be_==(4)
          outMap.size must be_==(1)
        }
      }
      .run
      .finish
  }
}

class TJoinCountJob(args : Args) extends Job(args) {
  (TypedPipe.from[(Int,Int)](Tsv("in0",(0,1)), (0,1))
    join TypedPipe.from[(Int,Int)](Tsv("in1", (0,1)), (0,1)))
    .size
    .toPipe('key, 'count)
    .write(Tsv("out"))

  //Also check simple joins:
  (TypedPipe.from[(Int,Int)](Tsv("in0",(0,1)), (0,1))
    join TypedPipe.from[(Int,Int)](Tsv("in1", (0,1)), (0,1)))
   //Flatten out to three values:
    .map { kvw => (kvw._1, kvw._2._1, kvw._2._2) }
    .write(('x, 'y, 'z), Tsv("out2"))

  //Also check simple leftJoins:
  (TypedPipe.from[(Int,Int)](Tsv("in0",(0,1)), (0,1))
    leftJoin TypedPipe.from[(Int,Int)](Tsv("in1", (0,1)), (0,1)))
   //Flatten out to three values:
    .map { kvw : (Int,(Int,Option[Int])) =>
      (kvw._1, kvw._2._1, kvw._2._2.getOrElse(-1))
    }
    .write(('x, 'y, 'z), Tsv("out3"))
}

class TypedPipeJoinCountTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  import Dsl._
  "A TJoinCountJob" should {
    JobTest(new com.twitter.scalding.TJoinCountJob(_))
      .source(Tsv("in0",(0,1)), List((0,1),(0,2),(1,1),(1,5),(2,10)))
      .source(Tsv("in1",(0,1)), List((0,10),(1,20),(1,10),(1,30)))
      .sink[(Int,Long)](Tsv("out")) { outbuf =>
        val outMap = outbuf.toMap
        "correctly reduce after cogroup" in {
          outMap(0) must be_==(2)
          outMap(1) must be_==(6)
          outMap.size must be_==(2)
        }
      }
      .sink[(Int,Int,Int)](Tsv("out2")) { outbuf2 =>
        val outMap = outbuf2.groupBy { _._1 }
        "correctly do a simple join" in {
          outMap.size must be_==(2)
          outMap(0).toList.sorted must be_==(List((0,1,10),(0,2,10)))
          outMap(1).toList.sorted must be_==(List((1,1,10),(1,1,20),(1,1,30),(1,5,10),(1,5,20),(1,5,30)))
        }
      }
      .sink[(Int,Int,Int)](Tsv("out3")) { outbuf =>
        val outMap = outbuf.groupBy { _._1 }
        "correctly do a simple leftJoin" in {
          outMap.size must be_==(3)
          outMap(0).toList.sorted must be_==(List((0,1,10),(0,2,10)))
          outMap(1).toList.sorted must be_==(List((1,1,10),(1,1,20),(1,1,30),(1,5,10),(1,5,20),(1,5,30)))
          outMap(2).toList.sorted must be_==(List((2,10,-1)))
        }
      }
      .run
      .runHadoop
      .finish
  }
}

class TCrossJob(args : Args) extends Job(args) {
  (TextLine("in0") cross TextLine("in1"))
    .write(('left, 'right), Tsv("crossed"))
}

class TypedPipeCrossTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  import Dsl._
  "A TCrossJob" should {
    TUtil.printStack {
    JobTest("com.twitter.scalding.TCrossJob")
      .source(TextLine("in0"), List((0,"you"),(1,"all")))
      .source(TextLine("in1"), List((0,"every"),(1,"body")))
      .sink[(String,String)](Tsv("crossed")) { outbuf =>
        val sortedL = outbuf.toList.sorted
        "create a cross-product" in {
          sortedL must be_==(List(("all","body"),
            ("all","every"),
            ("you","body"),
            ("you","every")))
        }
      }
      .run
      .runHadoop
      .finish
    }
  }
}

class TGroupAllJob(args : Args) extends Job(args) {
  TextLine("in")
    .groupAll
    .sorted
    .values
    .write('lines, Tsv("out"))
}

class TypedGroupAllTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  import Dsl._
  "A TGroupAllJob" should {
    TUtil.printStack {
    val input = List((0,"you"),(1,"all"), (2,"everybody"))
    JobTest(new TGroupAllJob(_))
      .source(TextLine("in"), input)
      .sink[String](Tsv("out")) { outbuf =>
        val sortedL = outbuf.toList
        val correct = input.map { _._2 }.sorted
        "create sorted output" in {
          sortedL must_==(correct)
        }
      }
      .run
      .runHadoop
      .finish
    }
  }
}

class TJoinWordCount(args : Args) extends Job(args) {

  def countWordsIn(pipe: TypedPipe[(String)]) = {
    pipe.flatMap { _.split("\\s+").map(_.toLowerCase) }
      .groupBy(identity)
      .mapValueStream(input => Iterator(input.size))
  }

  val first = countWordsIn(TypedPipe.from(TextLine("in0")))

  val second = countWordsIn(TypedPipe.from(TextLine("in1")))

  first.outerJoin(second)
    .toTypedPipe
    .map { case (word, (firstCount, secondCount)) =>
        (word, firstCount.getOrElse(0), secondCount.getOrElse(0))
    }
    .write( ('word, 'first, 'second), Tsv("out"))
}

class TypedJoinWCTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  import Dsl._
  "A TJoinWordCount" should {
    TUtil.printStack {
    val in0 = List((0,"you all everybody"),(1,"a b c d"), (2,"a b c"))
    val in1 = List((0,"you"),(1,"a b c d"), (2,"a a b b c c"))
    def count(in : List[(Int,String)]) : Map[String, Int] = {
      in.flatMap { _._2.split("\\s+").map { _.toLowerCase } }.groupBy { identity }.mapValues { _.size }
    }
    def outerjoin[K,U,V](m1 : Map[K,U], z1 : U, m2 : Map[K,V], z2 : V) : Map[K,(U,V)] = {
      (m1.keys ++ m2.keys).map { k => (k, (m1.getOrElse(k, z1), m2.getOrElse(k, z2))) }.toMap
    }
    val correct = outerjoin(count(in0), 0, count(in1), 0)
      .toList
      .map { tup => (tup._1, tup._2._1, tup._2._2) }
      .sorted

    JobTest(new TJoinWordCount(_))
      .source(TextLine("in0"), in0)
      .source(TextLine("in1"), in1)
      .sink[(String,Int,Int)](Tsv("out")) { outbuf =>
        val sortedL = outbuf.toList
        "create sorted output" in {
          sortedL must_==(correct)
        }
      }
      .run
      .finish
    }
  }
}

