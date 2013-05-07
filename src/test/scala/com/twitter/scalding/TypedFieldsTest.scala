package com.twitter.scalding

import cascading.flow.FlowException
import org.specs._

class TypedFieldsTest extends Specification with TupleConversions {

  "A fields API job" should {

    // First we check that the untyped version fails because
    // the Opaque class has no comparator

    "throw an exception if a field is not comparable" in {

      untypedJob must throwA(new FlowException("local step failed"))

    }

    // Now run the typed fields version

    "group by custom comparator correctly" in {

      JobTest("com.twitter.scalding.TypedFieldsJob").
        arg("input", "inputFile").
        arg("output", "outputFile").
        source(TextLine("inputFile"), List("0" -> "5,foo", "1" -> "6,bar", "2" -> "9,foo")).
        sink[(Opaque,Int)](Tsv("outputFile")){ outputBuffer =>
          val outMap = outputBuffer.map { case (opaque: Opaque, i: Int) => (opaque.str, i) }.toMap
            outMap.size must_== 2
            outMap("foo") must be_==(14)
            outMap("bar") must be_==(6)
        }.
        run.
        finish

    }

  }

  def untypedJob {
    JobTest("com.twitter.scalding.UntypedFieldsJob").
      arg("input", "inputFile").
      arg("output", "outputFile").
      source(TextLine("inputFile"), List("0" -> "5,foo", "1" -> "6,bar", "2" -> "9,foo")).
      sink[(Opaque,Int)](Tsv("outputFile")){ _ => }.
      run.
      finish
  }

}

class UntypedFieldsJob(args: Args) extends Job(args) {

  TextLine(args("input")).read
    .map('line -> ('x,'y)) { line: String =>
      val split = line.split(",")
      (split(0).toInt, new Opaque(split(1)))
    }
    .groupBy('y) { _.sum('x) }
    .write(Tsv(args("output")))

}

// The only difference here is that we type the Opaque field

class TypedFieldsJob(args: Args) extends Job(args) {

  implicit val ordering = new Ordering[Opaque] {
    def compare(a: Opaque, b: Opaque) = a.str compare b.str
  }

  val xField = Field[String]('x)
  val yField = Field[Opaque]('y)

  TextLine(args("input")).read
    .map('line -> (xField, yField)) { line: String =>
    val split = line.split(",")
      (split(0).toInt, new Opaque(split(1)))
    }
    .groupBy(yField) { _.sum(xField -> xField) }
    .write(Tsv(args("output")))

}

// This is specifically not a case class - it doesn't implement any
// useful interfaces (such as Comparable)

class Opaque(val str: String) {
  override def equals(other: Any) = other match {
    case other: Opaque => str equals other.str
    case _ => false
  }
  override def hashCode = str.hashCode
}