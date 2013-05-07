package com.twitter.scalding

import org.specs._
import com.twitter.scalding._
import com.codahale.jerkson.Json

class JsonLineJob(args : Args) extends Job(args) {
  try {
    Tsv("input0", ('query, 'queryCount)).read.write(JsonLine("output0"))
  } catch {
    case e : Exception => e.printStackTrace()
  }
}

class FileSourceTest extends Specification {
  noDetailedDiffs()
  import Dsl._

  "A JsonLine sink" should {
    JobTest("com.twitter.scalding.JsonLineJob")
      .source(Tsv("input0", ('query, 'queryCount)), List(("doctor's mask", 42)))
      .sink[String](JsonLine("output0")) { buf =>
        val json = buf.head
        "not escape single quotes" in {
            json must be_==("""{"query":"doctor's mask","queryCount":"42"}""")
        }
      }
      .run
      .finish
  }
}