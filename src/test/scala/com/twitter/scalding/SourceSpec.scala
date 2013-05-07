package com.twitter.scalding

import org.specs._

class SourceSpec extends Specification {
  "A case class Source" should {
    "inherit equality properly from TimePathedSource" in {
      val d1 = RichDate("2012-02-01")(DateOps.UTC)
      val d2 = RichDate("2012-02-02")(DateOps.UTC)
      val d3 = RichDate("2012-02-03")(DateOps.UTC)
      val dr1 = DateRange(d1, d2)
      val dr2 = DateRange(d2, d3)

      val a = DailySuffixTsv("/test")(dr1)
      val b = DailySuffixTsv("/test")(dr2)
      val c = DailySuffixTsv("/testNew")(dr1)
      val d = DailySuffixTsvSecond("/test")(dr1)
      val e = DailySuffixTsv("/test")(dr1)

      (a == b) must beFalse
      (b == c) must beFalse
      (a == d) must beFalse
      (a == e) must beTrue
    }
  }
}

case class DailySuffixTsv(p : String)(dr : DateRange)
  extends TimePathedSource(p + TimePathedSource.YEAR_MONTH_DAY + "/*", dr, DateOps.UTC)

case class DailySuffixTsvSecond(p : String)(dr : DateRange)
  extends TimePathedSource(p + TimePathedSource.YEAR_MONTH_DAY + "/*", dr, DateOps.UTC)
