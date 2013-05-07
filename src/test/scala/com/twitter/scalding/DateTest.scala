package com.twitter.scalding

import org.specs._
import DateOps._
import java.util.Calendar

class DateTest extends Specification {
  noDetailedDiffs()
  implicit val tz = DateOps.PACIFIC

  "A RichDate" should {
    "implicitly convert strings" in {
      val rd1 : RichDate = "2011-10-20"
      val rd2 : RichDate = "2011-10-20"
      rd1 must be_==(rd2)
    }
    "implicitly convert calendars" in {
      val rd1 : RichDate = "2011-10-20"
      val cal = Calendar.getInstance(tz)
      cal.setTime(rd1.value)
      val rd2 : RichDate = cal
      rd1 must_== rd2
    }
    "deal with strings with spaces" in {
      val rd1 : RichDate = "   2011-10-20 "
      val rd2 : RichDate = "2011-10-20 "
      val rd3 : RichDate = " 2011-10-20     "
      rd1 must be_==(rd2)
      rd1 must be_==(rd3)
    }
    "handle dates with slashes and underscores" in {
      val rd1 : RichDate = "2011-10-20"
      val rd2 : RichDate = "2011/10/20"
      val rd3 : RichDate = "2011_10_20"
      rd1 must be_==(rd2)
      rd1 must be_==(rd3)
    }
    "be able to parse milliseconds" in {
      val rd1 : RichDate = "2011-10-20 20:01:11.0"
      val rd2 : RichDate = "2011-10-20   22:11:24.23"
      val rd3 : RichDate = "2011-10-20 22:11:24.023    "
      rd2 must_== rd3
    }
    "throw an exception when trying to parse illegal strings" in {
      // Natty is *really* generous about what it accepts
      RichDate("jhbjhvhjv") must throwAn[IllegalArgumentException]
      RichDate("99-99-99") must throwAn[IllegalArgumentException]
    }
    "be able to deal with arithmetic operations with whitespace" in {
      val rd1 : RichDate = RichDate("2010-10-02") + Seconds(1)
      val rd2 : RichDate = "  2010-10-02  T  00:00:01    "
      rd1 must be_==(rd2)
    }
    "be well ordered" in {
      val rd1 : RichDate = "2011-10-20"
      val rd2 : RichDate = "2011-10-21"
      rd1 must be_<(rd2)
      rd1 must be_<=(rd2)
      rd2 must be_>(rd1)
      rd2 must be_>=(rd1)
      rd1 must be_>=(rd1)
      rd2 must be_>=(rd2)
    }
    "implicitly convert from long" in {
      //This is close to: Mon Oct 24 20:03:13 PDT 2011
      val long_val = 1319511818135L
      val rd1 = "2011-10-24T20:03:00"
      val rd2 = "2011-10-24T20:04:00"
      DateRange(rd1, rd2).contains(long_val) must beTrue
      //Check edge cases:
      DateRange(rd1, long_val).contains(long_val) must beTrue
      DateRange(rd1, (long_val+1)).contains(long_val) must beTrue
      DateRange(long_val, rd2).contains(long_val) must beTrue
      DateRange((long_val-1), rd2).contains(long_val) must beTrue

      DateRange(rd1, "2011-10-24T20:03:01").contains(long_val) must beFalse
      DateRange(rd1, (long_val-1)).contains(long_val) must beFalse
      DateRange((long_val+1), rd2).contains(long_val) must beFalse
    }
    "roundtrip successfully" in {
      val start_str = "2011-10-24 20:03:00"
      //string -> date -> string
      stringToRichDate(start_str).toString(DATETIME_HMS_WITH_DASH) must_== start_str
      //long -> date == date -> long -> date
      val long_val = 1319511818135L
      val date : RichDate = long_val
      val long2 = date.value.getTime
      val date2 : RichDate = long2
      date must_== date2
      long_val must_== long2
    }
    "know the most recent time units" in {
      //10-25 is a Tuesday, earliest in week is a monday
      Weeks(1).floorOf("2011-10-25") must_==(stringToRichDate("2011-10-24"))
      Days(1).floorOf("2011-10-25 10:01") must_==(stringToRichDate("2011-10-25 00:00"))
      //Leaving off the time should give the same result:
      Days(1).floorOf("2011-10-25 10:01") must_==(stringToRichDate("2011-10-25"))
      Hours(1).floorOf("2011-10-25 10:01") must_==(stringToRichDate("2011-10-25 10:00"))
    }
    "correctly do arithmetic" in {
      val d1 : RichDate = "2011-10-24"
      (-4 to 4).foreach { n =>
        List(Hours, Minutes, Seconds, Millisecs).foreach { u =>
          val d2 = d1 + u(n)
          if (n != 0) {
            //0 is a degenerate case
            (d2 - d1) must_== u(n)
          }
        }
      }
    }
    "be able to parse a natural language date" in {
      Days(1).floorOf(stringToRichDate("the 19th day of January, 2012")) must_== stringToRichDate("2012-01-19 00:00")
    }
    "correctly calculate upperBound" in {
      Seconds(1).floorOf(RichDate.upperBound("2010-10-01")) must_== Seconds(1).floorOf(RichDate("2010-10-01 23:59:59"))
      Seconds(1).floorOf(RichDate.upperBound("2010-10-01 14")) must_== Seconds(1).floorOf(RichDate("2010-10-01 14:59:59"))
      Seconds(1).floorOf(RichDate.upperBound("2010-10-01 14:15")) must_== Seconds(1).floorOf(RichDate("2010-10-01 14:15:59"))
    }
    "correctly calculate upperBound using natural language dates" in {
      // for natural language dates, we have to assume a resolution of a day, since Natty
      // converts everything to a Date with a the current time if time was not specified
      RichDate.upperBound("October 1, 2010") must_== RichDate.upperBound("2010-10-01")
    }
  }
  "A DateRange" should {
    "correctly iterate on each duration" in {
      def rangeContainTest(d1 : DateRange, dur : Duration) = {
        d1.each(dur).forall( (d1r : DateRange) => d1.contains(d1r) ) must beTrue
      }
      rangeContainTest(DateRange("2010-10-01", "2010-10-13"), Weeks(1))
      rangeContainTest(DateRange("2010-10-01", "2010-10-13"), Weeks(2))
      rangeContainTest(DateRange("2010-10-01", "2010-10-13"), Days(1))
      //Prime non one:
      rangeContainTest(DateRange("2010-10-01", "2010-10-13"), Days(5))
      //Prime number of Minutes
      rangeContainTest(DateRange("2010-10-01", "2010-10-13"), Minutes(13))
      rangeContainTest(DateRange("2010-10-01", "2010-10-13"), Hours(13))
      DateRange("2010-10-01", "2010-10-10").each(Days(1)).size must_== 10
      DateRange("2010-10-01 00:00", RichDate("2010-10-02") - Millisecs(1)).each(Hours(1)).size must_== 24
      DateRange("2010-10-01 00:00", RichDate("2010-10-02") + Millisecs(1)).each(Hours(1)).size must_== 25
      DateRange("2010-10-01",RichDate.upperBound("2010-10-20")).each(Days(1)).size must_== 20
      DateRange("2010-10-01",RichDate.upperBound("2010-10-01")).each(Hours(1)).size must_== 24
      DateRange("2010-10-31",RichDate.upperBound("2010-10-31")).each(Hours(1)).size must_== 24
      DateRange("2010-10-31",RichDate.upperBound("2010-10-31")).each(Days(1)).size must_== 1
      DateRange("2010-10-31 12:00",RichDate.upperBound("2010-10-31 13")).each(Minutes(1)).size must_== 120
    }
    "have each partition disjoint and adjacent" in {
      def eachIsDisjoint(d : DateRange, dur : Duration) {
        val dl = d.each(dur)
        dl.zip(dl.tail).forall { case (da, db) =>
          da.isBefore(db.start) && db.isAfter(da.end) && ((da.end + Millisecs(1)) == db.start)
        } must beTrue
      }
      eachIsDisjoint(DateRange("2010-10-01", "2010-10-03"), Days(1))
      eachIsDisjoint(DateRange("2010-10-01", "2010-10-03"), Weeks(1))
      eachIsDisjoint(DateRange("2010-10-01", "2011-10-03"), Weeks(1))
      eachIsDisjoint(DateRange("2010-10-01", "2010-10-03"), Months(1))
      eachIsDisjoint(DateRange("2010-10-01", "2011-10-03"), Months(1))
      eachIsDisjoint(DateRange("2010-10-01", "2010-10-03"), Hours(1))
      eachIsDisjoint(DateRange("2010-10-01", "2010-10-03"), Hours(2))
      eachIsDisjoint(DateRange("2010-10-01", "2010-10-03"), Minutes(1))
    }
  }
  "Time units" should {
    def isSame(d1 : Duration, d2 : Duration) = {
      (RichDate("2011-12-01") + d1) == (RichDate("2011-12-01") + d2)
    }
    "have 1000 milliseconds in a sec" in {
      isSame(Millisecs(1000), Seconds(1)) must beTrue
      Seconds(1).toMillisecs must_== 1000L
      Millisecs(1000).toSeconds must_== 1.0
      Seconds(2).toMillisecs must_== 2000L
      Millisecs(2000).toSeconds must_== 2.0
    }
    "have 60 seconds in a minute" in {
       isSame(Seconds(60), Minutes(1)) must beTrue
       Minutes(1).toSeconds must_== 60.0
       Minutes(1).toMillisecs must_== 60 * 1000L
       Minutes(2).toSeconds must_== 120.0
       Minutes(2).toMillisecs must_== 120 * 1000L
     }
    "have 60 minutes in a hour" in {
      isSame(Minutes(60),Hours(1)) must beTrue
       Hours(1).toSeconds must_== 60.0 * 60.0
       Hours(1).toMillisecs must_== 60 * 60 * 1000L
       Hours(2).toSeconds must_== 2 * 60.0 * 60.0
       Hours(2).toMillisecs must_== 2 * 60 * 60 * 1000L
    }
    "have 7 days in a week" in { isSame(Days(7), Weeks(1)) must beTrue }
  }
  "AbsoluteDurations" should {
    "behave as comparable" in {
      (Hours(5) >= Hours(2)) must beTrue
      (Minutes(60) >= Minutes(60)) must beTrue
      (Hours(1) < Millisecs(3600001)) must beTrue
    }
    "add properly" in {
      (Hours(2) + Hours(1)).compare(Hours(3)) must_== 0
    }
    "have a well behaved max function" in {
      AbsoluteDuration.max(Hours(1), Hours(2)).compare(Hours(2)) must_== 0
    }
  }
  "Globifiers" should {
    "handle specific hand crafted examples" in {
      val t1 = Globifier("/%1$tY/%1$tm/%1$td/%1$tH")
      val t2 = Globifier("/%1$tY/%1$tm/%1$td/")

      val testcases =
        (t1.globify(DateRange("2011-12-01T14", "2011-12-04")),
        List("/2011/12/01/14","/2011/12/01/15","/2011/12/01/16","/2011/12/01/17","/2011/12/01/18",
          "/2011/12/01/19","/2011/12/01/20", "/2011/12/01/21","/2011/12/01/22","/2011/12/01/23",
          "/2011/12/02/*","/2011/12/03/*","/2011/12/04/00")) ::
        (t1.globify(DateRange("2011-12-01", "2011-12-01T23:59")),
        List("/2011/12/01/*")) ::
        (t1.globify(DateRange("2011-12-01T12", "2011-12-01T12:59")),
        List("/2011/12/01/12")) ::
        (t1.globify(DateRange("2011-12-01T12", "2011-12-01T14")),
        List("/2011/12/01/12","/2011/12/01/13","/2011/12/01/14")) ::
        (t2.globify(DateRange("2011-12-01T14", "2011-12-04")),
        List("/2011/12/01/","/2011/12/02/","/2011/12/03/","/2011/12/04/")) ::
        (t2.globify(DateRange("2011-12-01", "2011-12-01T23:59")),
        List("/2011/12/01/")) ::
        (t2.globify(DateRange("2011-12-01T12", "2011-12-01T12:59")),
        List("/2011/12/01/")) ::
        (t2.globify(DateRange("2011-12-01T12", "2012-01-02T14")),
        List("/2011/12/*/","/2012/01/01/","/2012/01/02/")) ::
        (t2.globify(DateRange("2011-11-01T12", "2011-12-02T14")),
        List("/2011/11/*/","/2011/12/01/","/2011/12/02/")) ::
        (t2.globify(DateRange("October 2, 2012", RichDate.upperBound("October 3, 2012"))),
        List("/2012/10/02/", "/2012/10/03/")) ::
        (t2.globify(DateRange("October 1, 2012", RichDate.upperBound("October 1, 2012"))),
        List("/2012/10/01/")) ::
        Nil

       testcases.foreach { tup =>
         tup._1 must_== tup._2
       }
    }
    def eachElementDistinct(dates : List[String]) = dates.size == dates.toSet.size
    def globMatchesDate(glob : String)(date : String) = {
      java.util.regex.Pattern.matches(glob.replaceAll("\\*","[0-9]*"), date)
    }
    def bruteForce(pattern : String, dr : DateRange, dur : Duration)(implicit tz : java.util.TimeZone) = {
      dr.each(dur)
        .map { (dr : DateRange) => String.format(pattern, dr.start.toCalendar(tz)) }
    }

    "handle random test cases" in {
      val pattern = "/%1$tY/%1$tm/%1$td/%1$tH"
      val t1 = Globifier(pattern)

      val r = new java.util.Random()
      (0 until 100) foreach { step =>
        val start = RichDate("2011-08-03").value.getTime + r.nextInt(Int.MaxValue)
        val dr = DateRange(start, start + r.nextInt(Int.MaxValue))
        val splits = bruteForce(pattern, dr, Hours(1))
        val globed = t1.globify(dr)

        eachElementDistinct(globed) must beTrue
        //See that each path is matched by exactly one glob:
        splits.map { path => globed.filter { globMatchesDate(_)(path) }.size }
          .forall { _ == 1 } must beTrue
      }
    }
  }
}
