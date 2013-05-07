/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.twitter.scalding

import com.joestelmach.natty

import java.util.Calendar
import java.util.Date
import java.util.TimeZone
import java.text.SimpleDateFormat

import scala.util.matching.Regex

/**
* Holds some coversion functions for dealing with strings as RichDate objects
*/
object DateOps extends java.io.Serializable {
  val PACIFIC = TimeZone.getTimeZone("America/Los_Angeles")
  val UTC = TimeZone.getTimeZone("UTC")

  val DATE_WITH_DASH = "yyyy-MM-dd"
  val DATEHOUR_WITH_DASH = "yyyy-MM-dd HH"
  val DATETIME_WITH_DASH = "yyyy-MM-dd HH:mm"
  val DATETIME_HMS_WITH_DASH = "yyyy-MM-dd HH:mm:ss"
  val DATETIME_HMSM_WITH_DASH = "yyyy-MM-dd HH:mm:ss.SSS"

  private val DATE_RE = """\d{4}-\d{2}-\d{2}"""
  private val SEP_RE = """(T?|\s*)"""
  private val DATE_FORMAT_VALIDATORS = List(DATE_WITH_DASH -> new Regex("""^\s*""" + DATE_RE + """\s*$"""),
                                            DATEHOUR_WITH_DASH -> new Regex("""^\s*""" + DATE_RE +
                                                                            SEP_RE + """\d\d\s*$"""),
                                            DATETIME_WITH_DASH -> new Regex("""^\s*""" + DATE_RE +
                                                                            SEP_RE + """\d\d:\d\d\s*$"""),
                                            DATETIME_HMS_WITH_DASH -> new Regex("""^\s*""" + DATE_RE +
                                                                            SEP_RE + """\d\d:\d\d:\d\d\s*$"""),
                                            DATETIME_HMSM_WITH_DASH -> new Regex("""^\s*""" + DATE_RE +
                                                                            SEP_RE + """\d\d:\d\d:\d\d\.\d{1,3}\s*$"""))
  /**
  * Return the guessed format for this datestring
  */
  def getFormat(s : String) : Option[String] = DATE_FORMAT_VALIDATORS.find{_._2.findFirstIn(s).isDefined}.map{_._1}

  /**
  * Parse the string with one of the value DATE_FORMAT_VALIDATORS in the order listed above.
  * We allow either date, date with time in minutes, date with time down to seconds.
  * The separator between date and time can be a space or "T".
  */
  implicit def stringToRichDate(str : String)(implicit tz : TimeZone) = {
   val newStr = str
                  .replace("T"," ") //We allow T to separate dates and times, just remove it and then validate
                  .replaceAll("[/_]", "-")  // Allow for slashes and underscores 
    getFormat(newStr) match {
      case Some(fmtStr) =>
        val cal = Calendar.getInstance(tz)
        val formatter = new SimpleDateFormat(fmtStr)
        formatter.setCalendar(cal)
        new RichDate(formatter.parse(newStr))
      case None => // try to parse with Natty
        val timeParser = new natty.Parser(tz)
        val dateGroups = timeParser.parse(str)
        if (dateGroups.size == 0) {
          throw new IllegalArgumentException("Could not convert string: '" + str + "' into a date.")
        }
        // a DateGroup can have more than one Date (e.g. if you do "Sept. 11th or 12th"),
        // but we're just going to take the first
        val dates = dateGroups.get(0).getDates()
        new RichDate(dates.get(0))
    }
  }
  implicit def longToRichDate(ts : Long) = new RichDate(new Date(ts))
  implicit def dateToRichDate(d : Date) = new RichDate(d)
  implicit def richDateToDate(rd : RichDate) = rd.value
  implicit def richDateToCalendar(rd : RichDate)(implicit tz : TimeZone) = {
    val cal = Calendar.getInstance(tz)
    cal.setTime(rd.value)
    cal
  }
  implicit def calendarToRichDate(cal : Calendar) = RichDate(cal.getTime())
}
