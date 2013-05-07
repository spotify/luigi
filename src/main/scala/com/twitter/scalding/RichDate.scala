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

import java.text.SimpleDateFormat

import java.util.Calendar
import java.util.Date
import java.util.TimeZone

/**
* RichDate adds some nice convenience functions to the Java date/calendar classes
* We commonly do Date/Time work in analysis jobs, so having these operations convenient
* is very helpful.
*/
object RichDate {
  def apply(s : String)(implicit tz : TimeZone) = {
    DateOps.stringToRichDate(s)(tz)
  }
  def apply(l : Long) = {
    DateOps.longToRichDate(l)
  }
  def upperBound(s : String)(implicit tz : TimeZone) = {
    val end = apply(s)(tz)
    (DateOps.getFormat(s) match {
      case Some(DateOps.DATE_WITH_DASH) => end + Days(1)
      case Some(DateOps.DATEHOUR_WITH_DASH) => end + Hours(1)
      case Some(DateOps.DATETIME_WITH_DASH) => end + Minutes(1)
      case Some(DateOps.DATETIME_HMS_WITH_DASH) => end + Seconds(1)
      case Some(DateOps.DATETIME_HMSM_WITH_DASH) => end + Millisecs(2)
      case None => Days(1).floorOf(end + Days(1))
    }) - Millisecs(1)
  }
}

case class RichDate(val value : Date) extends Ordered[RichDate] {
  def +(interval : Duration) = interval.addTo(this)
  def -(interval : Duration) = interval.subtractFrom(this)

  //Inverse of the above, d2 + (d1 - d2) == d1
  def -(that : RichDate) = Duration.fromMillisecs(value.getTime - that.value.getTime)

  override def compare(that : RichDate) : Int = {
    if (value.before(that.value)) {
      -1
    }
    else if (value.after(that.value)) {
      1
    } else {
      0
    }
  }

  //True of the other is a RichDate with equal value, or a Date equal to value
  override def equals(that : Any) = {
    //Due to type erasure (scala 2.9 complains), we need to use a manifest:
    def opInst[T : Manifest](v : Any) = {
      val klass = manifest[T].erasure
      if(null != v && klass.isInstance(v)) Some(v.asInstanceOf[T]) else None
    }
    opInst[RichDate](that)
      .map( _.value)
      .orElse(opInst[Date](that))
      .map( _.equals(value) )
      .getOrElse(false)
  }

  /** Use String.format to format the date, as opposed to toString with uses SimpleDateFormat
   */
  def format(pattern: String)(implicit tz: TimeZone) : String = String.format(pattern, toCalendar(tz))

  override def hashCode = { value.hashCode }

  //milliseconds since the epoch
  def timestamp : Long = value.getTime

  def toCalendar(implicit tz: TimeZone) = {
    val cal = Calendar.getInstance(tz)
    cal.setTime(value)
    cal
  }
  override def toString = {
    value.toString
  }

  /** Use SimpleDateFormat to print the string
   */
  def toString(fmt : String)(implicit tz : TimeZone) : String = {
    val cal = toCalendar(tz)
    val sdfmt = new SimpleDateFormat(fmt)
    sdfmt.setCalendar(cal)
    sdfmt.format(cal.getTime)
  }
}

