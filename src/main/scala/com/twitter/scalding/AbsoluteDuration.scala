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

import java.util.Calendar

/*
 * These are reasonably indepedendent of calendars (or we will pretend)
 */
object AbsoluteDuration extends java.io.Serializable {
  def max(a : AbsoluteDuration, b : AbsoluteDuration) = if(a > b) a else b
}
trait AbsoluteDuration extends Duration with Ordered[AbsoluteDuration] {
  def toSeconds : Double = {
    calField match {
      case Calendar.MILLISECOND => count / 1000.0
      case Calendar.SECOND => count.toDouble
      case Calendar.MINUTE => count * 60.0
      case Calendar.HOUR => count * 60.0 * 60.0
    }
  }
  def toMillisecs : Long = {
    calField match {
      case Calendar.MILLISECOND => count.toLong
      case Calendar.SECOND => count.toLong * 1000L
      case Calendar.MINUTE => count.toLong * 1000L * 60L
      case Calendar.HOUR => count.toLong * 1000L * 60L * 60L
    }
  }
  def compare(that : AbsoluteDuration) : Int = {
    this.toMillisecs.compareTo(that.toMillisecs)
  }
  def +(that : AbsoluteDuration) = {
    Duration.fromMillisecs(this.toMillisecs + that.toMillisecs)
  }
}

case class Millisecs(cnt : Int) extends Duration(Calendar.MILLISECOND, cnt, DateOps.UTC)
  with AbsoluteDuration

case class Seconds(cnt : Int) extends Duration(Calendar.SECOND, cnt, DateOps.UTC)
  with AbsoluteDuration

case class Minutes(cnt : Int) extends Duration(Calendar.MINUTE, cnt, DateOps.UTC)
  with AbsoluteDuration

case class Hours(cnt : Int) extends Duration(Calendar.HOUR, cnt, DateOps.UTC)
  with AbsoluteDuration

case class AbsoluteDurationList(parts : List[AbsoluteDuration])
  extends AbstractDurationList[AbsoluteDuration](parts) with AbsoluteDuration {
  override def toSeconds = parts.map{ _.toSeconds }.sum
  override def toMillisecs : Long = parts.map{ _.toMillisecs }.sum
}
