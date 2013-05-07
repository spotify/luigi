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

import java.util.TimeZone
import java.util.regex.Pattern

/*
 * All the Globification logic is encoded in this one
 * class.  It has a list of child ranges that share boundaries
 * with the current range and are completely contained within
 * current range.  This children must be ordered from largest
 * to smallest in size.
 */
class BaseGlobifier(dur : Duration, val sym: String, pattern : String, tz : TimeZone, child : Option[BaseGlobifier])
  extends java.io.Serializable {
  import DateOps._
  // result <= rd
  private def greatestLowerBound(rd : RichDate) = dur.floorOf(rd)
  // rd <= result
  private def leastUpperBound(rd : RichDate) : RichDate = {
    val lb = greatestLowerBound(rd)
    if (lb == rd)
      rd
    else
      lb + dur
  }

  def format(rd: RichDate) = rd.format(pattern)(tz)

  // Generate a lazy list of all children
  final def children : Stream[BaseGlobifier] = child match {
    case Some(c) => Stream.cons(c, c.children)
    case None => Stream.empty
  }

  final def asteriskChildren(rd : RichDate) : String = {
    val childStarPattern = children.foldLeft(pattern) { (this_pat, child) =>
      this_pat.replaceAll(Pattern.quote(child.sym), "*")
    }
    rd.format(childStarPattern)(tz)
  }

  // Handles the case of zero interior boundaries
  // with potential boundaries only at the end points.
  private def simpleCase(dr : DateRange) : List[String] = {
    val sstr = format(dr.start)
    val estr = format(dr.end)
    if (dr.end < dr.start) {
      Nil
    }
    else if (child.isEmpty) {
      //There is only one block:
      assert(sstr == estr, "Malformed heirarchy" + sstr + " != " + estr)
      List(sstr)
    }
    else {
      /*
       * Two cases: we should asterisk our children, or we need
       * to recurse.  If we fill this entire range, just asterisk,
       */
      val bottom = children.last
      val fillsright = format(leastUpperBound(dr.end)) ==
        format(bottom.leastUpperBound(dr.end))
      val fillsleft = format(greatestLowerBound(dr.start)) ==
        format(bottom.greatestLowerBound(dr.start))
      if (fillsright && fillsleft) {
        List(asteriskChildren(dr.start))
      }
      else {
        child.get.globify(dr)
      }
    }
  }

  def globify(dr : DateRange) : List[String] = {
    /* We know:
     * start <= end : by assumption
     * mid1 - start < delta : mid1 is least upper bound
     * end - mid2 < delta : mid2 is greatest lower bound
     * mid1 = mid2 + n*delta : both on the boundary.
     * if mid1 <= mid2, then we contain a boundary point,
     * else we do not.
     */
    val mid1 = leastUpperBound(dr.start)
    val mid2 = greatestLowerBound(dr.end)
    //Imprecise patterns may not need to drill down, let's see if we can stop early:
    val sstr = format(dr.start)
    val estr = format(dr.end)
    if (sstr == estr) {
      List(sstr)
    }
    else if (dr.end < dr.start) {
      //This is nonsense:
      Nil
    }
    else if (mid2 < mid1) {
      //We do not contain a boundary point:
      simpleCase(dr)
    }
    // otherwise we contain one or more than one boundary points
    else if (mid1 == mid2) {
      //we contain exactly one boundary point:
      simpleCase(DateRange(dr.start, mid1 - Millisecs(1))) ++
        simpleCase(DateRange(mid1, dr.end))
    }
    else {
      //We contain 2 or more boundary points:
      // [start <= mid1 < mid2 <= end]
      // First check to see if we even need to check our children:
      simpleCase(DateRange(dr.start, mid1 - Millisecs(1))) ++
        (asteriskChildren(mid1) ::
        globify(DateRange(mid1 + dur, dr.end)))
    }
  }
}

case class HourGlob(pat : String)(implicit tz : TimeZone)
  extends BaseGlobifier(Hours(1),"%1$tH", pat, tz, None)

case class DayGlob(pat : String)(implicit tz: TimeZone)
  extends BaseGlobifier(Days(1)(tz), "%1$td", pat, tz, Some(HourGlob(pat)))

case class MonthGlob(pat : String)(implicit tz: TimeZone)
  extends BaseGlobifier(Months(1)(tz), "%1$tm", pat, tz, Some(DayGlob(pat)))

/*
 * This is the outermost globifier and should generally be used to globify
 */
case class Globifier(pat : String)(implicit tz: TimeZone)
  extends BaseGlobifier(Years(1)(tz), "%1$tY", pat, tz, Some(MonthGlob(pat)))
  with java.io.Serializable
