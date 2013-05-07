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

import scala.annotation.tailrec

/**
* represents a closed interval of time.
*
* TODO: This should be Range[RichDate, Duration] for an appropriate notion
* of Range
*/
case class DateRange(val start : RichDate, val end : RichDate) {
  import DateOps._
  /**
  * shift this by the given unit
  */
  def +(timespan : Duration) = DateRange(start + timespan, end + timespan)
  def -(timespan : Duration) = DateRange(start - timespan, end - timespan)

  def isBefore(d : RichDate) = end < d
  def isAfter(d : RichDate) = d < start
  /**
  * make the range wider by delta on each side.  Good to catch events which
  * might spill over.
  */
  def embiggen(delta : Duration) = DateRange(start - delta, end + delta)
  /**
  * Extend the length by moving the end. We can keep the party going, but we
  * can't start it earlier.
  */
  def extend(delta : Duration) = DateRange(start, end + delta)

  def contains(point : RichDate) = (start <= point) && (point <= end)
  /**
  * Is the given Date range a (non-strict) subset of the given range
  */
  def contains(dr : DateRange) = start <= dr.start && dr.end <= end

  /**
   * produce a contiguous non-overlapping set of DateRanges
   * whose union is equivalent to this.
   * If it is passed an integral unit of time (not a DurationList), it stops at boundaries
   * which are set by the start timezone, else break at start + k * span.
   */
  def each(span : Duration) : Iterable[DateRange] = {
    //tail recursive method which produces output (as a stack, so it is
    //reversed). acc is the accumulated list so far:
    @tailrec def eachRec(acc : List[DateRange], nextDr : DateRange) : List[DateRange] = {
      val next_start = span.floorOf(nextDr.start) + span
      //the smallest grain of time we count is 1 millisecond
      val this_end = next_start - Millisecs(1)
      if( nextDr.end <= this_end ) {
        //This is the last block, output and end:
        nextDr :: acc
      }
      else {
        //Put today's portion, and then start on tomorrow:
        val today = DateRange(nextDr.start, this_end)
        eachRec(today :: acc, DateRange(next_start, nextDr.end))
      }
    }
    //have to reverse because eachDayRec produces backwards
    eachRec(Nil, this).reverse
  }
}
