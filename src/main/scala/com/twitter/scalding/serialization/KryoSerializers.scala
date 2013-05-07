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
package com.twitter.scalding.serialization

import java.io.InputStream
import java.io.OutputStream
import java.io.Serializable
import java.nio.ByteBuffer

import org.apache.hadoop.io.serializer.{Serialization, Deserializer, Serializer, WritableSerialization}

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.{Serializer => KSerializer}
import com.esotericsoftware.kryo.io.{Input, Output}

import cascading.kryo.KryoSerialization;
import cascading.tuple.hadoop.TupleSerialization
import cascading.tuple.hadoop.io.BufferedInputStream

import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import scala.collection.mutable.{Map => MMap}

import com.twitter.scalding.DateRange
import com.twitter.scalding.RichDate
import com.twitter.scalding.Args

/***
 * Below are some serializers for objects in the scalding project.
 */
class RichDateSerializer() extends KSerializer[RichDate] {
  // RichDates are immutable, no need to copy them
  setImmutable(true)
  def write(kser: Kryo, out: Output, date: RichDate) {
    out.writeLong(date.value.getTime, true);
  }

  def read(kser: Kryo, in: Input, cls: Class[RichDate]): RichDate = {
    RichDate(in.readLong(true))
  }
}

class DateRangeSerializer() extends KSerializer[DateRange] {
  // DateRanges are immutable, no need to copy them
  setImmutable(true)
  def write(kser: Kryo, out: Output, range: DateRange) {
    out.writeLong(range.start.value.getTime, true);
    out.writeLong(range.end.value.getTime, true);
  }

  def read(kser: Kryo, in: Input, cls: Class[DateRange]): DateRange = {
    DateRange(RichDate(in.readLong(true)), RichDate(in.readLong(true)));
  }
}

class ArgsSerializer extends KSerializer[Args] {
  // Args are immutable, no need to copy them
  setImmutable(true)
  def write(kser: Kryo, out : Output, a : Args) {
    out.writeString(a.toString)
  }
  def read(kser : Kryo, in : Input, cls : Class[Args]) : Args =
    Args(in.readString)
}
