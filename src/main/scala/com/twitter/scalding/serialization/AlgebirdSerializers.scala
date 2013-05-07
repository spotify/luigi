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

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.{Serializer => KSerializer}
import com.esotericsoftware.kryo.io.{Input, Output}

import com.twitter.algebird.{AveragedValue, DecayedValue, HLL, HyperLogLog,
  HyperLogLogMonoid, Moments}

import scala.collection.mutable.{Map => MMap}

class AveragedValueSerializer extends KSerializer[AveragedValue] {
  setImmutable(true)
  def write(kser: Kryo, out : Output, s : AveragedValue) {
    out.writeLong(s.count, true)
    out.writeDouble(s.value)
  }
  def read(kser : Kryo, in : Input, cls : Class[AveragedValue]) : AveragedValue =
    AveragedValue(in.readLong(true), in.readDouble)
}

class MomentsSerializer extends KSerializer[Moments] {
  setImmutable(true)
  def write(kser: Kryo, out : Output, s : Moments) {
    out.writeLong(s.m0, true)
    out.writeDouble(s.m1)
    out.writeDouble(s.m2)
    out.writeDouble(s.m3)
    out.writeDouble(s.m4)
  }
  def read(kser : Kryo, in : Input, cls : Class[Moments]) : Moments = {
    Moments(in.readLong(true),
      in.readDouble,
      in.readDouble,
      in.readDouble,
      in.readDouble)
  }
}


class DecayedValueSerializer extends KSerializer[DecayedValue] {
  setImmutable(true)
  def write(kser: Kryo, out : Output, s : DecayedValue) {
    out.writeDouble(s.value)
    out.writeDouble(s.scaledTime)
  }
  def read(kser : Kryo, in : Input, cls : Class[DecayedValue]) : DecayedValue =
    DecayedValue(in.readDouble, in.readDouble)
}

class HLLSerializer extends KSerializer[HLL] {
  setImmutable(true)
  def write(kser: Kryo, out : Output, s : HLL) {
    val bytes = HyperLogLog.toBytes(s)
    out.writeInt(bytes.size, true)
    out.writeBytes(bytes)
  }
  def read(kser : Kryo, in : Input, cls : Class[HLL]) : HLL = {
    HyperLogLog.fromBytes(in.readBytes(in.readInt(true)))
  }
}

class HLLMonoidSerializer extends KSerializer[HyperLogLogMonoid] {
  setImmutable(true)
  val hllMonoids = MMap[Int,HyperLogLogMonoid]()
  def write(kser: Kryo, out : Output, mon : HyperLogLogMonoid) {
    out.writeInt(mon.bits, true)
  }
  def read(kser : Kryo, in : Input, cls : Class[HyperLogLogMonoid]) : HyperLogLogMonoid = {
    val bits = in.readInt(true)
    hllMonoids.getOrElseUpdate(bits, new HyperLogLogMonoid(bits))
  }
}
