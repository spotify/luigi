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

import cascading.tap.Tap
import java.util.Properties
import cascading.tuple._
import scala.collection.JavaConversions._
import cascading.scheme.Scheme
import cascading.flow.FlowProcess
import collection.mutable.{Buffer, MutableList}

class MemoryTap[In,Out](val scheme : Scheme[Properties,In,Out,_,_], val tupleBuffer : Buffer[Tuple])
  extends Tap[Properties, In, Out](scheme) {

  override def createResource(conf : Properties) = true
  override def deleteResource(conf : Properties) = true
  override def resourceExists(conf : Properties) = true
  override def getModifiedTime(conf : Properties) = 1L
  override def getIdentifier() : String = scala.math.random.toString

  override def openForRead(flowProcess : FlowProcess[Properties], input : In) = {
    new TupleEntryChainIterator(scheme.getSourceFields, tupleBuffer.toIterator)
  }

  override def openForWrite(flowProcess : FlowProcess[Properties], output : Out) : TupleEntryCollector = {
    new MemoryTupleEntryCollector(tupleBuffer)
  }

  override def equals(other : Any) = this.eq(other.asInstanceOf[AnyRef])

  override def hashCode() = System.identityHashCode(this)

}

class MemoryTupleEntryCollector(val tupleBuffer : Buffer[Tuple]) extends TupleEntryCollector {

  override def collect(tupleEntry : TupleEntry) {
    tupleBuffer += tupleEntry.getTuple
  }
}
