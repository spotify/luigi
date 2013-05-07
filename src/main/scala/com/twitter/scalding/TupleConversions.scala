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

import cascading.tuple.TupleEntry
import cascading.tuple.TupleEntryIterator
import cascading.tuple.{Tuple => CTuple}
import cascading.tuple.Tuples

trait TupleConversions extends GeneratedConversions {

  // Convert a TupleEntry to a List of CTuple, of length 2, with key, value
 // from the TupleEntry (useful for RichPipe.unpivot)
  def toKeyValueList(tupe : TupleEntry) : List[CTuple] = {
    val keys = tupe.getFields
    (0 until keys.size).map { idx =>
      new CTuple(keys.get(idx), tupe.getObject(idx))
    }.toList
  }

  // Convert a Cascading TupleEntryIterator into a Stream of a given type
  def toStream[T](it : TupleEntryIterator)(implicit conv : TupleConverter[T]) : Stream[T] = {
    if(null != it && it.hasNext) {
      val next = conv(it.next)
      // Note that Stream is lazy in the second parameter, so this doesn't blow up the stack
      Stream.cons(next, toStream(it)(conv))
    }
    else {
      Stream.Empty
    }
  }

  def tupleAt(idx: Int)(tup: CTuple): CTuple = {
    val obj = tup.getObject(idx)
    val res = CTuple.size(1)
    res.set(0, obj)
    res
  }

  implicit object TupleEntryConverter extends TupleConverter[TupleEntry] {
    override def apply(tup : TupleEntry) = tup
    override def arity = -1
  }

  implicit object CTupleConverter extends TupleConverter[CTuple] {
    override def apply(tup : TupleEntry) = Tuples.asUnmodifiable(tup.getTuple)
    override def arity = -1
  }


  implicit def iterableToIterable [A] (iterable : java.lang.Iterable[A]) : Iterable[A] = {
    if(iterable == null) {
      None
    } else {
      scala.collection.JavaConversions.iterableAsScalaIterable(iterable)
    }
  }

  implicit def iteratorToIterator [A] (iterator : java.util.Iterator[A]) : Iterator[A] = {
    if(iterator == null) {
      List().iterator
    } else {
      scala.collection.JavaConversions.asScalaIterator(iterator)
    }
  }

  implicit object UnitGetter extends TupleGetter[Unit] {
    override def get(tup : CTuple, i : Int) = ()
  }

  implicit object BooleanGetter extends TupleGetter[Boolean] {
    override def get(tup : CTuple, i : Int) = tup.getBoolean(i)
  }

  implicit object ShortGetter extends TupleGetter[Short] {
    override def get(tup : CTuple, i : Int) = tup.getShort(i)
  }

  implicit object IntGetter extends TupleGetter[Int] {
    override def get(tup : CTuple, i : Int) = tup.getInteger(i)
  }

  implicit object LongGetter extends TupleGetter[Long] {
    override def get(tup : CTuple, i : Int) = tup.getLong(i)
  }

  implicit object FloatGetter extends TupleGetter[Float] {
    override def get(tup : CTuple, i : Int) = tup.getFloat(i)
  }

  implicit object DoubleGetter extends TupleGetter[Double] {
    override def get(tup : CTuple, i : Int) = tup.getDouble(i)
  }

  implicit object StringGetter extends TupleGetter[String] {
    override def get(tup : CTuple, i : Int) = tup.getString(i)
  }

  //This is here for handling functions that return cascading tuples:
  implicit object CascadingTupleSetter extends TupleSetter[CTuple] {
    override def apply(arg : CTuple) = arg
    //We return an invalid value here, so we must check returns
    override def arity = -1
  }

  //Unit is like a Tuple0. It corresponds to Tuple.NULL
  implicit object UnitSetter extends TupleSetter[Unit] {
    override def apply(arg : Unit) = CTuple.NULL
    override def arity = 0
  }

  implicit object UnitConverter extends TupleConverter[Unit] {
    override def apply(arg : TupleEntry) = ()
    override def arity = 0
  }
}
