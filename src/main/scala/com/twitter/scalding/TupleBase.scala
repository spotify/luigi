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

import cascading.tuple.Fields
import cascading.tuple.Tuple
import cascading.tuple.TupleEntry

abstract class TupleGetter[@specialized(Int,Long,Float,Double)T] extends java.io.Serializable {
  def get(tup : Tuple, i : Int) : T
}

/**
* Mixed in to both TupleConverter and TupleSetter to improve arity safety
* of cascading jobs before we run anything on Hadoop.
*/
trait TupleArity {
  /**
  * Return the arity of product types, should probably only be used implicitly
  * The use case here is to see how many fake field names we need in Cascading
  * to hold an intermediate value for mapReduceMap
  */
  def arity : Int

  /**
  * assert that the arity of this setter matches the fields given.
  * if arity == -1, we can't check, and if Fields is not a definite
  * size, (such as Fields.ALL), we also cannot check, so this should
  * only be considered a weak check.
  */
  def assertArityMatches(f : Fields) {
    //Fields.size == 0 for the indefinite Fields: ALL, GROUP, VALUES, UNKNOWN, etc..
    if(f.size > 0 && arity >= 0) {
      assert(arity == f.size, "Arity of (" + super.getClass + ") is "
        + arity + ", which doesn't match: + (" + f.toString + ")")
    }
  }
}

//TupleSetter[AnyRef] <: TupleSetter[String] so TupleSetter is contravariant
abstract class TupleSetter[-T] extends java.io.Serializable with TupleArity {
  def apply(arg : T) : Tuple
}

abstract class TupleConverter[@specialized(Int,Long,Float,Double)T] extends java.io.Serializable with TupleArity {
  def apply(te : TupleEntry) : T
}

trait LowPriorityConversions {
  implicit def defaultTupleGetter[T] = new TupleGetter[T] {
    def get(tup : Tuple, i : Int) = tup.getObject(i).asInstanceOf[T]
  }

  def productToTuple(in : Product) : Tuple = {
    val t = new Tuple
    in.productIterator.foreach(t.add(_))
    t
  }

  implicit def singleConverter[@specialized(Int,Long,Float,Double)A](implicit g : TupleGetter[A]) =
    new TupleConverter[A] {
        def apply(tup : TupleEntry) = g.get(tup.getTuple, 0)
        def arity = 1
    }

  implicit object SingleSetter extends TupleSetter[Any] {
    override def apply(arg : Any) = {
      val tup = new Tuple
      tup.add(arg)
      tup
    }
    override def arity = 1
  }
}
