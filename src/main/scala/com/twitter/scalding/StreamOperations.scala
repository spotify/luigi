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
import cascading.tuple.{Tuple => CTuple, TupleEntry}

import scala.collection.JavaConverters._

import Dsl._ //Get the conversion implicits

/** Implements reductions on top of a simple abstraction for the Fields-API
 * We use the f-bounded polymorphism trick to return the type called Self
 * in each operation.
 */
trait StreamOperations[+Self <: StreamOperations[Self]] extends Sortable[Self] with java.io.Serializable {
  /** Corresponds to a Cascading Buffer
   * which allows you to stream through the data, keeping some, dropping, scanning, etc...
   * The iterator you are passed is lazy, and mapping will not trigger the
   * entire evaluation.  If you convert to a list (i.e. to reverse), you need to be aware
   * that memory constraints may become an issue.
   *
   * WARNING: Any fields not referenced by the input fields will be aligned to the first output,
   * and the final hadoop stream will have a length of the maximum of the output of this, and
   * the input stream.  So, if you change the length of your inputs, the other fields won't
   * be aligned.  YOU NEED TO INCLUDE ALL THE FIELDS YOU WANT TO KEEP ALIGNED IN THIS MAPPING!
   * POB: This appears to be a Cascading design decision.
   *
   * WARNING: mapfn needs to be stateless.  Multiple calls needs to be safe (no mutable
   * state captured)
   */
  def mapStream[T,X](fieldDef : (Fields,Fields))(mapfn : (Iterator[T]) => TraversableOnce[X])
    (implicit conv : TupleConverter[T], setter : TupleSetter[X]) : Self

  /////////////////////////////////////////
  // All the below functions are implemented in terms of the above
  /////////////////////////////////////////

  /**
   * Remove the first cnt elements
   */
  def drop(cnt : Int) : Self = {
    mapStream[CTuple,CTuple](Fields.VALUES -> Fields.ARGS){ s =>
      s.drop(cnt)
    }(CTupleConverter, CascadingTupleSetter)
  }

  /**
   * Drop while the predicate is true, starting at the first false, output all
   */
   def dropWhile[T](f : Fields)(fn : (T) => Boolean)(implicit conv : TupleConverter[T]) : Self = {
    mapStream[TupleEntry,CTuple](f -> Fields.ARGS){ s =>
      s.dropWhile(te => fn(conv(te))).map { _.getTuple }
    }(TupleEntryConverter, CascadingTupleSetter)
  }
  def scanLeft[X,T](fieldDef : (Fields,Fields))(init : X)(fn : (X,T) => X)
                 (implicit setter : TupleSetter[X], conv : TupleConverter[T]) : Self = {
    mapStream[T,X](fieldDef){ s =>
      // scala's default is not consistent in 2.8 and 2.9, this standardizes the behavior
      new ScanLeftIterator(s, init, fn)
    }(conv,setter)
  }
  
  /**
   * Only keep the first cnt elements
   */
  def take(cnt : Int) : Self = {
    mapStream[CTuple,CTuple](Fields.VALUES -> Fields.ARGS){ s =>
      s.take(cnt)
    }(CTupleConverter, CascadingTupleSetter)
  }
  
  /**
   * Take while the predicate is true, starting at the
   * first false, output all
   */
  def takeWhile[T](f : Fields)(fn : (T) => Boolean)(implicit conv : TupleConverter[T]) : Self = {
    mapStream[TupleEntry,CTuple](f -> Fields.ARGS){ s =>
      s.takeWhile(te => fn(conv(te))).map { _.getTuple }
    }(TupleEntryConverter, CascadingTupleSetter)
  }
}
