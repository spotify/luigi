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

import com.twitter.algebird.{
  Monoid,
  Ring,
  AveragedValue,
  Moments,
  SortedTakeListMonoid,
  HyperLogLogMonoid,
  Aggregator
}

import scala.collection.JavaConverters._

import Dsl._ //Get the conversion implicits

/** Implements reductions on top of a simple abstraction for the Fields-API
 * This is for associative and commutive operations (particularly Monoids play a big role here)
 *
 * We use the f-bounded polymorphism trick to return the type called Self
 * in each operation.
 */
trait ReduceOperations[+Self <: ReduceOperations[Self]] extends java.io.Serializable {
 /**
  * Type T is the type of the input field (input to map, T => X)
  * Type X is the intermediate type, which your reduce function operates on
  * (reduce is (X,X) => X)
  * Type U is the final result type, (final map is: X => U)
  *
  * The previous output goes into the reduce function on the left, like foldLeft,
  * so if your operation is faster for the accumulator to be on one side, be aware.
  */
  def mapReduceMap[T,X,U](fieldDef : (Fields, Fields))(mapfn : T => X )(redfn : (X, X) => X)
      (mapfn2 : X => U)(implicit startConv : TupleConverter[T],
                        middleSetter : TupleSetter[X],
                        middleConv : TupleConverter[X],
                        endSetter : TupleSetter[U]) : Self

  /////////////////////////////////////////
  // All the below functions are implemented in terms of the above
  /////////////////////////////////////////

  /** Pretty much a synonym for mapReduceMap with the methods collected into a trait. */
  def aggregate[A,B,C](fieldDef : (Fields, Fields))(ag: Aggregator[A,B,C])
    (implicit startConv : TupleConverter[A],
                        middleSetter : TupleSetter[B],
                        middleConv : TupleConverter[B],
                        endSetter : TupleSetter[C]): Self =
    mapReduceMap[A,B,C](fieldDef)(ag.prepare _)(ag.reduce _)(ag.present _)

  /**
   * uses a more stable online algorithm which should
   * be suitable for large numbers of records
   *
   * == Similar To ==
   * <a href="http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm">http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm</a>
   */
  def average(f : (Fields, Fields)) = mapPlusMap(f) { (x : Double) => AveragedValue(1L, x) } { _.value }
  def average(f : Symbol) : Self = average(f->f)

  /**
   * Approximate number of unique values
   * We use about m = (104/errPercent)^2 bytes of memory per key
   * Uses `.toString.getBytes` to serialize the data so you MUST
   * ensure that .toString is an equivalance on your counted fields
   * (i.e. `x.toString == y.toString` if and only if `x == y`)
   *
   * For each key:
   * {{{
   * 10% error ~ 256 bytes
   * 5% error ~ 1kb
   * 1% error ~ 8kb
   * 0.5% error ~ 64kb
   * 0.25% error ~ 256kb
   * }}}
   */
  def approxUniques(f : (Fields, Fields), errPercent : Double = 1.0)  = {
    //bits = log(m) == 2 *log(104/errPercent) = 2log(104) - 2*log(errPercent)
    def log2(x : Double) = scala.math.log(x)/scala.math.log(2.0)
    val bits = 2 * scala.math.ceil(log2(104) - log2(errPercent)).toInt
    implicit val hmm = new HyperLogLogMonoid(bits)
    mapPlusMap(f) { (in : CTuple) => hmm.create(in.toString.getBytes("UTF-8")) }
     { hmm.estimateSize(_) }
  }

  /**
   * This is count with a predicate: only counts the tuples for which
   * `fn(tuple)` is true
   */
  def count[T:TupleConverter](fieldDef : (Fields, Fields))(fn : T => Boolean) : Self = {
    mapPlusMap(fieldDef){(arg : T) => if(fn(arg)) 1L else 0L} { s => s }
  }

  /**
   * Opposite of RichPipe.unpivot.  See SQL/Excel for more on this function
   * converts a row-wise representation into a column-wise one.
   *
   * == Example ==
   * {{{
   * pivot(('feature, 'value) -> ('clicks, 'impressions, 'requests))
   * }}}
   *
   * it will find the feature named "clicks", and put the value in the column with the field named
   * clicks.
   *
   * Absent fields result in null unless a default value is provided. Unnamed output fields are ignored.
   *
   * == Note ==
   * Duplicated fields will result in an error.
   *
   * == Hint ==
   * if you want more precision, first do a
   *
   * {{{
   * map('value -> value) { x : AnyRef => Option(x) }
   * }}}
   *
   * and you will have non-nulls for all present values, and Nones for values that were present
   * but previously null.  All nulls in the final output will be those truly missing.
   * Similarly, if you want to check if there are any items present that shouldn't be:
   *
   * {{{
   * map('feature -> 'feature) { fname : String =>
   *   if (!goodFeatures(fname)) { throw new Exception("ohnoes") }
   *   else fname
   * }
   * }}}
   */
  def pivot(fieldDef : (Fields, Fields), defaultVal : Any = null) : Self = {
    // Make sure the fields are strings:
    mapList[(String,AnyRef),CTuple](fieldDef) { outputList =>
      val asMap = outputList.toMap
      assert(asMap.size == outputList.size, "Repeated pivot key fields: " + outputList.toString)
      val values = fieldDef._2
        .iterator.asScala
        // Look up this key:
        .map { fname => asMap.getOrElse(fname.asInstanceOf[String], defaultVal.asInstanceOf[AnyRef]) }
      // Create the cascading tuple
      new CTuple(values.toSeq : _*)
    }
  }

  /**
   * Compute the count, ave and standard deviation in one pass
   * example: g.sizeAveStdev('x -> ('cntx, 'avex, 'stdevx))
   */
  def sizeAveStdev(fieldDef : (Fields,Fields)) = {
    mapPlusMap(fieldDef) { (x : Double) => Moments(x) }
    { (mom : Moments) => (mom.count, mom.mean, mom.stddev) }
  }

  /*
   * check if a predicate is satisfied for all in the values for this key
   */
  def forall[T:TupleConverter](fieldDef : (Fields,Fields))(fn : (T) => Boolean) : Self = {
    mapReduceMap(fieldDef)(fn)({(x : Boolean, y : Boolean) => x && y})({ x => x })
  }

  /**
   * Return the first, useful probably only for sorted case.
   */
  def head(fd : (Fields,Fields)) : Self = {
    //CTuple's have unknown arity so we have to put them into a Tuple1 in the middle phase:
    mapReduceMap(fd) { ctuple : CTuple => Tuple1(ctuple) }
      { (oldVal, newVal) => oldVal }
      { result => result._1 }
  }
  def head(f : Symbol*) : Self = head(f -> f)

  def last(fd : (Fields,Fields)) = {
    //CTuple's have unknown arity so we have to put them into a Tuple1 in the middle phase:
    mapReduceMap(fd) { ctuple : CTuple => Tuple1(ctuple) }
      { (oldVal, newVal) => newVal }
      { result => result._1 }
  }
  def last(f : Symbol*) : Self = last(f -> f)

  /**
   * Collect all the values into a List[T] and then operate on that
   * list. This fundamentally uses as much memory as it takes to store the list.
   * This gives you the list in the reverse order it was encounted (it is built
   * as a stack for efficiency reasons). If you care about order, call .reverse in your fn
   *
   * STRONGLY PREFER TO AVOID THIS. Try reduce or plus and an O(1) memory algorithm.
   */
  def mapList[T,R](fieldDef : (Fields, Fields))(fn : (List[T]) => R)
    (implicit conv : TupleConverter[T], setter : TupleSetter[R]) : Self = {
    val midset = implicitly[TupleSetter[List[T]]]
    val midconv = implicitly[TupleConverter[List[T]]]

    mapReduceMap[T, List[T], R](fieldDef) { //Map
      x => List(x)
    } { //Reduce, note the bigger list is likely on the left, so concat into it:
      (prev, current) => current ++ prev
    } { fn(_) }(conv, midset, midconv, setter)
  }

  def mapPlusMap[T,X,U](fieldDef : (Fields, Fields))(mapfn : T => X)(mapfn2 : X => U)
    (implicit startConv : TupleConverter[T],
                        middleSetter : TupleSetter[X],
                        middleConv : TupleConverter[X],
                        endSetter : TupleSetter[U],
                        monX : Monoid[X]) : Self = {
    mapReduceMap[T,X,U](fieldDef) (mapfn)((x,y) => monX.plus(x,y))(mapfn2) (startConv, middleSetter, middleConv, endSetter)
  }

  private def extremum(max : Boolean, fieldDef : (Fields,Fields)) : Self = {
    //CTuple's have unknown arity so we have to put them into a Tuple1 in the middle phase:
    val select = if(max) {
        { (a : CTuple, b : CTuple) => (a.compareTo(b) >= 0) }
      }
      else {
        { (a : CTuple, b : CTuple) => (a.compareTo(b) <= 0) }
      }

    mapReduceMap(fieldDef) { ctuple : CTuple => Tuple1(ctuple) }
      { (oldVal, newVal) => if (select(oldVal._1, newVal._1)) oldVal else newVal }
      { result => result._1 }
  }
  def max(fieldDef : (Fields, Fields)) = extremum(true, fieldDef)
  def max(f : Symbol*) = extremum(true, (f -> f))
  def min(fieldDef : (Fields, Fields)) = extremum(false, fieldDef)
  def min(f : Symbol*) = extremum(false, (f -> f))

  /**
   * Similar to the scala.collection.Iterable.mkString
   * takes the source and destination fieldname, which should be a single
   * field. The result will be start, each item.toString separated by sep,
   * followed by end for convenience there several common variants below
   */
  def mkString(fieldDef : (Fields,Fields), start : String, sep : String, end : String) : Self = {
    mapList[String,String](fieldDef) { _.mkString(start, sep, end) }
  }
  def mkString(fieldDef : (Fields,Fields), sep : String) : Self = mkString(fieldDef,"",sep,"")
  def mkString(fieldDef : (Fields,Fields)) : Self = mkString(fieldDef,"","","")
  /**
  * these will only be called if a tuple is not passed, meaning just one
  * column
  */
  def mkString(fieldDef : Symbol, start : String, sep : String, end : String) : Self = {
    val f : Fields = fieldDef
    mkString((f,f),start,sep,end)
  }
  def mkString(fieldDef : Symbol, sep : String) : Self = mkString(fieldDef,"",sep,"")
  def mkString(fieldDef : Symbol) : Self = mkString(fieldDef,"","","")

 /**
   * Apply an associative/commutative operation on the left field.
   *
   * == Example ==
   * {{{
   * reduce(('mass,'allids)->('totalMass, 'idset)) { (left:(Double,Set[Long]),right:(Double,Set[Long])) =>
   *   (left._1 + right._1, left._2 ++ right._2)
   * }
   * }}}
   *
   * Equivalent to a mapReduceMap with trivial (identity) map functions.
   *
   * The previous output goes into the reduce function on the left, like foldLeft,
   * so if your operation is faster for the accumulator to be on one side, be aware.
   */
  def reduce[T](fieldDef : (Fields, Fields))(fn : (T,T)=>T)
               (implicit setter : TupleSetter[T], conv : TupleConverter[T]) : Self = {
    mapReduceMap[T,T,T](fieldDef)({ t => t })(fn)({t => t})(conv,setter,conv,setter)
  }
  //Same as reduce(f->f)
  def reduce[T](fieldDef : Symbol*)(fn : (T,T)=>T)(implicit setter : TupleSetter[T],
                                 conv : TupleConverter[T]) : Self = {
    reduce(fieldDef -> fieldDef)(fn)(setter,conv)
  }

  // Abstract algebra reductions (plus, times, dot):

   /**
   * Use `Monoid.plus` to compute a sum.  Not called sum to avoid conflicting with standard sum
   * Your `Monoid[T]` should be associated and commutative, else this doesn't make sense
   */
  def plus[T](fd : (Fields,Fields))
    (implicit monoid : Monoid[T], tconv : TupleConverter[T], tset : TupleSetter[T]) : Self = {
    // We reverse the order because the left is the old value in reduce, and for list concat
    // we are much better off concatenating into the bigger list
    reduce[T](fd)({ (left, right) => monoid.plus(right, left) })(tset, tconv)
  }

  /**
   * The same as `plus(fs -> fs)`
   */
  def plus[T](fs : Symbol*)
    (implicit monoid : Monoid[T], tconv : TupleConverter[T], tset : TupleSetter[T]) : Self = {
    plus[T](fs -> fs)(monoid,tconv,tset)
  }

  /**
   * Returns the product of all the items in this grouping
   */
  def times[T](fd : (Fields,Fields))
    (implicit ring : Ring[T], tconv : TupleConverter[T], tset : TupleSetter[T]) : Self = {
    // We reverse the order because the left is the old value in reduce, and for list concat
    // we are much better off concatenating into the bigger list
    reduce[T](fd)({ (left, right) => ring.times(right, left) })(tset, tconv)
  }

  /**
   * The same as `times(fs -> fs)`
   */
  def times[T](fs : Symbol*)
    (implicit ring : Ring[T], tconv : TupleConverter[T], tset : TupleSetter[T]) : Self = {
    times[T](fs -> fs)(ring,tconv,tset)
  }

  /**
   * Convert a subset of fields into a list of Tuples. Need to provide the types of the tuple fields.
   */
  def toList[T](fieldDef : (Fields, Fields))(implicit conv : TupleConverter[T]) : Self = {
    // TODO(POB) this is jank in my opinion. Nulls should be filter by the user if they want
    mapList[T,List[T]](fieldDef) { _.filter { t => t != null } }
  }


  /**
   * First do "times" on each pair, then "plus" them all together.
   *
   * == Example ==
   * {{{
   * groupBy('x) { _.dot('y,'z, 'ydotz) }
   * }}}
   */
  def dot[T](left : Fields, right : Fields, result : Fields)
    (implicit ttconv : TupleConverter[Tuple2[T,T]], ring : Ring[T],
     tconv : TupleConverter[T], tset : TupleSetter[T]) : Self = {
    mapReduceMap[(T,T),T,T](Fields.merge(left, right) -> result) { init : (T,T) =>
      ring.times(init._1, init._2)
    } { (left : T, right: T) =>
      ring.plus(left, right)
    } { result => result }
  }

  /**
   * How many values are there for this key
   */
   def size : Self = size('size)
  def size(thisF : Fields) : Self = {
    mapPlusMap(() -> thisF) { (u : Unit) => 1L } { s => s }
  }

  def sum(f : (Fields, Fields)) : Self = plus[Double](f)
  def sum(f : Symbol) : Self = sum(f -> f)

  /**
   * Equivalent to sorting by a comparison function
   * then take-ing k items.  This is MUCH more efficient than doing a total sort followed by a take,
   * since these bounded sorts are done on the mapper, so only a sort of size k is needed.
   *
   * == Example ==
   * {{{
   * sortWithTake( ('clicks, 'tweet) -> 'topClicks, 5) {
   *   fn : (t0 :(Long,Long), t1:(Long,Long) => t0._1 < t1._1 }
   * }}}
   *
   * topClicks will be a List[(Long,Long)]
   */
  def sortWithTake[T:TupleConverter](f : (Fields, Fields), k : Int)(lt : (T,T) => Boolean) : Self = {
    assert(f._2.size == 1, "output field size must be 1")
    val mon = new SortedTakeListMonoid[T](k)(new LtOrdering(lt))
    mapReduceMap(f) /* map1 */ { (tup : T) => List(tup) }
    /* reduce */ { (l1 : List[T], l2 : List[T]) =>
      mon.plus(l1, l2)
    } /* map2 */ {
      (lout : List[T]) => lout
    }
  }

  /**
   * Reverse of above when the implicit ordering makes sense.
   */
  def sortedReverseTake[T](f : (Fields, Fields), k : Int)
    (implicit conv : TupleConverter[T], ord : Ordering[T]) : Self = {
    sortWithTake(f,k) { (t0:T,t1:T) => ord.gt(t0,t1) }
  }

  /**
   * Same as above but useful when the implicit ordering makes sense.
   */
  def sortedTake[T](f : (Fields, Fields), k : Int)
    (implicit conv : TupleConverter[T], ord : Ordering[T]) : Self = {
    sortWithTake(f,k) { (t0:T,t1:T) => ord.lt(t0,t1) }
  }

  def histogram(f : (Fields, Fields),  binWidth : Double = 1.0) = {
      mapPlusMap(f)
        {x : Double => Map((math.floor(x / binWidth) * binWidth) -> 1)}
        {map => new mathematics.Histogram(map, binWidth)}
  }
}
