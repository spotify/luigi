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

import cascading.tap._
import cascading.scheme._
import cascading.pipe._
import cascading.pipe.assembly._
import cascading.pipe.joiner._
import cascading.flow._
import cascading.operation._
import cascading.operation.aggregator._
import cascading.operation.filter._
import cascading.tuple._
import cascading.cascade._

object RichPipe extends java.io.Serializable {
  private var nextPipe = -1

  def apply(p : Pipe) = new RichPipe(p)

  def getNextName = {
    nextPipe = nextPipe + 1
    "_pipe_" + nextPipe.toString
  }

  def assignName(p : Pipe) = new Pipe(getNextName, p)

  private val REDUCER_KEY = "mapred.reduce.tasks"
  /**
   * Gets the underlying config for this pipe and sets the number of reducers
   * useful for cascading GroupBy/CoGroup pipes.
   */
  def setReducers(p : Pipe, reducers : Int) : Pipe = {
    if(reducers > 0) {
      p.getStepConfigDef()
        .setProperty(REDUCER_KEY, reducers.toString)
    } else if(reducers != -1) {
      throw new IllegalArgumentException("Number of reducers must be non-negative")
    }
    p
  }
}

class RichPipe(val pipe : Pipe) extends java.io.Serializable with JoinAlgorithms {
  // We need this for the implicits
  import Dsl._
  import RichPipe.assignName

  /**
   * A simple trait for releasable resource. Provides noop implementation.
   */
  trait Stateful {
    def release() {}
  }

  /**
   * Rename the current pipe
   */
  def name(s : String) = new Pipe(s, pipe)

  /**
   * begining of block with access to expensive nonserializable state. The state object should
   * contain a function release() for resource management purpose.
   */
  def using[A, C <: { def release() }](bf: => C) = new {

    /**
     * For pure side effect.
     */
    def foreach(f: Fields)(fn: (C, A) => Unit)
            (implicit conv: TupleConverter[A], set: TupleSetter[Unit], flowDef: FlowDef, mode: Mode) = {
      conv.assertArityMatches(f)
      val newPipe = new Each(pipe, f, new SideEffectMapFunction(bf, fn,
        new Function1[C, Unit] with java.io.Serializable {
          def apply(c: C) { c.release() }
        },
        Fields.NONE, conv, set))
      NullSource.write(newPipe)(flowDef, mode)
      newPipe
    }

    /**
     * map with state
     */
    def map[A,T](fs: (Fields,Fields))(fn: (C, A) => T)
                (implicit conv: TupleConverter[A], set: TupleSetter[T]) = {
      conv.assertArityMatches(fs._1)
      set.assertArityMatches(fs._2)
      val mf = new SideEffectMapFunction(bf, fn,
        new Function1[C, Unit] with java.io.Serializable {
          def apply(c: C) { c.release() }
        },
        fs._2, conv, set)
      new Each(pipe, fs._1, mf, defaultMode(fs._1, fs._2))
    }

    /**
     * flatMap with state
     */
    def flatMap[A,T](fs: (Fields,Fields))(fn: (C, A) => Iterable[T])
                (implicit conv: TupleConverter[A], set: TupleSetter[T]) = {
      conv.assertArityMatches(fs._1)
      set.assertArityMatches(fs._2)
      val mf = new SideEffectFlatMapFunction(bf, fn,
        new Function1[C, Unit] with java.io.Serializable {
          def apply(c: C) { c.release() }
        },
        fs._2, conv, set)
      new Each(pipe, fs._1, mf, defaultMode(fs._1, fs._2))
    }
  }

  /**
   * Keep only the given fields, and discard the rest.
   * takes any number of parameters as long as we can convert
   * them to a fields object
   */
  def project(fields : Fields) = {
    new Each(pipe, fields, new Identity(fields))
  }

  /**
   * Discard the given fields, and keep the rest
   * Kind of the opposite previous.
   */
  def discard(f : Fields) = new Each(pipe, f, new NoOp, Fields.SWAP)

  /**
   * Insert a function into the pipeline:
   */
  def then[T,U](pfn : (T) => U)(implicit in : (RichPipe)=>T, out : (U)=>Pipe) = out(pfn(in(this)))

  /**
   * group
   *
   * builder is typically a block that modifies the given GroupBuilder
   * the final OUTPUT of the block is used to schedule the new pipe
   * each method in GroupBuilder returns this, so it is recommended
   * to chain them and use the default input:
   *
   * {{{
   *   _.size.max('f1) etc...
   * }}}
   */
  def groupBy(f : Fields)(builder : GroupBuilder => GroupBuilder) : Pipe = {
    builder(new GroupBuilder(f)).schedule(pipe.getName, pipe)
  }

  /**
   * Returns the set of unique tuples containing the specified fields
   */
  def unique(f : Fields) : Pipe = groupBy(f) { _.size('__uniquecount__) }.project(f)

  /**
   * Merge or Concatenate several pipes together with this one:
   */
  def ++(that : Pipe) = new Merge(assignName(this.pipe), assignName(that))

  /**
   * Group all tuples down to one reducer.
   * (due to cascading limitation).
   * This is probably only useful just before setting a tail such as Database
   * tail, so that only one reducer talks to the DB.  Kind of a hack.
   */
  def groupAll : Pipe = groupAll { g =>
    g.takeWhile(0)((t : TupleEntry) => true)
  }

  /**
   * == Warning ==
   * This kills parallelism.  All the work is sent to one reducer.
   *
   * Only use this in the case that you truly need all the data on one
   * reducer.
   *
   * Just about the only reasonable case of this data is to reduce all values of a column
   * or count all the rows.
   */
  def groupAll(gs : GroupBuilder => GroupBuilder) = {
    map(()->'__groupAll__) { (u:Unit) => 1 }
    .groupBy('__groupAll__) { gs(_).reducers(1) }
    .discard('__groupAll__)
  }

  /**
   * Rename some set of N fields as another set of N fields
   *
   * == Usage ==
   * {{{
   * rename('x -> 'z)
   *        rename(('x,'y) -> ('X,'Y))
   * }}}
   *
   * == Warning ==
   * `rename('x,'y)` is interpreted by scala as `rename(Tuple2('x,'y))`
   * which then does `rename('x -> 'y)`.  This is probably not what is intended
   * but the compiler doesn't resolve the ambiguity.  YOU MUST CALL THIS WITH
   * A TUPLE2!  If you don't, expect the unexpected.
   */
  def rename(fields : (Fields,Fields)) : Pipe = {
    val (fromFields, toFields) = fields
    val in_arity = fromFields.size
    val out_arity = toFields.size
    assert(in_arity == out_arity, "Number of field names must match for rename")
    new Each(pipe, fromFields, new Identity( toFields ), Fields.SWAP)
  }

  def filter[A](f : Fields)(fn : (A) => Boolean)
      (implicit conv : TupleConverter[A]) : Pipe = {
    conv.assertArityMatches(f)
    new Each(pipe, f, new FilterFunction(fn, conv))
  }

  /**
   * If you use a map function that does not accept TupleEntry args,
   * which is the common case, an implicit conversion in GeneratedConversions
   * will convert your function into a `(TupleEntry => T)`.  The result type
   * T is converted to a cascading Tuple by an implicit `TupleSetter[T]`.
   * acceptable T types are primitive types, cascading Tuples of those types,
   * or `scala.Tuple(1-22)` of those types.
   *
   * After the map, the input arguments will be set to the output of the map,
   * so following with filter or map is fine without a new using statement if
   * you mean to operate on the output.
   *
   * {{{
   * map('data -> 'stuff)
   * }}}
   *
   * * if output equals input, REPLACE is used.
   * * if output or input is a subset of the other SWAP is used.
   * * otherwise we append the new fields (cascading Fields.ALL is used)
   *
   * {{{
   * mapTo('data -> 'stuff)
   * }}}
   *
   *  Only the results (stuff) are kept (cascading Fields.RESULTS)
   *
   * == Note ==
   * Using mapTo is the same as using map followed by a project for
   * selecting just the ouput fields
   */
  def map[A,T](fs : (Fields,Fields))(fn : A => T)
                (implicit conv : TupleConverter[A], setter : TupleSetter[T]) : Pipe = {
      conv.assertArityMatches(fs._1)
      setter.assertArityMatches(fs._2)
      each(fs)(new MapFunction[A,T](fn, _, conv, setter))
  }
  def mapTo[A,T](fs : (Fields,Fields))(fn : A => T)
                (implicit conv : TupleConverter[A], setter : TupleSetter[T]) : Pipe = {
      conv.assertArityMatches(fs._1)
      setter.assertArityMatches(fs._2)
      eachTo(fs)(new MapFunction[A,T](fn, _, conv, setter))
  }
  def flatMap[A,T](fs : (Fields,Fields))(fn : A => Iterable[T])
                (implicit conv : TupleConverter[A], setter : TupleSetter[T]) : Pipe = {
      conv.assertArityMatches(fs._1)
      setter.assertArityMatches(fs._2)
      each(fs)(new FlatMapFunction[A,T](fn, _, conv, setter))
  }
  def flatMapTo[A,T](fs : (Fields,Fields))(fn : A => Iterable[T])
                (implicit conv : TupleConverter[A], setter : TupleSetter[T]) : Pipe = {
      conv.assertArityMatches(fs._1)
      setter.assertArityMatches(fs._2)
      eachTo(fs)(new FlatMapFunction[A,T](fn, _, conv, setter))
  }

  /**
   * the same as
   * {{{
   * flatMap(fs) { it : Iterable[T] => it }
   * }}}
   * Common enough to be useful.
   */
  def flatten[T](fs : (Fields, Fields))
    (implicit conv : TupleConverter[Iterable[T]], setter : TupleSetter[T]) : Pipe = {
    flatMap[Iterable[T],T](fs)({ it : Iterable[T] => it })(conv, setter)
  }

  /**
   * Force a materialization to disk in the flow.
   * This is useful before crossWithTiny if you filter just before. Ideally scalding/cascading would
   * see this (and may in future versions), but for now it is here to aid in hand-tuning jobs
   */
  lazy val forceToDisk: Pipe = new Checkpoint(pipe)

  /**
   * Convenience method for integrating with existing cascading Functions
   */
  def each(fs : (Fields,Fields))(fn : Fields => Function[_]) = {
    new Each(pipe, fs._1, fn(fs._2), defaultMode(fs._1, fs._2))
  }

  /**
   * Same as above, but only keep the results field.
   */
  def eachTo(fs : (Fields,Fields))(fn : Fields => Function[_]) = {
    new Each(pipe, fs._1, fn(fs._2), Fields.RESULTS)
  }

  /**
   * This is an analog of the SQL/Excel unpivot function which converts columns of data
   * into rows of data.  Only the columns given as input fields are expanded in this way.
   * For this operation to be reversible, you need to keep some unique key on each row.
   * See GroupBuilder.pivot to reverse this operation assuming you leave behind a grouping key
   * == Example ==
   * {{{
   * pipe.unpivot(('w,'x,'y,'z) -> ('feature, 'value))
   * }}}
   *
   * takes rows like:
   * {{{
   * key, w, x, y, z
   * 1, 2, 3, 4, 5
   * 2, 8, 7, 6, 5
   * }}}
   * to:
   * {{{
   * key, feature, value
   * 1, w, 2
   * 1, x, 3
   * 1, y, 4
   * }}}
   * etc...
   */
  def unpivot(fieldDef : (Fields,Fields)) : Pipe = {
    assert(fieldDef._2.size == 2, "Must specify exactly two Field names for the results")
    // toKeyValueList comes from TupleConversions
    pipe.flatMap(fieldDef)(toKeyValueList)
      .discard(fieldDef._1)
  }

  /**
   * Keep at most n elements.  This is implemented by keeping
   * approximately n/k elements on each of the k mappers or reducers (whichever we wind
   * up being scheduled on).
   */
  def limit(n : Long) = new Each(pipe, new Limit(n))

  def debug = new Each(pipe, new Debug())

  def write(outsource : Source)(implicit flowDef : FlowDef, mode : Mode) = {
    outsource.write(pipe)(flowDef, mode)
    pipe
  }

  /**
   * in some cases, crossWithTiny has been broken, this gives a work-around
   */
  def normalize(f : Fields, useTiny : Boolean = true) : Pipe = {
    val total = groupAll { _.sum(f -> 'total_for_normalize) }
    (if(useTiny) {
      crossWithTiny(total)
    } else {
      crossWithSmaller(total)
    })
    .map(Fields.merge(f, 'total_for_normalize) -> f) { args : (Double, Double) =>
      args._1 / args._2
    }
  }

  /** Maps the input fields into an output field of type T. For example:
   *
   * {{{
   *   pipe.pack[(Int, Int)] (('field1, 'field2) -> 'field3)
   * }}}
   *
   * will pack fields 'field1 and 'field2 to field 'field3, as long as 'field1 and 'field2
   * can be cast into integers. The output field 'field3 will be of tupel `(Int, Int)`
   *
   */
  def pack[T](fs : (Fields, Fields))(implicit packer : TuplePacker[T], setter : TupleSetter[T]) : Pipe = {
    val (fromFields, toFields) = fs
    assert(toFields.size == 1, "Can only output 1 field in pack")
    val conv = packer.newConverter(fromFields)
    pipe.map(fs) { input : T => input } (conv, setter)
  }

  /**
   * Same as pack but only the to fields are preserved.
   */
  def packTo[T](fs : (Fields, Fields))(implicit packer : TuplePacker[T], setter : TupleSetter[T]) : Pipe = {
    val (fromFields, toFields) = fs
    assert(toFields.size == 1, "Can only output 1 field in pack")
    val conv = packer.newConverter(fromFields)
    pipe.mapTo(fs) { input : T => input } (conv, setter)
  }

  /**
   * The opposite of pack. Unpacks the input field of type `T` into
   * the output fields. For example:
   *
   * {{{
   *   pipe.unpack[(Int, Int)] ('field1 -> ('field2, 'field3))
   * }}}
   *
   * will unpack 'field1 into 'field2 and 'field3
   */
  def unpack[T](fs : (Fields, Fields))(implicit unpacker : TupleUnpacker[T], conv : TupleConverter[T]) : Pipe = {
    val (fromFields, toFields) = fs
    assert(fromFields.size == 1, "Can only take 1 input field in unpack")
    val setter = unpacker.newSetter(toFields)
    pipe.map(fs) { input : T => input } (conv, setter)
  }

  /**
   * Same as unpack but only the to fields are preserved.
   */
  def unpackTo[T](fs : (Fields, Fields))(implicit unpacker : TupleUnpacker[T], conv : TupleConverter[T]) : Pipe = {
    val (fromFields, toFields) = fs
    assert(fromFields.size == 1, "Can only take 1 input field in unpack")
    val setter = unpacker.newSetter(toFields)
    pipe.mapTo(fs) { input : T => input } (conv, setter)
  }
}
