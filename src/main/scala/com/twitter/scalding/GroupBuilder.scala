/*
Copyright 2012 Twitter, Inc.

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

import cascading.pipe._
import cascading.pipe.assembly._
import cascading.operation._
import cascading.operation.aggregator._
import cascading.operation.filter._
import cascading.tuple.Fields
import cascading.tuple.{Tuple => CTuple, TupleEntry}

import scala.collection.JavaConverters._
import scala.annotation.tailrec
import scala.math.Ordering

/**
 * This controls the sequence of reductions that happen inside a
 * particular grouping operation.  Not all elements can be combined,
 * for instance, a scanLeft/foldLeft generally requires a sorting
 * but such sorts are (at least for now) incompatible with doing a combine
 * which includes some map-side reductions.
 */
class GroupBuilder(val groupFields : Fields) extends
  FoldOperations[GroupBuilder] with
  StreamOperations[GroupBuilder] {
  // We need the implicit conversions from symbols to Fields
  import Dsl._

  /**
  * Holds the "reducers/combiners", the things that we can do paritially map-side.
  */
  private var reds : Option[List[AggregateBy]] = Some(Nil)

  /**
   * This is the description of this Grouping in terms of a sequence of Every operations
   */
  protected var evs : List[Pipe => Every] = Nil
  protected var isReversed : Boolean = false

  protected var sortF : Option[Fields] = None
  def sorting = sortF
  /*
  * maxMF is the maximum index of a "middle field" allocated for mapReduceMap operations
  */
  private var maxMF : Int = 0

  private def getNextMiddlefield : String = {
    val out = "__middlefield__" + maxMF.toString
    maxMF += 1
    return out
  }

  private def tryAggregateBy(ab : AggregateBy, ev : Pipe => Every) : Boolean = {
    // Concat if there if not none
    reds = reds.map(rl => ab::rl)
    evs = ev :: evs
    return !reds.isEmpty
  }

  /**
   * Holds the number of reducers to use in the reduce stage of the groupBy/aggregateBy.
   * By default uses whatever value is set in the jobConf.
   */
  private var numReducers : Option[Int] = None

  /**
  * Limit of number of keys held in SpillableTupleMap on an AggregateBy
  */
  private var spillThreshold = 100000 //tune this, default is 10k

  /**
   * Override the number of reducers used in the groupBy.
   */
  def reducers(r : Int) = {
    if(r > 0) {
      numReducers = Some(r)
    }
    this
  }

  /**
   * Override the spill threshold on AggregateBy
   */
  def spillThreshold(t : Int) : GroupBuilder = {
    spillThreshold = t
    this
  }

  /**
   * This cancels map side aggregation
   * and forces everything to the reducers
   */
  def forceToReducers = {
    reds = None
    this
  }

  protected def overrideReducers(p : Pipe) : Pipe = {
    numReducers.map { r => RichPipe.setReducers(p, r) }.getOrElse(p)
  }

  /**
   * == Warning ==
   * This may significantly reduce performance of your job.
   * It kills the ability to do map-side aggregation.
   */
  def buffer(args : Fields)(b : Buffer[_]) : GroupBuilder = {
    every(pipe => new Every(pipe, args, b))
  }


  /**
  * By default adds a column with name "count" counting the number in
  * this group. deprecated, use size.
  */
  @deprecated("Use size instead to match the scala.collections.Iterable API", "0.2.0")
  def count(f : Symbol = 'count) : GroupBuilder = size(f)

  /**
   * Prefer aggregateBy operations!
   */
  def every(ev : Pipe => Every) : GroupBuilder = {
    reds = None
    evs = ev :: evs
    this
  }

  /**
   * Prefer reduce or mapReduceMap. foldLeft will force all work to be
   * done on the reducers.  If your function is not associative and
   * commutative, foldLeft may be required.
   *
   * == Best Practice ==
   * Make sure init is an immutable object.
   *
   * == Note ==
   * Init needs to be serializable with Kryo (because we copy it for each
   * grouping to avoid possible errors using a mutable init object).
   */
  def foldLeft[X,T](fieldDef : (Fields,Fields))(init : X)(fn : (X,T) => X)
                 (implicit setter : TupleSetter[X], conv : TupleConverter[T]) : GroupBuilder = {
      val (inFields, outFields) = fieldDef
      conv.assertArityMatches(inFields)
      setter.assertArityMatches(outFields)
      val ag = new FoldAggregator[T,X](fn, init, outFields, conv, setter)
      every(pipe => new Every(pipe, inFields, ag))
  }


  /**
   * Type `T` is the type of the input field `(input to map, T => X)`
   *
   * Type `X` is the intermediate type, which your reduce function operates on
   * `(reduce is (X,X) => X)`
   *
   * Type `U` is the final result type, `(final map is: X => U)`
   *
   * The previous output goes into the reduce function on the left, like foldLeft,
   * so if your operation is faster for the accumulator to be on one side, be aware.
   */
  def mapReduceMap[T,X,U](fieldDef : (Fields, Fields))(mapfn : T => X )(redfn : (X, X) => X)
      (mapfn2 : X => U)(implicit startConv : TupleConverter[T],
                        middleSetter : TupleSetter[X],
                        middleConv : TupleConverter[X],
                        endSetter : TupleSetter[U]) : GroupBuilder = {
    val (fromFields, toFields) = fieldDef
    //Check for arity safety:
    startConv.assertArityMatches(fromFields)
    endSetter.assertArityMatches(toFields)

    val ag = new MRMAggregator[T,X,U](mapfn, redfn, mapfn2, toFields, startConv, endSetter)
    val ev = (pipe => new Every(pipe, fromFields, ag)) : Pipe => Every
    assert(middleSetter.arity > 0,
      "The middle arity must have definite size, try wrapping in scala.Tuple1 if you need a hack")
    // Create the required number of middlefields based on the arity of middleSetter
    val middleFields = strFields( Range(0, middleSetter.arity).map{i => getNextMiddlefield} )
    val mrmBy = new MRMBy[T,X,U](fromFields, middleFields, toFields,
      mapfn, redfn, mapfn2, startConv, middleSetter, middleConv, endSetter)
    tryAggregateBy(mrmBy, ev)
    this
  }

  /**
   * Corresponds to a Cascading Buffer
   * which allows you to stream through the data, keeping some, dropping, scanning, etc...
   * The iterator you are passed is lazy, and mapping will not trigger the
   * entire evaluation.  If you convert to a list (i.e. to reverse), you need to be aware
   * that memory constraints may become an issue.
   *
   * == Warning ==
   * Any fields not referenced by the input fields will be aligned to the first output,
   * and the final hadoop stream will have a length of the maximum of the output of this, and
   * the input stream.  So, if you change the length of your inputs, the other fields won't
   * be aligned.  YOU NEED TO INCLUDE ALL THE FIELDS YOU WANT TO KEEP ALIGNED IN THIS MAPPING!
   * POB: This appears to be a Cascading design decision.
   *
   * == Warning ==
   * mapfn needs to be stateless.  Multiple calls needs to be safe (no mutable
   * state captured)
   */
  def mapStream[T,X](fieldDef : (Fields,Fields))(mapfn : (Iterator[T]) => TraversableOnce[X])
    (implicit conv : TupleConverter[T], setter : TupleSetter[X]) = {
    val (inFields, outFields) = fieldDef
    //Check arity
    conv.assertArityMatches(inFields)
    setter.assertArityMatches(outFields)
    val b = new BufferOp[Unit,T,X]((),
      (u : Unit, it: Iterator[T]) => mapfn(it), outFields, conv, setter)
    every(pipe => new Every(pipe, inFields, b, defaultMode(inFields, outFields)))
  }


  def reverse : GroupBuilder = {
    assert(reds.isEmpty, "Cannot sort when reducing")
    assert(!isReversed, "Reverse called a second time! Only one allowed")
    isReversed = true
    this
  }

  /**
   * Analog of standard scanLeft (@see scala.collection.Iterable.scanLeft )
   * This invalidates map-side aggregation, forces all data to be transferred
   * to reducers.  Use only if you REALLY have to.
   *
   * == Best Practice ==
   * Make sure init is an immutable object.
   *
   * == Note ==
   * init needs to be serializable with Kryo (because we copy it for each
   * grouping to avoid possible errors using a mutable init object).
   *  We override the default implementation here to use Kryo to serialize
   *  the initial value, for immutable serializable inits, this is not needed
   */
  override def scanLeft[X,T](fieldDef : (Fields,Fields))(init : X)(fn : (X,T) => X)
                 (implicit setter : TupleSetter[X], conv : TupleConverter[T]) : GroupBuilder = {
    val (inFields, outFields) = fieldDef
    //Check arity
    conv.assertArityMatches(inFields)
    setter.assertArityMatches(outFields)
    val b = new BufferOp[X,T,X](init,
      // On scala 2.8, there is no scanLeft
      // On scala 2.9, their implementation creates an off-by-one bug with the unused fields
      (i : X, it: Iterator[T]) => new ScanLeftIterator(it, i, fn),
      outFields, conv, setter)
    every(pipe => new Every(pipe, inFields, b, defaultMode(inFields, outFields)))
  }

  def groupMode : GroupMode = {
    return reds match {
      case None => GroupByMode
      case Some(Nil) => IdentityMode
      case Some(redList) => AggregateByMode
    }
  }

  def schedule(name : String, pipe : Pipe) : Pipe = {

    groupMode match {
      //In this case we cannot aggregate, so group:
      case GroupByMode => {
        val startPipe : Pipe = sortF match {
          case None => new GroupBy(name, pipe, groupFields)
          case Some(sf) => new GroupBy(name, pipe, groupFields, sf, isReversed)
        }
        overrideReducers(startPipe)

        // Time to schedule the addEverys:
        evs.foldRight(startPipe)( (op : Pipe => Every, p) => op(p) )
      }
      //This is the case where the group function is identity: { g => g }
      case IdentityMode => {
        val gb = new GroupBy(name, pipe, groupFields)
        overrideReducers(gb)
        gb
      }
      //There is some non-empty AggregateBy to do:
      case AggregateByMode => {
        val redlist = reds.get
        val ag = new AggregateBy(name, pipe, groupFields,
          spillThreshold, redlist.reverse.toArray : _*)
        overrideReducers(ag.getGroupBy())
        ag
      }
    }
  }

  /**
   * This invalidates aggregateBy!
   */
  def sortBy(f : Fields) : GroupBuilder = {
    reds = None
    sortF = sortF match {
      case None => Some(f)
      case Some(sf) => {
        sf.append(f)
        Some(sf)
      }
    }
    this
  }

  /**
   * This is convenience method to allow plugging in blocks
   * of group operations similar to `RichPipe.then`
   */
  def then(fn : (GroupBuilder) => GroupBuilder) = fn(this)
}

/**
 * Scala 2.8 Iterators don't support scanLeft so we have to reimplement
 */
class ScanLeftIterator[T,U](it : Iterator[T], init : U, fn : (U,T) => U) extends Iterator[U] with java.io.Serializable {
  protected var prev : Option[U] = None
  def hasNext : Boolean = { prev.isEmpty || it.hasNext }
  def next = {
    prev = prev.map { fn(_, it.next) }
            .orElse(Some(init))
    prev.get
  }
}

sealed private[scalding] abstract class GroupMode
private[scalding] case object AggregateByMode extends GroupMode
private[scalding] case object GroupByMode extends GroupMode
private[scalding] case object IdentityMode extends GroupMode
