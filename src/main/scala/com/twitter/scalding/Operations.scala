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
package com.twitter.scalding {

import cascading.operation._
import cascading.tuple._
import cascading.flow._
import cascading.pipe.assembly.AggregateBy
import cascading.pipe._

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration

import com.esotericsoftware.kryo.Kryo;

object CascadingUtils {
  def flowProcessToConfiguration(fp : FlowProcess[_]) : Configuration = {
    val confCopy = fp.asInstanceOf[FlowProcess[AnyRef]].getConfigCopy
    if (confCopy.isInstanceOf[Configuration]) {
      confCopy.asInstanceOf[Configuration]
    }
    else {
      // For local mode, we don't have a hadoop configuration
      val conf = new Configuration()
      fp.getPropertyKeys.asScala.foreach { key =>
        conf.set(key, fp.getStringProperty(key))
      }
      conf
    }
  }
  def kryoFor(fp : FlowProcess[_]) : Kryo = {
    (new cascading.kryo.KryoSerialization(flowProcessToConfiguration(fp)))
      .populatedKryo
  }
}

import CascadingUtils.kryoFor

  class FlatMapFunction[S,T](fn : S => Iterable[T], fields : Fields,
    conv : TupleConverter[S], set : TupleSetter[T])
    extends BaseOperation[Any](fields) with Function[Any] {

    def operate(flowProcess : FlowProcess[_], functionCall : FunctionCall[Any]) {
      fn(conv(functionCall.getArguments)).foreach { arg : T =>
        val this_tup = set(arg)
        functionCall.getOutputCollector.add(this_tup)
      }
    }
  }

  class MapFunction[S,T](fn : S => T, fields : Fields,
    conv : TupleConverter[S], set : TupleSetter[T])
    extends BaseOperation[Any](fields) with Function[Any] {

    def operate(flowProcess : FlowProcess[_], functionCall : FunctionCall[Any]) {
      val res = fn(conv(functionCall.getArguments))
      functionCall.getOutputCollector.add(set(res))
    }
  }

  /*
   * A map function that allows state object to be set up and tear down.
   */
  class SideEffectMapFunction[S, C, T] (
    bf: => C,                // begin function returns a context
    fn: (C, S) => T,         // function that takes a context and a tuple and generate a new tuple
    ef: C => Unit,           // end function to clean up context object
    fields: Fields,
    conv: TupleConverter[S],
    set: TupleSetter[T]
  ) extends BaseOperation[C](fields) with Function[C] {

    override def prepare(flowProcess: FlowProcess[_], operationCall: OperationCall[C]) {
      operationCall.setContext(bf)
    }

    override def operate(flowProcess: FlowProcess[_], functionCall: FunctionCall[C]) {
      val context = functionCall.getContext
      val s = conv(functionCall.getArguments)
      val res = fn(context, s)
      functionCall.getOutputCollector.add(set(res))
    }

    override def cleanup(flowProcess: FlowProcess[_], operationCall: OperationCall[C]) {
      ef(operationCall.getContext)
    }
  }

  /*
   * A flatmap function that allows state object to be set up and tear down.
   */
  class SideEffectFlatMapFunction[S, C, T] (
    bf: => C,                  // begin function returns a context
    fn: (C, S) => Iterable[T], // function that takes a context and a tuple, returns iterable of T
    ef: C => Unit,             // end function to clean up context object
    fields: Fields,
    conv: TupleConverter[S],
    set: TupleSetter[T]
  ) extends BaseOperation[C](fields) with Function[C] {

    override def prepare(flowProcess: FlowProcess[_], operationCall: OperationCall[C]) {
      operationCall.setContext(bf)
    }

    override def operate(flowProcess: FlowProcess[_], functionCall: FunctionCall[C]) {
      val context = functionCall.getContext
      val s = conv(functionCall.getArguments)
      fn(context, s) foreach { t => functionCall.getOutputCollector.add(set(t)) }
    }

    override def cleanup(flowProcess: FlowProcess[_], operationCall: OperationCall[C]) {
      ef(operationCall.getContext)
    }
  }

  class FilterFunction[T](fn : T => Boolean, conv : TupleConverter[T]) extends BaseOperation[Any] with Filter[Any] {
    def isRemove(flowProcess : FlowProcess[_], filterCall : FilterCall[Any]) = {
      !fn(conv(filterCall.getArguments))
    }
  }

  // All the following are operations for use in GroupBuilder

  class FoldAggregator[T,X](fn : (X,T) => X, init : X, fields : Fields,
    conv : TupleConverter[T], set : TupleSetter[X])
    extends BaseOperation[X](fields) with Aggregator[X] {

    def start(flowProcess : FlowProcess[_], call : AggregatorCall[X]) {
      val deepCopyInit = kryoFor(flowProcess).copy(init)
      call.setContext(deepCopyInit)
    }

    def aggregate(flowProcess : FlowProcess[_], call : AggregatorCall[X]) {
      val left = call.getContext
      val right = conv(call.getArguments)
      call.setContext(fn(left, right))
    }

    def complete(flowProcess : FlowProcess[_], call : AggregatorCall[X]) {
      emit(flowProcess, call)
    }

    def emit(flowProcess : FlowProcess[_], call : AggregatorCall[X]) {
      call.getOutputCollector.add(set(call.getContext))
    }
  }

  /*
   * fields are the declared fields of this aggregator
   */
  class MRMAggregator[T,X,U](fsmf : T => X, rfn : (X,X) => X, mrfn : X => U, fields : Fields,
    conv : TupleConverter[T], set : TupleSetter[U])
    extends BaseOperation[Tuple](fields) with Aggregator[Tuple] {
    // The context is a singleton Tuple, which is mutable so
    // we don't have to allocate at every step of the loop:
    def start(flowProcess : FlowProcess[_], call : AggregatorCall[Tuple]) {
        call.setContext(null)
    }

    def extractArgument(call : AggregatorCall[Tuple]) : X = fsmf(conv(call.getArguments))

    def aggregate(flowProcess : FlowProcess[_], call : AggregatorCall[Tuple]) {
      val arg = extractArgument(call)
      val ctx = call.getContext
      if (null == ctx) {
        // Initialize the context, this is the only allocation done by this loop.
        val newCtx = Tuple.size(1)
        newCtx.set(0, arg.asInstanceOf[AnyRef])
        call.setContext(newCtx)
      }
      else {
        // Mutate the context:
        val oldValue = ctx.getObject(0).asInstanceOf[X]
        val newValue = rfn(oldValue, arg)
        ctx.set(0, newValue.asInstanceOf[AnyRef])
      }
    }

    def complete(flowProcess : FlowProcess[_], call : AggregatorCall[Tuple]) {
      val ctx = call.getContext
      if (null != ctx) {
        val lastValue = ctx.getObject(0).asInstanceOf[X]
        // Make sure to drop the reference to the lastValue as soon as possible (it may be big)
        call.setContext(null)
        call.getOutputCollector.add(set(mrfn(lastValue)))
      }
      else {
        throw new Exception("MRMAggregator completed without any args")
      }
    }
  }

  /**
   * This handles the mapReduceMap work on the map-side of the operation.  The code below
   * attempts to be optimal with respect to memory allocations and performance, not functional
   * style purity.
   */
  abstract class FoldFunctor[X](fields : Fields) extends AggregateBy.Functor {

    // Extend these three methods:
    def first(args : TupleEntry) : X
    def subsequent(oldValue : X, newArgs : TupleEntry) : X
    def finish(lastValue : X) : Tuple

    override final def getDeclaredFields = fields

    /*
     * It's important to keep all state in the context as Cascading seems to
     * reuse these objects, so any per instance state might give unexpected
     * results.
     */
    override final def aggregate(flowProcess : FlowProcess[_], args : TupleEntry, context : Tuple) = {
      var nextContext : Tuple = null
      val newContextObj = if (null == context) {
        // First call, make a new mutable tuple to reduce allocations:
        nextContext = Tuple.size(1)
        first(args)
      }
      else {
        //We are updating
        val oldValue = context.getObject(0).asInstanceOf[X]
        nextContext = context
        subsequent(oldValue, args)
      }
      nextContext.set(0, newContextObj.asInstanceOf[AnyRef])
      //Return context for reuse next time:
      nextContext
    }

    override final def complete(flowProcess : FlowProcess[_], context : Tuple) = {
      if (null == context) {
        throw new Exception("FoldFunctor completed with any aggregate calls")
      }
      else {
        val res = context.getObject(0).asInstanceOf[X]
        // Make sure we remove the ref to the context ASAP:
        context.set(0, null)
        finish(res)
      }
    }
  }

  /**
   * This handles the mapReduceMap work on the map-side of the operation.  The code below
   * attempts to be optimal with respect to memory allocations and performance, not functional
   * style purity.
   */
  class MRMFunctor[T,X](mrfn : T => X, rfn : (X, X) => X, fields : Fields,
    conv : TupleConverter[T], set : TupleSetter[X])
    extends FoldFunctor[X](fields) {

    override def first(args : TupleEntry) : X = mrfn(conv(args))
    override def subsequent(oldValue : X, newArgs : TupleEntry) = {
      val right = mrfn(conv(newArgs))
      rfn(oldValue, right)
    }
    override def finish(lastValue : X) = set(lastValue)
  }

  /**
   * MapReduceMapBy Class
   */
  class MRMBy[T,X,U](arguments : Fields,
                   middleFields : Fields,
                   declaredFields : Fields,
                   mfn : T => X,
                   rfn : (X,X) => X,
                   mfn2 : X => U,
                   startConv : TupleConverter[T],
                   midSet : TupleSetter[X],
                   midConv : TupleConverter[X],
                   endSet : TupleSetter[U]) extends AggregateBy(
        arguments,
        new MRMFunctor[T,X](mfn, rfn, middleFields, startConv, midSet),
        new MRMAggregator[X,X,U](args => args, rfn, mfn2, declaredFields, midConv, endSet))

  class BufferOp[I,T,X](init : I, iterfn : (I, Iterator[T]) => TraversableOnce[X], fields : Fields,
    conv : TupleConverter[T], set : TupleSetter[X])
    extends BaseOperation[Any](fields) with Buffer[Any] {

    def operate(flowProcess : FlowProcess[_], call : BufferCall[Any]) {
      val deepCopyInit = kryoFor(flowProcess).copy(init)
      val oc = call.getOutputCollector
      val in = call.getArgumentsIterator.asScala.map { entry => conv(entry) }
      iterfn(deepCopyInit, in).foreach { x => oc.add(set(x)) }
    }
  }
}
