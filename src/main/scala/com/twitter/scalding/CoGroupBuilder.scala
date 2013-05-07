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

import cascading.pipe.{CoGroup, Every, Pipe}
import cascading.pipe.joiner.MixedJoin
import cascading.tuple.Fields

/**
 * Builder classes used internally to implement coGroups (joins).
 * Can also be used for more generalized joins, e.g., star joins.
 *
 */
class CoGroupBuilder(groupFields : Fields, joinMode : JoinMode) extends GroupBuilder(groupFields) {
  protected var coGroups : List[(Fields, Pipe, JoinMode)] = Nil

  // Joins (cogroups) with pipe p on fields f.
  // Make sure that pipe p is smaller than the left side pipe, otherwise this
  // might take a while.
  def coGroup(f : Fields, p : Pipe, j : JoinMode = InnerJoinMode) = {
    coGroups ::= (f, RichPipe.assignName(p), j)
    this
  }

  // TODO: move the automatic renaming of fields here
  // and remove it from joinWithSmaller/joinWithTiny
  override def schedule(name : String, pipe : Pipe) : Pipe = {
    assert(!sorting.isDefined, "cannot use a sortBy when doing a coGroup")
    assert(!coGroups.isEmpty, "coGroupBy requires at least one other pipe to .coGroup")
    val fields = (groupFields :: coGroups.map{ _._1 }).toArray
    val pipes = (pipe :: coGroups.map{ _._2 }).map{ RichPipe.assignName(_) }.toArray
    val joinModes = (joinMode :: coGroups.map{ _._3 }).map{ _.booleanValue }.toArray
    val mixedJoiner = new MixedJoin(joinModes)
    val cg : Pipe = new CoGroup(pipes, fields, null, mixedJoiner)
    overrideReducers(cg)
    evs.foldRight(cg)( (op : Pipe => Every, p) => op(p) )
  }
}

sealed abstract class JoinMode {
  def booleanValue : Boolean
}
case object InnerJoinMode extends JoinMode {
  override def booleanValue = true
}
case object OuterJoinMode extends JoinMode {
  override def booleanValue = false
}
