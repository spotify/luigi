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
package com.twitter.scalding.typed

import cascading.pipe.HashJoin
import cascading.pipe.joiner.{Joiner => CJoiner, JoinerClosure}
import cascading.tuple.{Tuple => CTuple, Fields, TupleEntry}

import com.twitter.scalding._

import scala.collection.JavaConverters._

/** This fully replicates the entire right hand side to the left.
 * This means that we never see the case where the key is absent on the left. This
 * means implementing a right-join is impossible.
 * Note, there is no reduce-phase in this operation.
 * The next issue is that obviously, unlike a cogroup, for a fixed key, each joiner will
 * NOT See all the tuples with those keys. This is because the keys on the left are
 * distributed across many machines
 * See hashjoin:
 * http://docs.cascading.org/cascading/2.0/javadoc/cascading/pipe/HashJoin.html
 */
class HashCoGrouped2[K,V,W,R](left: Grouped[K,V],
  right: Grouped[K,W],
  hashjoiner: (K, V, Iterable[W]) => Iterator[R])
  extends java.io.Serializable {

  lazy val toTypedPipe : TypedPipe[(K,R)] = {
    // Actually make a new coGrouping:
    assert(left.valueSort == None, "secondary sorting unsupported in HashCoGrouped2")
    assert(right.valueSort == None, "secondary sorting unsupported in HashCoGrouped2")

    import Dsl._
    val rightGroupKey = RichFields(StringField[K]("key1")(right.ordering, None))
    val joiner = Joiner.toCogroupJoiner2(hashjoiner)
    val newPipe = new HashJoin(left.pipe, left.groupKey,
      right.pipe.rename(('key, 'value) -> ('key1, 'value1)),
      rightGroupKey,
      new Joiner2(left.streamMapping, right.streamMapping, joiner))

    //Construct the new TypedPipe
    TypedPipe.from(newPipe.project('key,'value), ('key, 'value))
  }
}
