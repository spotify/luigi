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

package com.twitter.scalding;

import cascading.tuple.Hasher;

import java.io.Serializable;
import java.util.Comparator;

/*
 * Handles numerical hashing properly
 */
class IntegralComparator extends Comparator[AnyRef] with Hasher[AnyRef] with Serializable {

  val integralTypes : Set[Class[_]] = Set(classOf[java.lang.Long],
                          classOf[java.lang.Integer],
                          classOf[java.lang.Short],
                          classOf[java.lang.Byte])

  def isIntegral(boxed : AnyRef) = integralTypes(boxed.getClass)

  override def compare(a1: AnyRef, a2: AnyRef) : Int = {
    val a1IsNull = if (null == a1) 1 else 0
    val a2IsNull = if (null == a2) 1 else 0
    if (a1IsNull + a2IsNull > 0) {
      //if a2IsNull, but a1IsNot, a2 is less:
      a2IsNull - a1IsNull
    }
    else if (isIntegral(a1) && isIntegral(a2)) {
      val long1 = a1.asInstanceOf[Number].longValue
      val long2 = a2.asInstanceOf[Number].longValue
      if (long1 < long2)
        -1
      else if (long1 > long2)
        1
      else
        0
    }
    else
      a1.asInstanceOf[Comparable[AnyRef]].compareTo(a2)
  }

  override def hashCode(obj : AnyRef) : Int = {
    if (null == obj) {
      0
    }
    else if (isIntegral(obj)) {
      obj.asInstanceOf[Number]
        .longValue
        .hashCode
    }
    else {
      //Use the default:
      obj.hashCode
    }
  }
}
