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

import cascading.pipe._
import cascading.pipe.joiner._
import cascading.tuple._

import java.lang.reflect.Method
import java.lang.reflect.Constructor

import scala.reflect.Manifest

/** Base class for classes which pack a Tuple into a serializable object.
  *
  * @author Argyris Zymnis
  * @author Oscar Boykin
  */
object TuplePacker extends CaseClassPackers
abstract class TuplePacker[T] extends java.io.Serializable {
  def newConverter(fields : Fields) : TupleConverter[T]
}

trait CaseClassPackers extends LowPriorityTuplePackers {
  implicit def caseClassPacker[T <: Product](implicit mf : Manifest[T]) = new OrderedTuplePacker[T]
}

trait LowPriorityTuplePackers extends TupleConversions {
  implicit def genericTuplePacker[T : Manifest] = new ReflectionTuplePacker[T]
}

/** Packs a tuple into any object with set methods, e.g. thrift or proto objects.
  * TODO: verify that protobuf setters for field camel_name are of the form setCamelName.
  * In that case this code works for proto.
  *
  * @author Argyris Zymnis
  * @author Oscar Boykin
  */
class ReflectionTuplePacker[T](implicit m : Manifest[T]) extends TuplePacker[T] {
  override def newConverter(fields : Fields) = new ReflectionTupleConverter[T](fields)(m)
}

class ReflectionTupleConverter[T](fields : Fields)(implicit m : Manifest[T]) extends TupleConverter[T] {
  override val arity = fields.size

  def lowerFirst(s : String) = s.substring(0,1).toLowerCase + s.substring(1)
  // Cut out "set" and lower case the first after
  def setterToFieldName(setter : Method) = lowerFirst(setter.getName.substring(3))

  def validate {
    //We can't touch setters because that shouldn't be accessed until map/reduce side, not
    //on submitter.
    val missing = Dsl.asList(fields).filter { f => !getSetters.contains(f.toString) }.headOption
    assert(missing.isEmpty, "Field: " + missing.get.toString + " not in setters")
  }
  validate

  def getSetters = m.erasure
    .getDeclaredMethods
    .filter { _.getName.startsWith("set") }
    .groupBy { setterToFieldName(_) }
    .mapValues { _.head }

  // Do all the reflection for the setters we need:
  // This needs to be lazy because Method is not serializable
  // TODO: filter by isAccessible, which somehow seems to fail
  lazy val setters = getSetters

  override def apply(input : TupleEntry) : T = {
    val newInst = m.erasure.newInstance()
    val fields = input.getFields
    (0 until fields.size).map { idx =>
      val thisField = fields.get(idx)
      val setMethod = setters(thisField.toString)
      setMethod.invoke(newInst, input.getObject(thisField))
    }
    newInst.asInstanceOf[T]
  }
}

/**
 * This just blindly uses the first public constructor with the same arity as the fields size
 */
class OrderedTuplePacker[T](implicit m : Manifest[T]) extends TuplePacker[T] {
  override def newConverter(fields : Fields) = new OrderedConstructorConverter[T](fields)(m)
}

class OrderedConstructorConverter[T](fields : Fields)(implicit mf : Manifest[T]) extends TupleConverter[T] {
  override val arity = fields.size
  // Keep this as a method, so we can validate by calling, but don't serialize it, and keep it lazy
  // below
  def getConstructor = mf.erasure
    .getConstructors
    .filter { _.getParameterTypes.size == fields.size }
    .head.asInstanceOf[Constructor[T]]

  //Make sure we can actually get a constructor:
  getConstructor

  lazy val cons = getConstructor

  override def apply(input : TupleEntry) : T = {
    val tup = input.getTuple
    val args = (0 until tup.size).map { tup.getObject(_) }
    cons.newInstance(args : _*)
  }
}
