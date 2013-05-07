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

import scala.reflect.Manifest

/** Base class for objects which unpack an object into a tuple.
  * The packer can verify the arity, types, and also the existence
  * of the getter methods at plan time, without having the job
  * blow up in the middle of a run.
  *
  * @author Argyris Zymnis
  * @author Oscar Boykin
  */
object TupleUnpacker extends LowPriorityTupleUnpackers
abstract class TupleUnpacker[T] extends java.io.Serializable {
  def newSetter(fields : Fields) : TupleSetter[T]
}

trait LowPriorityTupleUnpackers extends TupleConversions {
  implicit def genericUnpacker[T : Manifest] = new ReflectionTupleUnpacker[T]
}

class ReflectionTupleUnpacker[T](implicit m : Manifest[T]) extends TupleUnpacker[T] {
  override def newSetter(fields : Fields) = new ReflectionSetter[T](fields)(m)
}

class ReflectionSetter[T](fields : Fields)(implicit m : Manifest[T]) extends TupleSetter[T] {

  validate // Call the validation method at the submitter

  // This is lazy because it is not serializable
  // Contains a list of methods used to set the Tuple from an input of type T
  lazy val setters = makeSetters

  // Methods and Fields are not serializable so we
  // make these defs instead of vals
  // TODO: filter by isAccessible, which somehow seems to fail
  def methodMap = m.erasure
    .getDeclaredMethods
    // Keep only methods with 0 parameter types
    .filter { m => m.getParameterTypes.length == 0 }
    .groupBy { _.getName }
    .mapValues { _.head }

  // TODO: filter by isAccessible, which somehow seems to fail
  def fieldMap = m.erasure
    .getDeclaredFields
    .groupBy { _.getName }
    .mapValues { _.head }

  def makeSetters = {
    (0 until fields.size).map { idx =>
      val fieldName = fields.get(idx).toString
      setterForFieldName(fieldName)
    }
  }

  // This validation makes sure that the setters exist
  // but does not save them in a val (due to serialization issues)
  def validate = makeSetters

  override def apply(input : T) : Tuple = {
    val values = setters.map { setFn => setFn(input) }
    new Tuple(values : _*)
  }

  override def arity = fields.size

  private def setterForFieldName(fieldName : String) : (T => AnyRef) = {
    getValueFromMethod(createGetter(fieldName))
      .orElse(getValueFromMethod(fieldName))
      .orElse(getValueFromField(fieldName))
      .getOrElse(
        throw new TupleUnpackerException("Unrecognized field: " + fieldName + " for class: " + m.erasure.getName)
      )
  }

  private def getValueFromField(fieldName : String) : Option[(T => AnyRef)] = {
    fieldMap.get(fieldName).map { f => (x : T) => f.get(x) }
  }

  private def getValueFromMethod(methodName : String) : Option[(T => AnyRef)] = {
    methodMap.get(methodName).map { m => (x : T) => m.invoke(x) }
  }

  private def upperFirst(s : String) = s.substring(0,1).toUpperCase + s.substring(1)
  private def createGetter(s : String) = "get" + upperFirst(s)
}

class TupleUnpackerException(args : String) extends Exception(args)
