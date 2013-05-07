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
package com.twitter.scalding.serialization

import java.io.InputStream
import java.io.OutputStream
import java.io.Serializable
import java.nio.ByteBuffer

import org.apache.hadoop.io.serializer.{Serialization, Deserializer, Serializer, WritableSerialization}

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.{Serializer => KSerializer}
import com.esotericsoftware.kryo.io.{Input, Output}

import cascading.kryo.KryoSerialization;
import cascading.tuple.hadoop.TupleSerialization
import cascading.tuple.hadoop.io.BufferedInputStream

import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import scala.collection.immutable.HashMap

import com.twitter.scalding.DateRange
import com.twitter.scalding.RichDate
import com.twitter.scalding.Args

import com.twitter.chill._

import org.objenesis.strategy.StdInstantiatorStrategy;

class KryoHadoop extends KryoSerialization {

  /** TODO!!!
   * Deal with this issue.  The problem is grouping by Kryo serialized
   * objects silently breaks the results.  If Kryo gets in front of TupleSerialization
   * (and possibly Writable, unclear at this time), grouping is broken.
   * There are two issues here:
   * 1) Kryo objects not being compared properly.
   * 2) Kryo being used instead of cascading.
   *
   * We must identify each and fix these bugs.
   */
  val highPrioritySerializations = List(new WritableSerialization, new TupleSerialization)

  override def accept(klass : Class[_]) = {
    highPrioritySerializations.forall { !_.accept(klass) }
  }

  override def newKryo() : Kryo = {
    val k = new Kryo {
      lazy val objSer = new ObjectSerializer[AnyRef]
      override def newDefaultSerializer(cls : Class[_]) : KSerializer[_] = {
        if(objSer.accepts(cls)) {
          objSer
        }
        else {
          super.newDefaultSerializer(cls)
        }
      }
    }
    k.setInstantiatorStrategy(new StdInstantiatorStrategy());
    k
  }

  override def decorateKryo(newK : Kryo) {
    // These are scalding objects:
    newK.register(classOf[RichDate], new RichDateSerializer())
    newK.register(classOf[DateRange], new DateRangeSerializer())
    newK.register(classOf[Args], new ArgsSerializer)
    // Some of the monoids from Algebird that we use:
    newK.register(classOf[com.twitter.algebird.AveragedValue], new AveragedValueSerializer)
    newK.register(classOf[com.twitter.algebird.DecayedValue], new DecayedValueSerializer)
    newK.register(classOf[com.twitter.algebird.HyperLogLogMonoid], new HLLMonoidSerializer)
    newK.register(classOf[com.twitter.algebird.Moments], new MomentsSerializer)
    newK.addDefaultSerializer(classOf[com.twitter.algebird.HLL], new HLLSerializer)

    // Register all the chill serializers:
    KryoSerializer.registerAll(newK)

    //Add commonly used types with Fields serializer:
    registeredTypes.foreach { cls => newK.register(cls) }
    /**
     * Pipes can be swept up into closures inside of case classes.  This can generally
     * be safely ignored.  If the case class has a method that actually accesses something
     * in the job, you will get a null pointer exception, so it shouldn't cause data corruption.
     * a more robust solution is to use Spark's closure cleaner approach on every object that
     * is serialized, but that's very expensive.
     */
     newK.addDefaultSerializer(classOf[cascading.pipe.Pipe], new SingletonSerializer(null))
    // keeping track of references is costly for memory, and often triggers OOM on Hadoop
    val useRefs = getConf.getBoolean("scalding.kryo.setreferences", false)
    newK.setReferences(useRefs)
  }

  // Types to pre-register.
  // TODO: this was cargo-culted from spark. We should actually measure to see the best
  // choices for the common use cases. Since hadoop tells us the class we are deserializing
  // the benefit of this is much less than spark
  def registeredTypes : List[Class[_]] = {
    List(
      // Arrays
      Array(1), Array(1.0), Array(1.0f), Array(1L), Array(""), Array(("", "")),
      Array(new java.lang.Object), Array(1.toByte), Array(true), Array('c'),
      // Options and Either
      Some(1), Left(1), Right(1)
    ).map { _.getClass }
  }
}
