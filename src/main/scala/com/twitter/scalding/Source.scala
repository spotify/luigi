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

import com.twitter.maple.tap.MemorySourceTap

import java.io.File
import java.util.TimeZone
import java.util.Calendar
import java.util.{Map => JMap}

import cascading.flow.hadoop.HadoopFlowProcess
import cascading.flow.{FlowProcess, FlowDef}
import cascading.flow.local.LocalFlowProcess
import cascading.pipe.Pipe
import cascading.scheme.{NullScheme, Scheme}
import cascading.scheme.local.{TextLine => CLTextLine, TextDelimited => CLTextDelimited}
import cascading.scheme.hadoop.{TextLine => CHTextLine, TextDelimited => CHTextDelimited, SequenceFile => CHSequenceFile}
import cascading.tap.hadoop.Hfs
import cascading.tap.MultiSourceTap
import cascading.tap.SinkMode
import cascading.tap.{Tap, SinkTap}
import cascading.tap.local.FileTap
import cascading.tuple.{Tuple, Fields, TupleEntry, TupleEntryCollector}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import collection.mutable.{Buffer, MutableList}
import scala.collection.JavaConverters._

import java.io.{InputStream, OutputStream}
import java.util.Properties

/**
 * thrown when validateTaps fails
 */
class InvalidSourceException(message : String) extends RuntimeException(message)

/*
 * Denotes the access mode for a Source
 */
sealed abstract class AccessMode
case object Read extends AccessMode
case object Write extends AccessMode

// Scala is pickier than Java about type parameters, and Cascading's Scheme
// declaration leaves some type parameters underspecified.  Fill in the type
// parameters with wildcards so the Scala compiler doesn't complain.

object HadoopSchemeInstance {
  def apply(scheme: Scheme[_, _, _, _, _]) =
    scheme.asInstanceOf[Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]]
}

/**
* Every source must have a correct toString method.  If you use
* case classes for instances of sources, you will get this for free.
* This is one of the several reasons we recommend using cases classes
*
* java.io.Serializable is needed if the Source is going to have any
* methods attached that run on mappers or reducers, which will happen
* if you implement transformForRead or transformForWrite.
*/
abstract class Source extends java.io.Serializable {
  type LocalScheme = Scheme[Properties, InputStream, OutputStream, _, _]

  def localScheme : LocalScheme = {
    sys.error("Cascading local mode not supported for: " + toString)
  }
  def hdfsScheme : Scheme[JobConf,RecordReader[_,_],OutputCollector[_,_],_,_] = {
    sys.error("Cascading Hadoop mode not supported for: " + toString)
  }

  def read(implicit flowDef : FlowDef, mode : Mode) = {
    //insane workaround for scala compiler bug
    val sources = flowDef.getSources().asInstanceOf[JMap[String,Any]]
    val srcName = this.toString
    if (!sources.containsKey(srcName)) {
      sources.put(srcName, createTap(Read)(mode))
    }
    mode.getReadPipe(this, transformForRead(new Pipe(srcName)))
  }

  /**
  * write the pipe and return the input so it can be chained into
  * the next operation
  */
  def write(pipe : Pipe)(implicit flowDef : FlowDef, mode : Mode) = {
    //insane workaround for scala compiler bug
    val sinks = flowDef.getSinks().asInstanceOf[JMap[String,Any]]
    val sinkName = this.toString
    if (!sinks.containsKey(sinkName)) {
      sinks.put(sinkName, createTap(Write)(mode))
    }
    flowDef.addTail(new Pipe(sinkName, transformForWrite(pipe)))
    pipe
  }

  protected def transformForWrite(pipe : Pipe) = pipe
  protected def transformForRead(pipe : Pipe) = pipe

  // The scala compiler has problems with the generics in Cascading
  protected def castHfsTap(tap : Hfs) : Tap[JobConf, RecordReader[_,_], OutputCollector[_,_]] = {
    tap.asInstanceOf[Tap[JobConf, RecordReader[_,_], OutputCollector[_,_]]]
  }

  /**
  * Subclasses of Source MUST override this method.  The base only handles test
  * modes, so you should invoke this method for test modes unless your Source
  * has some special handling of testing.
  */
  def createTap(readOrWrite : AccessMode)(implicit mode : Mode) : Tap[_,_,_] = {
    mode match {
      case Test(buffers) => {
        /*
        * There MUST have already been a registered sink or source in the Test mode.
        * to access this.  You must explicitly name each of your test sources in your
        * JobTest.
        */
        val buf = buffers(this)
        if (readOrWrite == Write) {
          //Make sure we wipe it out:
          buf.clear()
        }
        // TODO MemoryTap could probably be rewritten not to require localScheme, and just fields
        new MemoryTap[InputStream, OutputStream](localScheme, buf)
      }
      case hdfsTest @ HadoopTest(conf, buffers) => readOrWrite match {
        case Read => {
          val buffer = buffers(this)
          val fields = hdfsScheme.getSourceFields
          (new MemorySourceTap(buffer.toList.asJava, fields)).asInstanceOf[Tap[JobConf,_,_]]
        }
        case Write => {
          val path = hdfsTest.getWritePathFor(this)
          castHfsTap(new Hfs(hdfsScheme, path, SinkMode.REPLACE))
        }
      }
      case _ => {
        throw new RuntimeException("Source: (" + toString + ") doesn't support mode: " + mode.toString)
      }
    }
  }

  /*
   * This throws InvalidSourceException if this source is invalid.
   */
  def validateTaps(mode : Mode) : Unit = { }

  /**
  * Allows you to read a Tap on the submit node NOT FOR USE IN THE MAPPERS OR REDUCERS.
  * Typical use might be to read in Job.next to determine if another job is needed
  */
  def readAtSubmitter[T](implicit mode : Mode, conv : TupleConverter[T]) : Stream[T] = {
    val tap = createTap(Read)(mode)
    Dsl.toStream[T](mode.openForRead(tap))(conv)
  }
}

/**
* Usually as soon as we open a source, we read and do some mapping
* operation on a single column or set of columns.
* T is the type of the single column.  If doing multiple columns
* T will be a TupleN representing the types, e.g. (Int,Long,String)
*/
trait Mappable[T] extends Source {
  // These are the default column number YOU MAY NEED TO OVERRIDE!
  def sourceFields : Fields = Dsl.intFields(0 until converter.arity)
  // Due to type erasure, your subclass must supply this
  val converter : TupleConverter[T]
  def mapTo[U](out : Fields)(mf : (T) => U)
    (implicit flowDef : FlowDef, mode : Mode, setter : TupleSetter[U]) = {
    RichPipe(read(flowDef, mode)).mapTo[T,U](sourceFields -> out)(mf)(converter, setter)
  }
  /**
  * If you want to filter, you should use this and output a 0 or 1 length Iterable.
  * Filter does not change column names, and we generally expect to change columns here
  */
  def flatMapTo[U](out : Fields)(mf : (T) => Iterable[U])
    (implicit flowDef : FlowDef, mode : Mode, setter : TupleSetter[U]) = {
    RichPipe(read(flowDef, mode)).flatMapTo[T,U](sourceFields -> out)(mf)(converter, setter)
  }
}


/**
 * A tap that output nothing. It is used to drive execution of a task for side effect only. This
 * can be used to drive a pipe without actually writing to HDFS.
 * */
class NullTap[Config, Input, Output, SourceContext, SinkContext]
  extends SinkTap[Config, Output] (
    new NullScheme[Config, Input, Output, SourceContext, SinkContext](Fields.NONE, Fields.ALL),
      SinkMode.UPDATE) {

  def getIdentifier = "nullTap"
  def openForWrite(flowProcess: FlowProcess[Config], output: Output) =
    new TupleEntryCollector {
      override def add(te: TupleEntry) {}
      override def add(t: Tuple) {}
      protected def collect(te: TupleEntry) {}
    }

  def createResource(conf: Config) = true
  def deleteResource(conf: Config) = false
  def resourceExists(conf: Config) = true
  def getModifiedTime(conf: Config) = 0
}

/**
 * A source outputs nothing. It is used to drive execution of a task for side effect only.
 */
object NullSource extends Source {
  override def localScheme =
    new NullScheme[Properties, InputStream, OutputStream, Any, Any]
      (Fields.NONE, Fields.ALL)
  override def hdfsScheme =
    new NullScheme[JobConf, RecordReader[_,_], OutputCollector[_,_], Any, Any]
      (Fields.NONE, Fields.ALL)

  override def createTap(readOrWrite : AccessMode)(implicit mode : Mode) : Tap[_,_,_] = {
    readOrWrite match {
      case Read => throw new Exception("not supported, reading from null")
      case Write => mode match {
        case Hdfs(_, _) => new NullTap[JobConf, RecordReader[_,_], OutputCollector[_,_], Any, Any]
        case Local(_) => new NullTap[Properties, InputStream, OutputStream, Any, Any]
        case Test(_) => new NullTap[Properties, InputStream, OutputStream, Any, Any]
      }
    }
  }
}
