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

import java.io.File
import java.util.{Calendar, TimeZone, UUID, Map => JMap}

import cascading.flow.hadoop.HadoopFlowProcess
import cascading.flow.{FlowProcess, FlowDef}
import cascading.flow.local.LocalFlowProcess
import cascading.pipe.Pipe
import cascading.scheme.Scheme
import cascading.scheme.local.{TextLine => CLTextLine, TextDelimited => CLTextDelimited}
import cascading.scheme.hadoop.{TextLine => CHTextLine, TextDelimited => CHTextDelimited, SequenceFile => CHSequenceFile, WritableSequenceFile => CHWritableSequenceFile }
import cascading.tap.hadoop.Hfs
import cascading.tap.MultiSourceTap
import cascading.tap.SinkMode
import cascading.tap.Tap
import cascading.tap.local.FileTap
import cascading.tuple.{Tuple, TupleEntry, TupleEntryIterator, Fields}

import com.etsy.cascading.tap.local.LocalTap

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.io.Writable
import org.apache.commons.lang.StringEscapeUtils

import collection.mutable.{Buffer, MutableList}
import scala.collection.JavaConverters._

import com.codahale.jerkson.Json

/**
* This is a base class for File-based sources
*/
abstract class FileSource extends Source {

  protected def pathIsGood(p : String, conf : Configuration) = {
    val path = new Path(p)
    Option(path.getFileSystem(conf).globStatus(path)).
        map(_.length > 0).
        getOrElse(false)
  }

  def hdfsPaths : Iterable[String]
  // By default, we write to the LAST path returned by hdfsPaths
  def hdfsWritePath = hdfsPaths.last
  def localPath : String

  override def createTap(readOrWrite : AccessMode)(implicit mode : Mode) : Tap[_,_,_] = {
    mode match {
      // TODO support strict in Local
      case Local(_) => {
        val sinkmode = readOrWrite match {
          case Read => SinkMode.KEEP
          case Write => SinkMode.REPLACE
        }
        createLocalTap(sinkmode)
      }
      case hdfsMode @ Hdfs(_, _) => readOrWrite match {
        case Read => createHdfsReadTap(hdfsMode)
        case Write => castHfsTap(new Hfs(hdfsScheme, hdfsWritePath, SinkMode.REPLACE))
      }
      case _ => super.createTap(readOrWrite)(mode)
    }
  }

  def createLocalTap(sinkMode : SinkMode) : Tap[_,_,_] = new FileTap(localScheme, localPath, sinkMode)

  // This is only called when Mode.sourceStrictness is true
  protected def hdfsReadPathsAreGood(conf : Configuration) = {
    hdfsPaths.forall { pathIsGood(_, conf) }
  }

  /*
   * This throws InvalidSourceException if:
   * 1) we are in sourceStrictness mode and all sources are not present.
   * 2) we are not in the above, but some source has no input whatsoever
   * TODO this only does something for HDFS now. Maybe we should do the same for LocalMode
   */
  override def validateTaps(mode : Mode) : Unit = {
    mode match {
      case Hdfs(strict, conf) => {
        if (strict && (!hdfsReadPathsAreGood(conf))) {
          throw new InvalidSourceException(
            "[" + this.toString + "] Data is missing from one or more paths in: " +
            hdfsPaths.toString)
        }
        else if (!hdfsPaths.exists { pathIsGood(_, conf) }) {
          //Check that there is at least one good path:
          throw new InvalidSourceException(
            "[" + this.toString + "] No good paths in: " + hdfsPaths.toString)
        }
      }
      case _ => ()
    }
  }

  /*
   * Get all the set of valid paths based on source strictness.
   */
  protected def goodHdfsPaths(hdfsMode : Hdfs) = {
    if (hdfsMode.sourceStrictness) {
      //we check later that all the paths are good
      hdfsPaths
    }
    else {
      // If there are no matching paths, this is still an error, we need at least something:
      hdfsPaths.filter{ pathIsGood(_, hdfsMode.jobConf) }
    }
  }

  protected def createHdfsReadTap(hdfsMode : Hdfs) : Tap[JobConf, _, _] = {
    val taps : List[Tap[JobConf, RecordReader[_,_], OutputCollector[_,_]]] =
      goodHdfsPaths(hdfsMode)
        .toList.map { path => castHfsTap(new Hfs(hdfsScheme, path, SinkMode.KEEP)) }
    taps.size match {
      case 0 => {
        // This case is going to result in an error, but we don't want to throw until
        // validateTaps, so we just put a dummy path to return something so the
        // Job constructor does not fail.
        castHfsTap(new Hfs(hdfsScheme, hdfsPaths.head, SinkMode.KEEP))
      }
      case 1 => taps.head
      case _ => new ScaldingMultiSourceTap(taps)
    }
  }
}

class ScaldingMultiSourceTap(taps : Seq[Tap[JobConf, RecordReader[_,_], OutputCollector[_,_]]])
    extends MultiSourceTap[Tap[JobConf, RecordReader[_,_], OutputCollector[_,_]], JobConf, RecordReader[_,_]](taps : _*) {
  private final val randomId = UUID.randomUUID.toString
  override def getIdentifier() = randomId
}

/**
* The fields here are ('offset, 'line)
*/
trait TextLineScheme extends Mappable[String] {
  import Dsl._
  override val converter = implicitly[TupleConverter[String]]
  override def localScheme = new CLTextLine()
  override def hdfsScheme = HadoopSchemeInstance(new CHTextLine())
  //In textline, 0 is the byte position, the actual text string is in column 1
  override def sourceFields = Dsl.intFields(Seq(1))
}

/**
* Mix this in for delimited schemes such as TSV or one-separated values
* By default, TSV is given
*/
trait DelimitedScheme extends Source {
  //override these as needed:
  val fields = Fields.ALL
  //This is passed directly to cascading where null is interpretted as string
  val types : Array[Class[_]] = null
  val separator = "\t"
  val skipHeader = false
  val writeHeader = false
  //These should not be changed:
  override def localScheme = new CLTextDelimited(fields, skipHeader, writeHeader, separator, types)

  override def hdfsScheme =
    HadoopSchemeInstance(new CHTextDelimited(fields, skipHeader, writeHeader, separator, types))
}

trait SequenceFileScheme extends Source {
  //override these as needed:
  val fields = Fields.ALL
  // TODO Cascading doesn't support local mode yet
  override def hdfsScheme = {
    HadoopSchemeInstance(new CHSequenceFile(fields))
  }
}

trait WritableSequenceFileScheme extends Source {
  //override these as needed:
  val fields = Fields.ALL
  val keyType : Class[_ <: Writable]
  val valueType : Class[_ <: Writable]

  // TODO Cascading doesn't support local mode yet
  override def hdfsScheme =
    HadoopSchemeInstance(new CHWritableSequenceFile(fields, keyType, valueType))
}

/**
 * Ensures that a _SUCCESS file is present in the Source path.
 */
trait SuccessFileSource extends FileSource {
  override protected def pathIsGood(p: String, conf: Configuration) = {
    val path = new Path(p)
    Option(path.getFileSystem(conf).globStatus(path)).
      map { statuses: Array[FileStatus] =>
        // Must have a file that is called "_SUCCESS"
        statuses.exists { fs: FileStatus =>
          fs.getPath.getName == "_SUCCESS"
        }
      }
      .getOrElse(false)
  }
}

trait LocalTapSource extends FileSource {
  override def createLocalTap(sinkMode : SinkMode) = new LocalTap(localPath, hdfsScheme, sinkMode).asInstanceOf[Tap[_, _, _]]
}

abstract class FixedPathSource(path : String*) extends FileSource {
  def localPath = { assert(path.size == 1); path(0) }
  def hdfsPaths = path.toList
}

/**
* Tab separated value source
*/

case class Tsv(p : String, override val fields : Fields = Fields.ALL,
  override val skipHeader : Boolean = false, override val writeHeader: Boolean = false) extends FixedPathSource(p)
  with DelimitedScheme

/** Allows you to set the types, prefer this:
 * If T is a subclass of Product, we assume it is a tuple. If it is not, wrap T in a Tuple1:
 * e.g. TypedTsv[Tuple1[List[Int]]]
 */
object TypedTsv {
  def apply[T : Manifest : TupleConverter](paths : Seq[String]) = {
    val f = Dsl.intFields(0 until implicitly[TupleConverter[T]].arity)
    new TypedDelimited[T](paths, f, false, false, "\t")
  }
  def apply[T : Manifest : TupleConverter](path : String) = {
    val f = Dsl.intFields(0 until implicitly[TupleConverter[T]].arity)
    new TypedDelimited[T](Seq(path), f, false, false, "\t")
  }
  def apply[T : Manifest : TupleConverter](path : String, f : Fields) = {
    new TypedDelimited[T](Seq(path), f, false, false, "\t")
  }
}

class TypedDelimited[T](p : Seq[String], override val fields : Fields,
  override val skipHeader : Boolean, override val writeHeader : Boolean,
  override val separator : String)
  (implicit mf : Manifest[T], override val converter : TupleConverter[T]) extends FixedPathSource(p : _*)
  with DelimitedScheme with Mappable[T] {

  // For Mappable:
  override def mapTo[U](out : Fields)(fun : (T) => U)
    (implicit flowDef : FlowDef, mode : Mode, setter : TupleSetter[U]) = {
    RichPipe(read(flowDef, mode)).mapTo[T,U](sourceFields -> out)(fun)(converter, setter)
  }
  // For Mappable:
  override def flatMapTo[U](out : Fields)(fun : (T) => Iterable[U])
    (implicit flowDef : FlowDef, mode : Mode, setter : TupleSetter[U]) = {
    RichPipe(read(flowDef, mode)).flatMapTo[T,U](sourceFields -> out)(fun)(converter, setter)
  }


  override val types : Array[Class[_]] = {
    if (classOf[scala.Product].isAssignableFrom(mf.erasure)) {
      //Assume this is a Tuple:
      mf.typeArguments.map { _.erasure }.toArray
    }
    else {
      //Assume there is only a single item
      Array(mf.erasure)
    }
  }
  override lazy val toString : String = "TypedDelimited" +
    ((p,fields,skipHeader,writeHeader, separator,mf).toString)

  override def equals(that : Any) : Boolean = Option(that)
    .map { _.toString == this.toString }.getOrElse(false)

  override lazy val hashCode : Int = toString.hashCode
}

/**
* One separated value (commonly used by Pig)
*/
case class Osv(p : String, f : Fields = Fields.ALL) extends FixedPathSource(p)
  with DelimitedScheme {
    override val fields = f
    override val separator = "\1"
}

object TimePathedSource {
  val YEAR_MONTH_DAY = "/%1$tY/%1$tm/%1$td"
  val YEAR_MONTH_DAY_HOUR = "/%1$tY/%1$tm/%1$td/%1$tH"
}

/**
 * This will automatically produce a globbed version of the given path.
 * THIS MEANS YOU MUST END WITH A / followed by * to match a file
 * For writing, we write to the directory specified by the END time.
 */
abstract class TimePathedSource(val pattern : String, val dateRange : DateRange, val tz : TimeZone) extends FileSource {
  val glober = Globifier(pattern)(tz)
  override def hdfsPaths = glober.globify(dateRange)
  //Write to the path defined by the end time:
  override def hdfsWritePath = {
    val lastSlashPos = pattern.lastIndexOf('/')
    assert(lastSlashPos >= 0, "/ not found in: " + pattern)
    val stripped = pattern.slice(0,lastSlashPos)
    String.format(stripped, dateRange.end.toCalendar(tz))
  }
  override def localPath = pattern

  /*
   * Get path statuses based on daterange.
   */
  protected def getPathStatuses(conf : Configuration) : Iterable[(String, Boolean)] = {
    List("%1$tH" -> Hours(1), "%1$td" -> Days(1)(tz),
      "%1$tm" -> Months(1)(tz), "%1$tY" -> Years(1)(tz))
      .find { unitDur : (String,Duration) => pattern.contains(unitDur._1) }
      .map { unitDur =>
        // This method is exhaustive, but too expensive for Cascading's JobConf writing.
        dateRange.each(unitDur._2)
          .map { dr : DateRange =>
            val path = String.format(pattern, dr.start.toCalendar(tz))
            val good = pathIsGood(path, conf)
            (path, good)
          }
      }
      .getOrElse(Nil : Iterable[(String, Boolean)])
  }

  // Override because we want to check UNGLOBIFIED paths that each are present.
  override def hdfsReadPathsAreGood(conf : Configuration) : Boolean = {
    getPathStatuses(conf).forall{ x =>
      if (!x._2) {
        System.err.println("[ERROR] Path: " + x._1 + " is missing in: " + toString)
      }
      x._2
    }
  }

  override def toString =
    "TimePathedSource(" + pattern + ", " + dateRange + ", " + tz + ")"

  override def equals(that : Any) =
    (that != null) &&
    (this.getClass == that.getClass) &&
    this.pattern == that.asInstanceOf[TimePathedSource].pattern &&
    this.dateRange == that.asInstanceOf[TimePathedSource].dateRange &&
    this.tz == that.asInstanceOf[TimePathedSource].tz

  override def hashCode = pattern.hashCode +
    31 * dateRange.hashCode +
    (31 ^ 2) * tz.hashCode
}

/*
 * A source that contains the most recent existing path in this date range.
 */
abstract class MostRecentGoodSource(p : String, dr : DateRange, t : TimeZone)
    extends TimePathedSource(p, dr, t) {

  override def toString =
    "MostRecentGoodSource(" + p + ", " + dr + ", " + t + ")"

  override protected def goodHdfsPaths(hdfsMode : Hdfs) = getPathStatuses(hdfsMode.jobConf)
    .toList
    .reverse
    .find{ _._2 }
    .map{ x => x._1 }

  override def hdfsReadPathsAreGood(conf : Configuration) = getPathStatuses(conf)
    .exists{ _._2 }
}

case class TextLine(p : String) extends FixedPathSource(p) with TextLineScheme

case class SequenceFile(p : String, f : Fields = Fields.ALL) extends FixedPathSource(p) with SequenceFileScheme with LocalTapSource {
  override val fields = f
}

case class MultipleSequenceFiles(p : String*) extends FixedPathSource(p:_*) with SequenceFileScheme

case class MultipleTextLineFiles(p : String*) extends FixedPathSource(p:_*) with TextLineScheme

case class WritableSequenceFile[K <: Writable : Manifest, V <: Writable : Manifest](p : String, f : Fields) extends FixedPathSource(p)
  with WritableSequenceFileScheme {
    override val fields = f
    override val keyType = manifest[K].erasure.asInstanceOf[Class[_ <: Writable]]
    override val valueType = manifest[V].erasure.asInstanceOf[Class[_ <: Writable]]
  }

/**
* This Source writes out the TupleEntry as a simple JSON object, using the field names
* as keys and the string representation of the values.
* Only useful for writing, on read it is identical to TextLineScheme.
*/
case class JsonLine(p : String) extends FixedPathSource(p) with TextLineScheme {
  import Dsl._

  override def transformForWrite(pipe : Pipe) = pipe.mapTo(Fields.ALL -> 'json) {
    t : TupleEntry =>
    Json.generate(t.getFields.asScala.map(f => f.toString -> t.getString(f.toString)).toMap)
  }
}
