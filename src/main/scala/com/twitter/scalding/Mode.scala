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
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapred.JobConf

import cascading.flow.FlowConnector
import cascading.flow.FlowProcess
import cascading.flow.hadoop.HadoopFlowProcess
import cascading.flow.hadoop.HadoopFlowConnector
import cascading.flow.local.LocalFlowConnector
import cascading.flow.local.LocalFlowProcess
import cascading.pipe.Pipe
import cascading.tap.Tap
import cascading.tuple.Tuple
import cascading.tuple.TupleEntryIterator

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.mutable.Buffer
import scala.collection.mutable.{Map => MMap}
import scala.collection.mutable.{Set => MSet}

object Mode {
  /**
  * This mode is used by default by sources in read and write
  */
  implicit var mode : Mode = Local(false)

  // This should be passed ALL the args supplied after the job name
  def apply(args : Args, config : Configuration) : Mode = {
    val strictSources = args.boolean("tool.partialok") == false
    if (!strictSources) {
      // TODO we should do smarter logging here
      println("[Scalding:INFO] using --tool.partialok. Missing log data won't cause errors.")
    }

    if (args.boolean("local"))
       Local(strictSources)
    else if (args.boolean("hdfs"))
      Hdfs(strictSources, config)
    else
      sys.error("[ERROR] Mode must be one of --local or --hdfs, you provided '" + mode + "'")
  }
}
/**
* There are three ways to run jobs
* sourceStrictness is set to true
*/
abstract class Mode(val sourceStrictness : Boolean) {
  // We can't name two different pipes with the same name.
  // NOTE: there is a subtle bug in scala regarding case classes
  // with multiple sets of arguments, and their equality.
  // For this reason, we use Source.toString as the key in this map
  protected val sourceMap = MMap[String, (Source, Pipe)]()

  def config = Map[AnyRef,AnyRef]()
  def newFlowConnector(props : Map[AnyRef,AnyRef]) : FlowConnector

  /*
   * Using a new FlowProcess, which is only suitable for reading outside
   * of a map/reduce job, open a given tap and return the TupleEntryIterator
   */
  def openForRead(tap : Tap[_,_,_]) : TupleEntryIterator

  /**
  * Cascading can't handle multiple head pipes with the same
  * name.  This handles them by caching the source and only
  * having a single head pipe to represent each head.
  */
  def getReadPipe(s : Source, p: => Pipe) : Pipe = {
    val entry = sourceMap.getOrElseUpdate(s.toString, (s, p))
    val mapSource = entry._1
    if (mapSource.toString == s.toString && (mapSource != s)) {
      // We have seen errors with case class equals, and names so we are paranoid here:
      throw new Exception("Duplicate Source.toString are equal, but values are not.  May result in invalid data: " + s.toString)
    } else {
      entry._2
    }
  }

  def getSourceNamed(name : String) : Option[Source] = {
    sourceMap.get(name).map { _._1 }
  }

  // Returns true if the file exists on the current filesystem.
  def fileExists(filename : String) : Boolean
}

trait HadoopMode extends Mode {

  def jobConf : Configuration

  override def config = {
    jobConf.foldLeft(Map[AnyRef, AnyRef]()) {
      (acc, kv) => acc + ((kv.getKey, kv.getValue))
    }
  }

  override def newFlowConnector(props : Map[AnyRef,AnyRef]) = {
    new HadoopFlowConnector(props)
  }

  // TODO  unlike newFlowConnector, this does not look at the Job.config
  override def openForRead(tap : Tap[_,_,_]) = {
    val htap = tap.asInstanceOf[Tap[JobConf,_,_]]
    val fp = new HadoopFlowProcess(new JobConf(jobConf))
    htap.openForRead(fp)
  }
}

trait CascadingLocal extends Mode {
  override def newFlowConnector(props : Map[AnyRef,AnyRef]) = new LocalFlowConnector(props)
  override def openForRead(tap : Tap[_,_,_]) = {
    val ltap = tap.asInstanceOf[Tap[Properties,_,_]]
    val fp = new LocalFlowProcess
    ltap.openForRead(fp)
  }
}

// Mix-in trait for test modes; overrides fileExists to allow the registration
// of mock filenames for testing.
trait TestMode extends Mode {
  private var fileSet = Set[String]()
  def registerTestFiles(files : Set[String]) = fileSet = files
  override def fileExists(filename : String) : Boolean = fileSet.contains(filename)
}

case class Hdfs(strict : Boolean, conf : Configuration) extends Mode(strict) with HadoopMode {
  override def jobConf = conf
  override def fileExists(filename : String) : Boolean =
    FileSystem.get(jobConf).exists(new Path(filename))
}

case class HadoopTest(conf : Configuration, buffers : Map[Source,Buffer[Tuple]])
    extends Mode(false) with HadoopMode with TestMode {

  // This is a map from source.toString to disk path
  private val writePaths = MMap[Source, String]()
  private val allPaths = MSet[String]()

  override def jobConf = conf

  @tailrec
  private def allocateNewPath(prefix : String, idx : Int) : String = {
    val candidate = prefix + idx.toString
    if (allPaths(candidate)) {
      //Already taken, try again:
      allocateNewPath(prefix, idx + 1)
    }
    else {
      // Update all paths:
      allPaths += candidate
      candidate
    }
  }

  private val basePath = "/tmp/scalding/"
  // Looks up a local path to write the given source to
  def getWritePathFor(src : Source) : String = {
    writePaths.getOrElseUpdate(src, allocateNewPath(basePath + src.getClass.getName, 0))
  }

  def finalize(src : Source) {
    // Get the buffer for the given source, and empty it:
    val buf = buffers(src)
    buf.clear()
    // Now fill up this buffer with the content of the file
    val path = getWritePathFor(src)
    // We read the write tap in order to add its contents in the test buffers
    val it = openForRead(src.createTap(Write)(this))
    while(it != null && it.hasNext) {
      buf += new Tuple(it.next.getTuple)
    }
    //Clean up this data off the disk
    new File(path).delete()
    writePaths -= src
  }
}

case class Local(strict : Boolean) extends Mode(strict) with CascadingLocal {
  override def fileExists(filename : String) : Boolean = new File(filename).exists
}

/**
* Memory only testing for unit tests
*/
case class Test(val buffers : Map[Source,Buffer[Tuple]]) extends Mode(false)
  with TestMode with CascadingLocal
