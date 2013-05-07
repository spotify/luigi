package com.twitter.scalding

import scala.collection.mutable.{Buffer, ListBuffer}
import scala.annotation.tailrec

import cascading.tuple.Tuple
import cascading.tuple.TupleEntry

import org.apache.hadoop.mapred.JobConf

object JobTest {
  def apply(jobName : String) = {
    new JobTest((args : Args) => Job(jobName,args))
  }
  def apply(cons : (Args) => Job) = {
    new JobTest(cons)
  }
  def apply[T <: Job : Manifest] = {
    val cons = { (args : Args) => manifest[T].erasure
      .getConstructor(classOf[Args])
      .newInstance(args)
      .asInstanceOf[Job] }
    new JobTest(cons)
  }
}

/**
 * This class is used to construct unit tests for scalding jobs.
 * You should not use it unless you are writing tests.
 * For examples of how to do that, see the tests included in the
 * main scalding repository:
 * https://github.com/twitter/scalding/tree/master/src/test/scala/com/twitter/scalding
 */
class JobTest(cons : (Args) => Job) extends TupleConversions {
  private var argsMap = Map[String, List[String]]()
  private val callbacks = Buffer[() => Unit]()
  // TODO: Switch the following maps and sets from Source to String keys
  // to guard for scala equality bugs
  private var sourceMap = Map[Source, Buffer[Tuple]]()
  private var sinkSet = Set[Source]()
  private var fileSet = Set[String]()

  def arg(inArg : String, value : List[String]) = {
    argsMap += inArg -> value
    this
  }

  def arg(inArg : String, value : String) = {
    argsMap += inArg -> List(value)
    this
  }

  def source(s : Source, iTuple : Iterable[Product]) = {
    sourceMap += s -> iTuple.toList.map{ productToTuple(_) }.toBuffer
    this
  }

  def sink[A](s : Source)(op : Buffer[A] => Unit )
    (implicit conv : TupleConverter[A]) = {
    val buffer = new ListBuffer[Tuple]
    sourceMap += s -> buffer
    sinkSet += s
    callbacks += (() => op(buffer.map { tup => conv(new TupleEntry(tup)) }))
    this
  }

  // Simulates the existance of a file so that mode.fileExists returns true.  We
  // do not simulate the file contents; that should be done through mock
  // sources.
  def registerFile(filename : String) = {
    fileSet += filename
    this
  }


  def run = {
    runJob(initJob(false), true)
    this
  }

  def runWithoutNext(useHadoop : Boolean = false) = {
    runJob(initJob(useHadoop), false)
    this
  }

  def runHadoop = {
    runJob(initJob(true), true)
    this
  }

  // This SITS is unfortunately needed to get around Specs
  def finish : Unit = { () }

  // Registers test files, initializes the global mode, and creates a job.
  private def initJob(useHadoop : Boolean) : Job = {
    // Create a global mode to use for testing.
    val testMode : TestMode =
      if (useHadoop) {
        val conf = new JobConf
        // Set the polling to a lower value to speed up tests:
        conf.set("jobclient.completion.poll.interval", "100")
        HadoopTest(conf, sourceMap)
      } else {
        Test(sourceMap)
      }
    testMode.registerTestFiles(fileSet)
    Mode.mode = testMode

    // Construct a job.
    cons(new Args(argsMap))
  }

  @tailrec
  private final def runJob(job : Job, runNext : Boolean) : Unit = {
    job.buildFlow.complete
    val next : Option[Job] = if (runNext) { job.next } else { None }
    next match {
      case Some(nextjob) => runJob(nextjob, runNext)
      case None => {
        Mode.mode match {
          case hadoopTest @ HadoopTest(_,_) => {
            // The sinks are written to disk, we need to clean them up:
            sinkSet.foreach{ hadoopTest.finalize(_) }
          }
          case _ => ()
        }
        // Now it is time to check the test conditions:
        callbacks.foreach { cb => cb() }
      }
    }
  }
}
