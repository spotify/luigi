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

import cascading.pipe.Pipe

/**
 * This object has all the implicit functions and values that are used
 * to make the scalding DSL.
 *
 * It's useful to import Dsl._ when you are writing scalding code outside
 * of a Job.
 */
object Dsl extends FieldConversions with TupleConversions with GeneratedTupleAdders {
  implicit def pipeToRichPipe(pipe : Pipe) : RichPipe = new RichPipe(pipe)
  implicit def richPipeToPipe(rp : RichPipe) : Pipe = rp.pipe
  // Scala 2.8 iterators don't have a scanLeft
  implicit def iteratorToScanIterator[T](it : Iterator[T]) = new {
    def scanLeft[U](init : U)(fn : (U,T) => U) : Iterator[U] = {
      new ScanLeftIterator(it, init, fn)
    }
  }
}
