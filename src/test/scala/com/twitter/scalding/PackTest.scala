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

import org.specs._

class IntContainer {
  private var firstValue = 0
  private var secondValue = 0
  def getFirstValue = firstValue
  def getSecondValue = secondValue
  def setFirstValue(v : Int) { firstValue = v }
  def setSecondValue(v : Int) { secondValue = v }
}

case class IntCaseClass(firstValue : Int, secondValue : Int)

class ContainerPopulationJob (args : Args) extends Job(args) {
  Tsv("input")
    .read
    .mapTo((0, 1) -> ('firstValue, 'secondValue)) { v : (Int, Int) => v}
    .pack[IntContainer](('firstValue, 'secondValue) -> 'combined)
    .project('combined)
    .unpack[IntContainer]('combined -> ('firstValue, 'secondValue))
    .project('firstValue, 'secondValue)
    .write(Tsv("output"))
}

class ContainerToPopulationJob (args : Args) extends Job(args) {
  Tsv("input")
    .read
    .mapTo((0, 1) -> ('firstValue, 'secondValue)) { v : (Int, Int) => v}
    .packTo[IntContainer](('firstValue, 'secondValue) -> 'combined)
    .unpackTo[IntContainer]('combined -> ('firstValue, 'secondValue))
    .write(Tsv("output"))

  Tsv("input")
    .read
    .mapTo((0, 1) -> ('firstValue, 'secondValue)) { v : (Int, Int) => v}
    .packTo[IntCaseClass](('firstValue, 'secondValue) -> 'combined)
    .unpackTo[IntCaseClass]('combined -> ('firstValue, 'secondValue))
    .write(Tsv("output-cc"))
}

class PackTest extends Specification with TupleConversions {
  noDetailedDiffs()

  val inputData = List(
    (1, 2),
    (2, 2),
    (3, 2)
  )

  "A ContainerPopulationJob" should {
    JobTest("com.twitter.scalding.ContainerPopulationJob")
      .source(Tsv("input"), inputData)
      .sink[(Int, Int)](Tsv("output")) { buf =>
        "correctly populate container objects" in {
          buf.size must_== 3
          buf.toSet must_== inputData.toSet
        }
      }
      .run
      .finish
  }

  "A ContainerToPopulationJob" should {
    JobTest("com.twitter.scalding.ContainerToPopulationJob")
      .source(Tsv("input"), inputData)
      .sink[(Int, Int)](Tsv("output")) { buf =>
        "correctly populate container objects" in {
          buf.size must_== 3
          buf.toSet must_== inputData.toSet
        }
      }
      .sink[(Int, Int)](Tsv("output-cc")) { buf =>
        "correctly populate container case class objects" in {
          buf.size must_== 3
          buf.toSet must_== inputData.toSet
        }
      }
      .run
      .finish
  }
}
