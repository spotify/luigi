package com.twitter.scalding.mathematics

import com.twitter.scalding._
import cascading.pipe.joiner._
import org.specs._
import com.twitter.algebird.Group

object TUtil {
  def printStack( fn: => Unit ) {
    try { fn } catch { case e : Throwable => e.printStackTrace; throw e }
  }
}

class MatrixProd(args : Args) extends Job(args) {

  import Matrix._

  val mat1 = Tsv("mat1",('x1,'y1,'v1))
    .toMatrix[Int,Int,Double]('x1,'y1,'v1)

  val gram = mat1 * mat1.transpose
  gram.pipe.write(Tsv("product"))
}


class MatrixSum(args : Args) extends Job(args) {

  import Matrix._

  val mat1 = Tsv("mat1",('x1,'y1,'v1))
    .mapToMatrix('x1,'y1,'v1) { rowColVal : (Int,Int,Double) => rowColVal }
  val mat2 = Tsv("mat2",('x2,'y2,'v2))
    .mapToMatrix('x2,'y2,'v2) { rowColVal : (Int,Int,Double) => rowColVal }

  val sum = mat1 + mat2
  sum.pipe.write(Tsv("sum"))
}

class MatrixSum3(args : Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("mat1",('x1,'y1,'v1)).read
  val mat1 = new Matrix[Int,Int,(Double, Double, Double)]('x1,'y1,'v1, p1)

  val sum = mat1 + mat1
  sum.pipe.write(Tsv("sum"))
}


class Randwalk(args : Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("mat1",('x1,'y1,'v1)).read
  val mat1 = new Matrix[Int,Int,Double]('x1,'y1,'v1, p1)

  val mat1L1Norm = mat1.rowL1Normalize
  val randwalk = mat1L1Norm * mat1L1Norm
  randwalk.pipe.write(Tsv("randwalk"))
}

class Cosine(args : Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("mat1",('x1,'y1,'v1)).read
  val mat1 = new Matrix[Int,Int,Double]('x1,'y1,'v1, p1)

  val matL2Norm = mat1.rowL2Normalize
  val cosine = matL2Norm * matL2Norm.transpose
  cosine.pipe.write(Tsv("cosine"))
}

class Covariance(args : Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("mat1",('x1,'y1,'v1)).read
  val mat1 = new Matrix[Int,Int,Double]('x1,'y1,'v1, p1)

  val matCentered = mat1.colMeanCentering
  val cov = matCentered * matCentered.transpose
  cov.pipe.write(Tsv("cov"))
}

class VctProd(args : Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("mat1",('x1,'y1,'v1)).read
  val mat1 = new Matrix[Int,Int,Double]('x1,'y1,'v1, p1)

  val row = mat1.getRow(1)
  val rowProd = row * row.transpose
  rowProd.pipe.write(Tsv("vctProd"))
}

class VctDiv(args : Args) extends Job(args) {

  import Matrix._

  val p1 = Tsv("mat1",('x1,'y1,'v1)).read
  val mat1 = new Matrix[Int,Int,Double]('x1,'y1,'v1, p1)

  val row = mat1.getRow(1).diag
  val row2 = mat1.getRow(2).diag.inverse
  val rowDiv = row * row2
  rowDiv.pipe.write(Tsv("vctDiv"))
}

class ScalarOps(args: Args) extends Job(args) {
  import Matrix._
  val p1 = Tsv("mat1",('x1,'y1,'v1)).read
  val mat1 = new Matrix[Int,Int,Double]('x1,'y1,'v1, p1)
  (mat1 * 3.0).pipe.write(Tsv("times3"))
  (mat1 / 3.0).pipe.write(Tsv("div3"))
  (3.0 * mat1).pipe.write(Tsv("3times"))
  // Now with Scalar objects:
  (mat1.trace * mat1).pipe.write(Tsv("tracetimes"))
  (mat1 * mat1.trace).pipe.write(Tsv("timestrace"))
  (mat1 / mat1.trace).pipe.write(Tsv("divtrace"))
}

class DiagonalOps(args : Args) extends Job(args) {
  import Matrix._
  val mat = Tsv("mat1",('x1,'y1,'v1))
    .read
    .toMatrix[Int,Int,Double]('x1,'y1,'v1)
  (mat * mat.diagonal).write(Tsv("mat-diag"))
  (mat.diagonal * mat).write(Tsv("diag-mat"))
  (mat.diagonal * mat.diagonal).write(Tsv("diag-diag"))
  (mat.diagonal * mat.getCol(1)).write(Tsv("diag-col"))
  (mat.getRow(1) * mat.diagonal).write(Tsv("row-diag"))
}

class PropJob(args: Args) extends Job(args) {
  import Matrix._

  val mat = TypedTsv[(Int,Int,Int)]("graph").toMatrix
  val row = TypedTsv[(Int,Double)]("row").toRow
  val col = TypedTsv[(Int,Double)]("col").toCol

  mat.binarizeAs[Boolean].propagate(col).write(Tsv("prop-col"))
  row.propagate(mat.binarizeAs[Boolean]).write(Tsv("prop-row"))
}

class MatrixMapWithVal(args: Args) extends Job(args) {
  import Matrix._

  val mat = TypedTsv[(Int,Int,Int)]("graph").toMatrix
  val row = TypedTsv[(Int,Double)]("row").toRow

  mat.mapWithIndex { (v,r,c) => if (r == c) v else 0 }.write(Tsv("diag"))
  row.mapWithIndex { (v,c) => if (c == 0) v else 0.0 }.write(Tsv("first"))
}

class MatrixTest extends Specification {
  noDetailedDiffs() // For scala 2.9
  import Dsl._

  def toSparseMat[Row,Col,V](iter : Iterable[(Row,Col,V)]) : Map[(Row,Col),V] = {
    iter.map { it => ((it._1, it._2),it._3) }.toMap
  }
  def oneDtoSparseMat[Idx,V](iter : Iterable[(Idx,V)]) : Map[(Idx,Idx),V] = {
    iter.map { it => ((it._1, it._1), it._2) }.toMap
  }

  "A MatrixProd job" should {
    TUtil.printStack {
    JobTest("com.twitter.scalding.mathematics.MatrixProd")
      .source(Tsv("mat1",('x1,'y1,'v1)), List((1,1,1.0),(2,2,3.0),(1,2,4.0)))
      .sink[(Int,Int,Double)](Tsv("product")) { ob =>
        "correctly compute products" in {
          val pMap = toSparseMat(ob)
          pMap must be_==( Map((1,1)->17.0, (1,2)->12.0, (2,1)->12.0, (2,2)->9.0))
        }
      }
      .run
      .finish
    }
  }

  "A MatrixSum job" should {
    TUtil.printStack {
    JobTest("com.twitter.scalding.mathematics.MatrixSum")
      .source(Tsv("mat1",('x1,'y1,'v1)), List((1,1,1.0),(2,2,3.0),(1,2,4.0)))
      .source(Tsv("mat2",('x2,'y2,'v2)), List((1,3,3.0),(2,1,8.0),(1,2,4.0)))
      .sink[(Int,Int,Double)](Tsv("sum")) { ob =>
        "correctly compute sums" in {
          val pMap = toSparseMat(ob)
          pMap must be_==( Map((1,1)->1.0, (1,2)->8.0, (1,3)->3.0, (2,1)->8.0, (2,2)->3.0))
        }
      }
      .run
      .finish
    }
  }

  "A MatrixSum job, where the Matrix contains tuples as values," should {
    TUtil.printStack {
    JobTest("com.twitter.scalding.mathematics.MatrixSum3")
      .source(Tsv("mat1",('x1,'y1,'v1)), List((1,1,(1.0, 3.0, 5.0)),(2,2,(3.0, 2.0, 1.0)),(1,2,(4.0, 5.0, 2.0))))
      .sink[(Int,Int,(Double, Double, Double))](Tsv("sum")) { ob =>
        "correctly compute sums" in {
          val pMap = toSparseMat(ob)
          pMap must be_==( Map((1,1)->(2.0, 6.0, 10.0), (2,2)->(6.0, 4.0, 2.0), (1,2)->(8.0, 10.0, 4.0)))
        }
      }
      .run
      .finish
    }
  }

  "A Matrix Randwalk job" should {
    TUtil.printStack {
    JobTest("com.twitter.scalding.mathematics.Randwalk")
      /*
       * 1.0 4.0
       * 0.0 3.0
       * row normalized:
       * 1.0/5.0 4.0/5.0
       * 0.0 1.0
       * product with itself:
       * 1.0/25.0 (4.0/25.0 + 4.0/5.0)
       * 0.0 1.0
       */
      .source(Tsv("mat1",('x1,'y1,'v1)), List((1,1,1.0),(2,2,3.0),(1,2,4.0)))
      .sink[(Int,Int,Double)](Tsv("randwalk")) { ob =>
        "correctly compute matrix randwalk" in {
          val pMap = toSparseMat(ob)
          val exact = Map((1,1)->(1.0/25.0) , (1,2)->(4.0/25.0 + 4.0/5.0), (2,2)->1.0)
          val grp = implicitly[Group[Map[(Int,Int),Double]]]
          // doubles are hard to compare
          grp.minus(pMap, exact)
            .mapValues { x => x*x }
            .map { _._2 }
            .sum must be_<(0.0001)
        }
      }
      .run
      .finish
    }
  }
  "A Matrix Cosine job" should {
    TUtil.printStack {
    JobTest("com.twitter.scalding.mathematics.Cosine")
      .source(Tsv("mat1",('x1,'y1,'v1)), List((1,1,1.0),(2,2,3.0),(1,2,4.0)))
      .sink[(Int,Int,Double)](Tsv("cosine")) { ob =>
        "correctly compute cosine similarity" in {
          val pMap = toSparseMat(ob)
          pMap must be_==( Map((1,1)->1.0, (1,2)->0.9701425001453319, (2,1)->0.9701425001453319, (2,2)->1.0 ))
        }
      }
      .run
      .finish
    }
  }
  "A Matrix Covariance job" should {
    TUtil.printStack {
    JobTest("com.twitter.scalding.mathematics.Covariance")
      .source(Tsv("mat1",('x1,'y1,'v1)), List((1,1,1.0),(2,2,3.0),(1,2,4.0)))
      .sink[(Int,Int,Double)](Tsv("cov")) { ob =>
        "correctly compute matrix covariance" in {
          val pMap = toSparseMat(ob)
          pMap must be_==( Map((1,1)->0.25, (1,2)-> -0.25, (2,1)-> -0.25, (2,2)->0.25 ))
        }
      }
      .run
      .finish
    }
  }
  "A Matrix VctProd job" should {
    TUtil.printStack {
    JobTest("com.twitter.scalding.mathematics.VctProd")
      .source(Tsv("mat1",('x1,'y1,'v1)), List((1,1,1.0),(2,2,3.0),(1,2,4.0)))
      .sink[Double](Tsv("vctProd")) { ob =>
        "correctly compute vector inner products" in {
          ob(0) must be_==(17.0)
        }
      }
      .run
      .finish
    }
  }
  "A Matrix VctDiv job" should {
    TUtil.printStack {
    JobTest("com.twitter.scalding.mathematics.VctDiv")
      .source(Tsv("mat1",('x1,'y1,'v1)), List((1,1,1.0),(2,2,3.0),(1,2,4.0)))
      .sink[(Int,Double)](Tsv("vctDiv")) { ob =>
        "correctly compute vector element-wise division" in {
          val pMap = oneDtoSparseMat(ob)
          pMap must be_==( Map((2,2)->1.3333333333333333) )
        }
      }
      .run
      .finish
    }
  }
  "A Matrix ScalarOps job" should {
    TUtil.printStack {
    JobTest("com.twitter.scalding.mathematics.ScalarOps")
      .source(Tsv("mat1",('x1,'y1,'v1)), List((1,1,1.0),(2,2,3.0),(1,2,4.0)))
      .sink[(Int,Int,Double)](Tsv("times3")) { ob =>
        "correctly compute M * 3" in {
          toSparseMat(ob) must be_==( Map((1,1)->3.0, (2,2)->9.0, (1,2)->12.0) )
        }
      }
      .sink[(Int,Int,Double)](Tsv("3times")) { ob =>
        "correctly compute 3 * M" in {
          toSparseMat(ob) must be_==( Map((1,1)->3.0, (2,2)->9.0, (1,2)->12.0) )
        }
      }
      .sink[(Int,Int,Double)](Tsv("div3")) { ob =>
        "correctly compute M / 3" in {
          toSparseMat(ob) must be_==( Map((1,1)->(1.0/3.0), (2,2)->(3.0/3.0), (1,2)->(4.0/3.0)) )
        }
      }
      .sink[(Int,Int,Double)](Tsv("timestrace")) { ob =>
        "correctly compute M * Tr(M)" in {
          toSparseMat(ob) must be_==( Map((1,1)->4.0, (2,2)->12.0, (1,2)->16.0) )
        }
      }
      .sink[(Int,Int,Double)](Tsv("tracetimes")) { ob =>
        "correctly compute Tr(M) * M" in {
          toSparseMat(ob) must be_==( Map((1,1)->4.0, (2,2)->12.0, (1,2)->16.0) )
        }
      }
      .sink[(Int,Int,Double)](Tsv("divtrace")) { ob =>
        "correctly compute M / Tr(M)" in {
          toSparseMat(ob) must be_==( Map((1,1)->(1.0/4.0), (2,2)->(3.0/4.0), (1,2)->(4.0/4.0)) )
        }
      }
      .run
      .finish
    }
  }
  "A Matrix Diagonal job" should {
    TUtil.printStack {
    JobTest(new DiagonalOps(_))
      /* [[1.0 4.0]
       *  [0.0 3.0]]
       */
      .source(Tsv("mat1",('x1,'y1,'v1)), List((1,1,1.0),(2,2,3.0),(1,2,4.0)))
      .sink[(Int,Int,Double)](Tsv("diag-mat")) { ob =>
        "correctly compute diag * matrix" in {
          val pMap = toSparseMat(ob)
          pMap must be_==( Map((1,1)->1.0, (1,2)->4.0, (2,2)->9.0) )
        }
      }
      .sink[(Int,Double)](Tsv("diag-diag")) { ob =>
        "correctly compute diag * diag" in {
          val pMap = oneDtoSparseMat(ob)
          pMap must be_==( Map((1,1)->1.0, (2,2)->9.0) )
        }
      }
      .sink[(Int,Int,Double)](Tsv("mat-diag")) { ob =>
        "correctly compute matrix * diag" in {
          val pMap = toSparseMat(ob)
          pMap must be_==( Map((1,1)->1.0, (1,2)->12.0, (2,2)->9.0) )
        }
      }
      .sink[(Int,Double)](Tsv("diag-col")) { ob =>
        "correctly compute diag * col" in {
          ob.toMap must be_==( Map(1->1.0))
        }
      }
      .sink[(Int,Double)](Tsv("row-diag")) { ob =>
        "correctly compute row * diag" in {
          ob.toMap must be_==( Map(1->1.0, 2 -> 12.0))
        }
      }
      .run
      .finish
    }
  }

  "A Propagation job" should {
    TUtil.printStack {
    JobTest(new PropJob(_))
       /* [[0 1 1],
        *  [0 0 1],
        *  [1 0 0]] = List((0,1,1), (0,2,1), (1,2,1), (2,0,1))
        * [1.0 2.0 4.0] = List((0,1.0), (1,2.0), (2,4.0))
        */
      .source(TypedTsv[(Int,Int,Int)]("graph"), List((0,1,1), (0,2,1), (1,2,1), (2,0,1)))
      .source(TypedTsv[(Int,Double)]("row"), List((0,1.0), (1,2.0), (2,4.0)))
      .source(TypedTsv[(Int,Double)]("col"), List((0,1.0), (1,2.0), (2,4.0)))
      .sink[(Int,Double)](Tsv("prop-col")) { ob =>
        "correctly propagate columns" in {
          ob.toMap must be_==(Map(0 -> 6.0, 1 -> 4.0, 2 -> 1.0))
        }
      }
      .sink[(Int,Double)](Tsv("prop-row")) { ob =>
        "correctly propagate rows" in {
          ob.toMap must be_==(Map(0 -> 4.0, 1 -> 1.0, 2 -> 3.0))
        }
      }
      .run
      .finish
    }
  }

  "A MapWithIndex job" should {
    JobTest(new MatrixMapWithVal(_))
      .source(TypedTsv[(Int,Int,Int)]("graph"), List((0,1,1), (1,1,3), (0,2,1), (1,2,1), (2,0,1)))
      .source(TypedTsv[(Int,Double)]("row"), List((0,1.0), (1,2.0), (2,4.0)))
      .sink[(Int,Double)](Tsv("first")) { ob =>
        "correctly mapWithIndex on Row" in {
          ob.toMap must be_==(Map(0 -> 1.0))
        }
      }
      .sink[(Int,Int,Int)](Tsv("diag")) { ob =>
        "correctly mapWithIndex on Matrix" in {
          toSparseMat(ob) must be_==(Map((1,1) -> 3))
        }
      }
      .run
      .finish
  }
}
