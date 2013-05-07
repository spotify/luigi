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
package com.twitter.scalding.mathematics

import com.twitter.algebird.{Monoid, Group, Ring, Field}
import com.twitter.scalding._

import cascading.pipe.assembly._
import cascading.pipe.joiner._
import cascading.pipe.Pipe
import cascading.tuple.Fields
import cascading.tuple._
import cascading.flow._
import cascading.tap._

import com.twitter.scalding.Dsl._
import scala.math.max
import scala.annotation.tailrec

/**
 * Matrix class - represents an infinite (hopefully sparse) matrix.
 *  any elements without a row are interpretted to be zero.
 *  the pipe hold ('rowIdx, 'colIdx, 'val) where in principle
 *  each row/col/value type is generic, with the constraint that ValT is a Ring[T]
 *  In practice, RowT and ColT are going to be Strings, Integers or Longs in the usual case.
 *
 * WARNING:
 *   It is NOT OKAY to use the same instance of Matrix/Row/Col with DIFFERENT Monoids/Rings/Fields.
 *   If you want to change, midstream, the Monoid on your ValT, you have to construct a new Matrix.
 *   This is due to caching of internal computation graphs.
 *
 * RowVector - handles matrices of row dimension one. It is the result of some of the matrix methods and has methods
 *  that return ColVector and diagonal matrix
 *
 * ColVector - handles matrices of col dimension one. It is the result of some of the matrix methods and has methods
 *  that return RowVector and diagonal matrix
 */

// Implicit coversions
// Add methods we want to add to pipes here:
class MatrixPipeExtensions(pipe : Pipe) {
  def toMatrix[RowT,ColT,ValT](fields : Fields)
    (implicit conv : TupleConverter[(RowT,ColT,ValT)], setter : TupleSetter[(RowT,ColT,ValT)]) = {
    val matPipe = RichPipe(pipe).mapTo(fields -> ('row,'col,'val))((tup : (RowT,ColT,ValT)) => tup)(conv,setter)
    new Matrix[RowT,ColT,ValT]('row, 'col, 'val, matPipe)
  }
  def mapToMatrix[T,RowT,ColT,ValT](fields : Fields)(mapfn : T => (RowT,ColT,ValT))
    (implicit conv : TupleConverter[T], setter : TupleSetter[(RowT,ColT,ValT)]) = {
    val matPipe = RichPipe(pipe).mapTo(fields -> ('row,'col,'val))(mapfn)(conv,setter)
    new Matrix[RowT,ColT,ValT]('row, 'col, 'val, matPipe)
  }
  def flatMapToMatrix[T,RowT,ColT,ValT](fields : Fields)(flatMapfn : T => Iterable[(RowT,ColT,ValT)])
    (implicit conv : TupleConverter[T], setter : TupleSetter[(RowT,ColT,ValT)]) = {
    val matPipe = RichPipe(pipe).flatMapTo(fields -> ('row,'col,'val))(flatMapfn)(conv,setter)
    new Matrix[RowT,ColT,ValT]('row, 'col, 'val, matPipe)
  }
}

/** This is the enrichment pattern on Mappable[T] for converting to Matrix types
 */
class MatrixMappableExtensions[T](mappable: Mappable[T])(implicit fd: FlowDef) {
  def toMatrix[Row,Col,Val](implicit ev: <:<[T,(Row,Col,Val)],
    setter: TupleSetter[(Row,Col,Val)]) : Matrix[Row,Col,Val] =
    mapToMatrix { _.asInstanceOf[(Row,Col,Val)] }

  def mapToMatrix[Row,Col,Val](fn: (T) => (Row,Col,Val))
    (implicit setter: TupleSetter[(Row,Col,Val)]) : Matrix[Row,Col,Val] = {
    val fields = ('row, 'col, 'val)
    val matPipe = mappable.mapTo(fields)(fn)
    new Matrix[Row,Col,Val]('row, 'col, 'val, matPipe)
  }

  def toRow[Row,Val](implicit ev: <:<[T,(Row,Val)], setter: TupleSetter[(Row,Val)])
  : RowVector[Row,Val] = mapToRow { _.asInstanceOf[(Row,Val)] }

  def mapToRow[Row,Val](fn: (T) => (Row,Val))
    (implicit setter: TupleSetter[(Row,Val)], fd: FlowDef) : RowVector[Row,Val] = {
    val fields = ('row, 'val)
    val rowPipe = mappable.mapTo(fields)(fn)
    new RowVector[Row,Val]('row,'val, rowPipe)
  }

  def toCol[Col,Val](implicit ev: <:<[T,(Col,Val)], setter: TupleSetter[(Col,Val)]) : ColVector[Col,Val] =
    mapToCol { _.asInstanceOf[(Col,Val)] }

  def mapToCol[Col,Val](fn: (T) => (Col,Val))
    (implicit setter: TupleSetter[(Col,Val)]) : ColVector[Col,Val] = {
    val fields = ('col, 'val)
    val colPipe = mappable.mapTo(fields)(fn)
    new ColVector[Col,Val]('col,'val, colPipe)
  }
}

object Matrix {
  // If this function is implicit, you can use the PipeExtensions methods on pipe
  implicit def pipeExtensions[P <% Pipe](p : P) = new MatrixPipeExtensions(p)
  implicit def mappableExtensions[T](mt: Mappable[T])(implicit fd: FlowDef) =
    new MatrixMappableExtensions(mt)(fd)

  def filterOutZeros[ValT](fSym : Symbol, group : Monoid[ValT])(fpipe : Pipe) : Pipe = {
    fpipe.filter(fSym) { tup : Tuple1[ValT] => group.isNonZero(tup._1) }
  }

  def meanCenter[T](vct: Iterable[(T,Double)]) : Iterable[(T,Double)] = {
    val valList = vct.map { _._2 }
    val sum = valList.sum
    val count = valList.size
    val avg = sum / count
    vct.map { tup => (tup._1, tup._2 - avg) }
  }

  implicit def literalToScalar[ValT](v : ValT) = new LiteralScalar(v)

  // Converts to Matrix for addition
  implicit def diagonalToMatrix[RowT,ValT](diag : DiagonalMatrix[RowT,ValT]) : Matrix[RowT,RowT,ValT] = {
    val colSym = newSymbol(Set(diag.idxSym, diag.valSym), 'col)
    val newPipe = diag.pipe.map(diag.idxSym -> colSym) { (x : RowT) => x }
    new Matrix[RowT,RowT,ValT](diag.idxSym, colSym, diag.valSym, newPipe, diag.sizeHint)
  }
}

// The linear algebra objects (Matrix, *Vector, Scalar) wrap pipes and have some
// common properties.  The main common pattern is the desire to write them to sources
// without needless duplication of code.
trait WrappedPipe {
  def fields : Fields
  def pipe : Pipe
  def writePipe(src : Source, outFields : Fields = Fields.NONE)(implicit fd : FlowDef) {
    val toWrite = if (outFields.isNone) pipe else pipe.rename(fields -> outFields)
    toWrite.write(src)
  }
}

class Matrix[RowT, ColT, ValT]
  (val rowSym : Symbol, val colSym : Symbol, val valSym : Symbol,
    inPipe : Pipe, val sizeHint : SizeHint = NoClue)
  extends WrappedPipe with java.io.Serializable {
  import Matrix._
  import MatrixProduct._
  import Dsl.ensureUniqueFields
  import Dsl.getField

  //The access function for inPipe. Ensures the right order of: row,col,val
  lazy val pipe = inPipe.project(rowSym,colSym,valSym)
  def fields = rowColValSymbols

  def pipeAs(toFields : Fields) = pipe.rename((rowSym,colSym,valSym) -> toFields)

  def hasHint = sizeHint != NoClue

  override def hashCode = inPipe.hashCode
  override def equals(that : Any) : Boolean = {
    (that != null) && (that.isInstanceOf[Matrix[_,_,_]]) && {
      val thatM = that.asInstanceOf[Matrix[RowT,ColT,ValT]]
      (this.rowSym == thatM.rowSym) && (this.colSym == thatM.colSym) &&
      (this.valSym == thatM.valSym) && (this.pipe == thatM.pipe)
    }
  }

  // Value operations
  def mapValues[ValU](fn:(ValT) => ValU)(implicit mon : Monoid[ValU]) : Matrix[RowT,ColT,ValU] = {
    val newPipe = pipe.flatMap(valSym -> valSym) { imp : Tuple1[ValT] => //Ensure an arity of 1
      //This annoying Tuple1 wrapping ensures we can handle ValT that may itself be a Tuple.
      mon.nonZeroOption(fn(imp._1)).map { Tuple1(_) }
    }
    new Matrix[RowT,ColT,ValU](this.rowSym, this.colSym, this.valSym, newPipe, sizeHint)
  }
  /** like zipWithIndex.map but ONLY CHANGES THE VALUE not the index.
   * Note you will only see non-zero elements on the matrix. This does not enumerate the zeros
   */
  def mapWithIndex[ValNew](fn: (ValT,RowT,ColT) => ValNew)(implicit mon: Monoid[ValNew]):
    Matrix[RowT,ColT,ValNew] = {
    val newPipe = pipe.flatMap(fields -> fields) { imp : (RowT,ColT,ValT) =>
      mon.nonZeroOption(fn(imp._3, imp._1, imp._2)).map { (imp._1, imp._2, _) }
    }
    new Matrix[RowT,ColT,ValNew](rowSym, colSym, valSym, newPipe, sizeHint)
  }

  // Filter values
  def filterValues(fn : (ValT) => Boolean) : Matrix[RowT,ColT,ValT] = {
    val newPipe = pipe.filter(valSym) { imp : Tuple1[ValT] => //Ensure an arity of 1
      //This annoying Tuple1 wrapping ensures we can handle ValT that may itself be a Tuple.
      fn(imp._1)
    }
    new Matrix[RowT,ColT,ValT](this.rowSym, this.colSym, this.valSym, newPipe, sizeHint)
  }

  // Binarize values, all x != 0 become 1
  def binarizeAs[NewValT](implicit mon : Monoid[ValT], ring : Ring[NewValT]) : Matrix[RowT,ColT,NewValT] = {
    mapValues( x => if ( mon.isNonZero(x) ) { ring.one } else { ring.zero } )(ring)
  }

  // Row Operations

  // Get a specific row
  def getRow (index : RowT) : RowVector[ColT,ValT] = {
    val newPipe = inPipe
      .filter(rowSym){ input : RowT => input == index }
      .project(colSym,valSym)
    val newHint = sizeHint.setRows(1L)
    new RowVector[ColT,ValT](colSym,valSym,newPipe,newHint)
  }

  // Reduce all rows to a single row (zeros or ignored)
  def reduceRowVectors(fn: (ValT,ValT) => ValT)(implicit mon : Monoid[ValT]) : RowVector[ColT,ValT] = {
    val newPipe = filterOutZeros(valSym, mon) {
      pipe.groupBy(colSym) {
        _.reduce(valSym) { (x : Tuple1[ValT], y: Tuple1[ValT]) => Tuple1(fn(x._1,y._1)) }
          // Matrices are generally huge and cascading has problems with diverse key spaces and
          // mapside operations
          // TODO continually evaluate if this is needed to avoid OOM
          .forceToReducers
      }
    }
    val newHint = sizeHint.setRows(1L)
    new RowVector[ColT,ValT](colSym,valSym,newPipe,newHint)
  }

  // Sums all the rows per column
  def sumRowVectors(implicit mon : Monoid[ValT]) : RowVector[ColT,ValT] = {
    this.reduceRowVectors((x,y) => mon.plus(x,y))
  }

  // Maps rows using a per-row mapping function
  // Use this for non-decomposable vector processing functions
  // and with vectors that can fit in one-single machine memory
  def mapRows (fn: Iterable[(ColT,ValT)] => Iterable[(ColT,ValT)])(implicit mon : Monoid[ValT])
  : Matrix[RowT,ColT,ValT] = {
    val newListSym = Symbol(colSym.name + "_" + valSym.name + "_list")
    // TODO, I think we can count the rows/cols for free here
    val newPipe = filterOutZeros(valSym, mon) {
      pipe.groupBy(rowSym) {
        _.toList[(ColT,ValT)]((colSym,valSym) -> newListSym)
      }
      .flatMapTo( (rowSym, newListSym) -> (rowSym,colSym,valSym) ) { tup : (RowT,List[(ColT,ValT)]) =>
          val row = tup._1
          val list = fn(tup._2)
          // Now flatten out to (row, col, val):
          list.map{ imp : (ColT,ValT) => (row,imp._1,imp._2) }
      }
    }
    new Matrix[RowT,ColT,ValT](rowSym, colSym, valSym, newPipe, sizeHint)
  }


  def topRowElems( k : Int )(implicit ord : Ordering[ValT]) : Matrix[RowT,ColT,ValT] = {
    if (k < 1000) {
      topRowWithTiny(k)
    }
    else {
      val newPipe = pipe.groupBy(rowSym){ _
          .sortBy(valSym)
          .reverse
          .take(k)
        }
        .project(rowSym,colSym,valSym)
      new Matrix[RowT,ColT,ValT](rowSym, colSym, valSym, newPipe, FiniteHint(-1L,k))
    }
  }

  protected def topRowWithTiny( k : Int )(implicit ord : Ordering[ValT]) : Matrix[RowT,ColT,ValT] = {
    val topSym = Symbol(colSym.name + "_topK")
    val newPipe = pipe.groupBy(rowSym){ _
        .sortWithTake( (colSym, valSym) -> 'top_vals, k ) ( (t0 :(ColT,ValT), t1:(ColT,ValT)) => ord.gt(t0._2,t1._2) )
    }
    .flatMapTo((0,1) ->(rowSym,topSym,valSym)) { imp:(RowT,List[(ColT,ValT)]) =>
      val row = imp._1
      val list = imp._2
      list.map{ imp : (ColT,ValT) => (row,imp._1,imp._2) }
    }
    new Matrix[RowT,ColT,ValT](rowSym, topSym, valSym, newPipe, FiniteHint(-1L,k))
  }

  protected lazy val rowL1Norm = {
    val matD = this.asInstanceOf[Matrix[RowT,ColT,Double]]
    (matD.mapValues { x => x.abs }
      .sumColVectors
      .diag
      .inverse) * matD
  }
  // Row L1 normalization, only makes sense for Doubles
  // At the end of L1 normalization, sum of row values is one
  def rowL1Normalize(implicit ev : =:=[ValT,Double]) : Matrix[RowT,ColT,Double] = rowL1Norm

  protected lazy val rowL2Norm = {
    val matD = this.asInstanceOf[Matrix[RowT,ColT,Double]]
    (matD.mapValues { x => x*x }
      .sumColVectors
      .diag
      .mapValues { x => scala.math.sqrt(x) }
      .diagonal
      .inverse) * matD
  }
  // Row L2 normalization (can only be called for Double)
  // After this operation, the sum(|x|^2) along each row will be 1.
  def rowL2Normalize(implicit ev : =:=[ValT,Double]) : Matrix[RowT,ColT,Double] = rowL2Norm

  // Remove the mean of each row from each value in a row.
  // Double ValT only (only over the observed values, not dividing by the unobserved ones)
  def rowMeanCentering(implicit ev : =:=[ValT,Double]) = {
    val matD = this.asInstanceOf[Matrix[RowT,ColT,Double]]
    matD.mapRows { Matrix.meanCenter }
  }


  // Row non-zeroes, ave and standard deviation in one pass - Double ValT only
  // It produces a matrix with the same number of rows, but the cols are the three moments.
  // (moments are computed only over the observed values, not taking into account the unobserved ones)
  def rowSizeAveStdev(implicit ev : =:=[ValT,Double]) = {
    val newColSym = Symbol(colSym.name + "_newCol")
    val newValSym = Symbol(valSym.name + "_newVal")

    val newPipe = inPipe
      .groupBy(rowSym) { _.sizeAveStdev((valSym)->('size,'ave,'stdev)) }
      .flatMapTo( (rowSym,'size,'ave,'stdev) -> (rowSym,newColSym,newValSym) ) { tup : (RowT,Long,Double,Double) =>
          val row = tup._1
          val size = tup._2.toDouble
          val avg = tup._3
          val stdev = tup._4
          List((row,1,size),(row,2,avg),(row,3,stdev))
      }
    val newHint = sizeHint.setCols(3L)
    new Matrix[RowT,Int,Double](rowSym, newColSym, newValSym, newPipe, newHint)
  }

  def rowColValSymbols : Fields = (rowSym, colSym, valSym)

  // Column operations - see Row operations above

  def getCol (index : ColT) : ColVector[RowT,ValT] = {
    this.transpose.getRow(index).transpose
  }

  def reduceColVectors (fn: (ValT,ValT) => ValT)( implicit mon: Monoid[ValT] ) : ColVector[RowT,ValT] = {
    this.transpose.reduceRowVectors(fn)(mon).transpose
  }

  def sumColVectors( implicit mon : Monoid[ValT] ) : ColVector[RowT,ValT] = {
    this.transpose.sumRowVectors(mon).transpose
  }

  def mapCols(fn: Iterable[(RowT,ValT)] => Iterable[(RowT,ValT)])( implicit mon : Monoid[ValT] ) : Matrix[RowT,ColT,ValT] = {
    this.transpose.mapRows(fn)(mon).transpose
  }

  def topColElems( k : Int )(implicit ord : Ordering[ValT]) : Matrix[RowT,ColT,ValT] = {
    this.transpose.topRowElems(k)(ord).transpose
  }

  def colL1Normalize(implicit ev : =:=[ValT,Double]) = {
    this.transpose.rowL1Normalize.transpose
  }

  def colL2Normalize(implicit ev : =:=[ValT,Double]) = {
    this.transpose.rowL2Normalize.transpose
  }

  def colMeanCentering(implicit ev : =:=[ValT,Double]) = {
    this.transpose.rowMeanCentering.transpose
  }

  def colSizeAveStdev(implicit ev : =:=[ValT,Double]) = {
    this.transpose.rowSizeAveStdev
  }

  def *[That,Res](that : That)(implicit prod : MatrixProduct[Matrix[RowT,ColT,ValT],That,Res]) : Res = {
    prod(this, that)
  }

  def /(that : LiteralScalar[ValT])(implicit field : Field[ValT]) = {
    field.assertNotZero(that.value)
    mapValues(elem => field.div(elem, that.value))(field)
  }

  def /(that : Scalar[ValT])(implicit field : Field[ValT]) = {
    nonZerosWith(that)
      .mapValues({leftRight : (ValT,ValT) =>
        val (left, right) = leftRight
        field.div(left, right)
      })(field)
  }

  // Between Matrix value reduction - Generalizes matrix addition with an arbitrary value aggregation function
  // It assumes that the function fn(0,0) = 0
  // This function assumes only one value in each matrix for a given row and column index. (no stacking of operations yet)
  // TODO: Optimize this later and be lazy on groups and joins.
  def elemWiseOp(that : Matrix[RowT,ColT,ValT])(fn : (ValT,ValT) => ValT)(implicit mon : Monoid[ValT])
    : Matrix[RowT,ColT,ValT] = {
    // If the following is not true, it's not clear this is meaningful
    // assert(mon.isZero(fn(mon.zero,mon.zero)), "f is illdefined")
    zip(that).mapValues({ pair => fn(pair._1, pair._2) })(mon)
  }

  // Matrix summation
  def +(that : Matrix[RowT,ColT,ValT])(implicit mon : Monoid[ValT]) : Matrix[RowT,ColT,ValT] = {
    if (equals(that)) {
      // No need to do any groupBy operation
      mapValues { v => mon.plus(v,v) }(mon)
    }
    else {
      elemWiseOp(that)((x,y) => mon.plus(x,y))(mon)
    }
  }

  // Matrix difference
  def -(that : Matrix[RowT,ColT,ValT])(implicit grp : Group[ValT]) : Matrix[RowT,ColT,ValT] = {
    elemWiseOp(that)((x,y) => grp.minus(x,y))(grp)
  }

  // Matrix elementwise product / Hadamard product
  // see http://en.wikipedia.org/wiki/Hadamard_product_(matrices)
  def hProd(mat: Matrix[RowT,ColT,ValT])(implicit ring : Ring[ValT]) : Matrix[RowT,ColT,ValT] = {
    elemWiseOp(mat)((x,y) => ring.times(x,y))(ring)
  }

  /** Considering the matrix as a graph, propagate the column:
   * Does the calculation: \sum_{j where M(i,j) == true) c_j
   */
  def propagate[ColValT](vec: ColVector[ColT,ColValT])(implicit ev: =:=[ValT,Boolean], monT: Monoid[ColValT])
    : ColVector[RowT,ColValT] = {
    //This cast will always succeed:
    val boolMat = this.asInstanceOf[Matrix[RowT,ColT,Boolean]]
    boolMat.zip(vec.transpose)
      .mapValues { boolT => if (boolT._1) boolT._2 else monT.zero }
      .sumColVectors
  }

  // Compute the sum of the main diagonal.  Only makes sense cases where the row and col type are
  // equal
  def trace(implicit mon : Monoid[ValT], ev : =:=[RowT,ColT]) : Scalar[ValT] = {
    diagonal.trace(mon)
  }

  // Compute the sum of all the elements in the matrix
  def sum(implicit mon : Monoid[ValT]) : Scalar[ValT] = {
    sumRowVectors.sum
  }

  def transpose : Matrix[ColT, RowT, ValT] = {
    new Matrix[ColT,RowT,ValT](colSym, rowSym, valSym, inPipe, sizeHint.transpose)
  }

  // This should only be called by def diagonal, which verifies that RowT == ColT
  protected lazy val mainDiagonal : DiagonalMatrix[RowT,ValT] = {
    val diagPipe = pipe.filter(rowSym, colSym) { input : (RowT, RowT) =>
        (input._1 == input._2)
      }
      .project(rowSym, valSym)
    new DiagonalMatrix[RowT,ValT](rowSym, valSym, diagPipe, SizeHint.asDiagonal(sizeHint))
  }
  // This method will only work if the row type and column type are the same
  // the type constraint below means there is evidence that RowT and ColT are
  // the same type
  def diagonal(implicit ev : =:=[RowT,ColT]) = mainDiagonal

  /*
   * This just removes zeros after the join inside a zip
   */
  private def cleanUpZipJoin[ValU](otherVSym : Fields, pairMonoid : Monoid[(ValT,ValU)])(joinedPipe : Pipe)
    : Pipe = {
    joinedPipe
      //Make sure the zeros are set correctly:
      .map(valSym -> valSym) { (x : ValT) =>
        if (null == x) pairMonoid.zero._1 else x
      }
      .map(otherVSym -> otherVSym) { (x : ValU) =>
        if (null == x) pairMonoid.zero._2 else x
      }
      //Put the pair into a single item, ugly in scalding sadly...
      .map(valSym.append(otherVSym) -> valSym) { tup : (ValT,ValU) => Tuple1(tup) }
      .project(rowColValSymbols)
  }

  /*
   * This ensures both side rows and columns have correct indexes (fills in nulls from the other side
   * in the case of outerjoins)
   */
  private def cleanUpIndexZipJoin(fields : Fields, joinedPipe : RichPipe)
    : Pipe = {

      def anyRefOr( tup : (AnyRef, AnyRef)) : (AnyRef, AnyRef) =  {
        val newRef = Option(tup._1).getOrElse(tup._2)
        (newRef, newRef)
      }

      joinedPipe
        .map(fields -> fields) { tup : (AnyRef, AnyRef) => anyRefOr(tup) }
  }

  // Similar to zip, but combine the scalar on the right with all non-zeros in this matrix:
  def nonZerosWith[ValU](that : Scalar[ValU]) : Matrix[RowT,ColT,(ValT,ValU)] = {
    val (newRFields, newRPipe) = ensureUniqueFields(rowColValSymbols, that.valSym, that.pipe)
    val newPipe = inPipe.crossWithTiny(newRPipe)
      .map(valSym.append(getField(newRFields, 0)) -> valSym) { leftRight : (ValT, ValU) => Tuple1(leftRight) }
      .project(rowColValSymbols)
    new Matrix[RowT,ColT,(ValT,ValU)](rowSym, colSym, valSym, newPipe, sizeHint)
  }

  // Similar to zip, but combine the scalar on the right with all non-zeros in this matrix:
  def nonZerosWith[ValU](that : LiteralScalar[ValU]) : Matrix[RowT,ColT,(ValT,ValU)] = {
    val newPipe = inPipe.map(valSym -> valSym) { left : Tuple1[ValT] =>
        Tuple1((left._1, that.value))
      }
      .project(rowColValSymbols)
    new Matrix[RowT,ColT,(ValT,ValU)](rowSym, colSym, valSym, newPipe, sizeHint)
  }

  // Override the size hint
  def withSizeHint(sh : SizeHint) : Matrix[RowT,ColT,ValT] = {
    new Matrix[RowT,ColT,ValT](rowSym, colSym, valSym, pipe, sh)
  }

  // Zip the given row with all the rows of the matrix
  def zip[ValU](that : ColVector[RowT,ValU])(implicit pairMonoid : Monoid[(ValT,ValU)])
    : Matrix[RowT,ColT,(ValT,ValU)] = {
    val (newRFields, newRPipe) = ensureUniqueFields(rowColValSymbols, (that.rowS, that.valS), that.pipe)
    // we must do an outer join to preserve zeros on one side or the other.
    // joinWithTiny can't do outer.  And since the number
    // of values for each key is 1,2 it doesn't matter if we do joinWithSmaller or Larger:
    // TODO optimize the number of reducers
    val zipped = cleanUpZipJoin(getField(newRFields, 1), pairMonoid) {
      pipe
        .joinWithSmaller(rowSym -> getField(newRFields, 0), newRPipe, new OuterJoin)
        .then{ p : RichPipe => cleanUpIndexZipJoin(rowSym.append(getField(newRFields, 0)),p) }
    }
    new Matrix[RowT,ColT,(ValT,ValU)](rowSym, colSym, valSym, zipped, sizeHint + that.sizeH)
  }
  // Zip the given row with all the rows of the matrix
  def zip[ValU](that : RowVector[ColT,ValU])(implicit pairMonoid : Monoid[(ValT,ValU)])
    : Matrix[RowT,ColT,(ValT,ValU)] = {
    val (newRFields, newRPipe) = ensureUniqueFields(rowColValSymbols, (that.colS, that.valS), that.pipe)
    // we must do an outer join to preserve zeros on one side or the other.
    // joinWithTiny can't do outer.  And since the number
    // of values for each key is 1,2 it doesn't matter if we do joinWithSmaller or Larger:
    // TODO optimize the number of reducers
    val zipped = cleanUpZipJoin(getField(newRFields, 1), pairMonoid) {
      pipe
        .joinWithSmaller(colSym -> getField(newRFields, 0), newRPipe, new OuterJoin)
        .then{ p : RichPipe => cleanUpIndexZipJoin(colSym.append(getField(newRFields, 0)),p) }
    }
    new Matrix[RowT,ColT,(ValT,ValU)](rowSym, colSym, valSym, zipped, sizeHint + that.sizeH)
  }

  // This creates the matrix with pairs for the entries
  def zip[ValU](that : Matrix[RowT,ColT,ValU])(implicit pairMonoid : Monoid[(ValT,ValU)])
    : Matrix[RowT,ColT,(ValT,ValU)] = {
    val (newRFields, newRPipe) = ensureUniqueFields(rowColValSymbols, that.rowColValSymbols, that.pipe)
    // we must do an outer join to preserve zeros on one side or the other.
    // joinWithTiny can't do outer.  And since the number
    // of values for each key is 1,2 it doesn't matter if we do joinWithSmaller or Larger:
    // TODO optimize the number of reducers
    val zipped = cleanUpZipJoin[ValU](getField(newRFields, 2), pairMonoid) {
      pipe
        .joinWithSmaller((rowSym, colSym) ->
          (getField(newRFields, 0).append(getField(newRFields, 1))),
          newRPipe, new OuterJoin)
        .then{ p : RichPipe => cleanUpIndexZipJoin(rowSym.append(getField(newRFields,0)),p) }
        .then{ p : RichPipe => cleanUpIndexZipJoin(colSym.append(getField(newRFields,1)),p) }
    }
    new Matrix[RowT,ColT,(ValT,ValU)](rowSym, colSym, valSym, zipped, sizeHint + that.sizeHint)
  }

  /** Write the matrix, optionally renaming row,col,val fields to the given fields
   * then return this.
   */
  def write(src : Source, outFields : Fields = Fields.NONE)(implicit fd : FlowDef)
    : Matrix[RowT,ColT,ValT] = {
    writePipe(src, outFields)
    this
  }
}

class LiteralScalar[ValT](val value : ValT) extends java.io.Serializable {
  def *[That,Res](that : That)(implicit prod : MatrixProduct[LiteralScalar[ValT],That,Res]) : Res
    = { prod(this, that) }
}

class Scalar[ValT](val valSym : Symbol, inPipe : Pipe) extends WrappedPipe with java.io.Serializable {
  def pipe = inPipe
  def fields = valSym
  def *[That,Res](that : That)(implicit prod : MatrixProduct[Scalar[ValT],That,Res]) : Res
    = { prod(this, that) }
  /** Write the Scalar, optionally renaming val fields to the given fields
   * then return this.
   */
  def write(src : Source, outFields : Fields = Fields.NONE)(implicit fd : FlowDef) = {
    writePipe(src, outFields)
    this
  }
}

class DiagonalMatrix[IdxT,ValT](val idxSym : Symbol,
  val valSym : Symbol, inPipe : Pipe, val sizeHint : SizeHint)
  extends WrappedPipe {

  def *[That,Res](that : That)(implicit prod : MatrixProduct[DiagonalMatrix[IdxT,ValT],That,Res]) : Res
    = { prod(this, that) }

  def pipe = inPipe
  def fields = (idxSym, valSym)
  def trace(implicit mon : Monoid[ValT]) : Scalar[ValT] = {
    val scalarPipe = inPipe.groupAll {
      _.reduce(valSym -> valSym) { (left : Tuple1[ValT], right : Tuple1[ValT]) =>
        Tuple1(mon.plus(left._1, right._1))
      }
    }
    new Scalar[ValT](valSym, scalarPipe)
  }
  def toCol : ColVector[IdxT,ValT] = {
    new ColVector[IdxT,ValT](idxSym, valSym, inPipe, sizeHint.setRows(1L))
  }
  def toRow : RowVector[IdxT,ValT] = {
    new RowVector[IdxT,ValT](idxSym, valSym, inPipe, sizeHint.setCols(1L))
  }
  // Inverse of this matrix *IGNORING ZEROS*
  def inverse(implicit field : Field[ValT]) : DiagonalMatrix[IdxT, ValT] = {
    val diagPipe = inPipe.flatMap(valSym -> valSym) { element : ValT =>
        field.nonZeroOption(element)
          .map { field.inverse }
      }
    new DiagonalMatrix[IdxT,ValT](idxSym, valSym, diagPipe, sizeHint)
  }
  /** Write optionally renaming val fields to the given fields
   * then return this.
   */
  def write(src : Source, outFields : Fields = Fields.NONE)(implicit fd : FlowDef) = {
    writePipe(src, outFields)
    this
  }
}

class RowVector[ColT,ValT] (val colS:Symbol, val valS:Symbol, inPipe: Pipe, val sizeH: SizeHint = FiniteHint(1L, -1L))
  extends java.io.Serializable with WrappedPipe {

  def pipe = inPipe.project(colS,valS)
  def fields = (colS,valS)

  def *[That,Res](that : That)(implicit prod : MatrixProduct[RowVector[ColT,ValT],That,Res]) : Res
    = { prod(this, that) }

  def transpose : ColVector[ColT,ValT] = {
    new ColVector[ColT,ValT](colS, valS, inPipe, sizeH.transpose)
  }

  def diag : DiagonalMatrix[ColT,ValT] = {
    val newHint = SizeHint.asDiagonal(sizeH.setRowsToCols)
    new DiagonalMatrix[ColT,ValT](colS, valS, inPipe, newHint)
  }

  /** like zipWithIndex.map but ONLY CHANGES THE VALUE not the index.
   * Note you will only see non-zero elements on the vector. This does not enumerate the zeros
   */
  def mapWithIndex[ValNew](fn: (ValT,ColT) => ValNew)(implicit mon: Monoid[ValNew]):
    RowVector[ColT,ValNew] = {
    val newPipe = pipe.mapTo((valS,colS) -> (valS,colS)) { tup: (ValT,ColT) => (fn(tup._1, tup._2), tup._2) }
      .filter(valS) { (v: ValNew) => mon.isNonZero(v) }
    new RowVector(colS, valS, newPipe, sizeH)
  }

  /** Do a right-propogation of a row, transpose of Matrix.propagate
   */
  def propagate[MatColT](mat: Matrix[ColT,MatColT,Boolean])(implicit monT: Monoid[ValT])
    : RowVector[MatColT,ValT] = {
    mat.transpose.propagate(this.transpose).transpose
  }

  def sum(implicit mon : Monoid[ValT]) : Scalar[ValT] = {
    val scalarPipe = pipe.groupAll{ _.reduce(valS -> valS) { (left : Tuple1[ValT], right : Tuple1[ValT]) =>
        Tuple1(mon.plus(left._1, right._1))
      }
    }
    new Scalar[ValT](valS, scalarPipe)
  }

  def topElems( k : Int )(implicit ord : Ordering[ValT]) : RowVector[ColT,ValT] = {
    // TODO this should be tunable:
    if (k < 1000) { topWithTiny(k) }
    else {
      val fieldName = valS.toString
      val ordValS = new Fields(fieldName)
      ordValS.setComparator(fieldName, ord)

      val newPipe = pipe.groupAll{ _
          .sortBy(ordValS)
          .reverse
          .take(k)
        }.project(colS,valS)
      new RowVector[ColT,ValT](colS, valS, newPipe, sizeH.setCols(k).setRows(1L))
    }
  }

  protected def topWithTiny( k : Int )(implicit ord : Ordering[ValT]) : RowVector[ColT,ValT] = {
    val topSym = Symbol(colS.name + "_topK")
    val newPipe = pipe.groupAll{ _
        .sortWithTake( (colS, valS) -> 'top_vals, k ) ( (t0 :(ColT,ValT), t1:(ColT,ValT)) => ord.gt(t0._2,t1._2) )
    }
    .flatMap('top_vals ->(topSym, valS)) { imp:List[(ColT,ValT)] => imp }
    new RowVector[ColT,ValT](topSym, valS, newPipe, sizeH.setCols(k).setRows(1L))
  }

  def toMatrix[RowT](rowId : RowT) : Matrix[RowT,ColT,ValT] = {
    val rowSym = newSymbol(Set(colS, valS), 'row) //Matrix.newSymbol(Set(colS, valS), 'row)
    val newPipe = inPipe.map(() -> rowSym){ u: Unit => rowId }
      .project(rowSym, colS, valS)
    new Matrix[RowT,ColT,ValT](rowSym, colS, valS, newPipe, sizeH.setRows(1L))
  }

  // Override the size hint
  def withColsHint(cols : Long) : RowVector[ColT,ValT] = {
    new RowVector[ColT,ValT](colS, valS, pipe, sizeH.setRows(1L).setCols(cols))
  }

  /** Write optionally renaming val fields to the given fields
   * then return this.
   */
  def write(src : Source, outFields : Fields = Fields.NONE)(implicit fd : FlowDef) = {
    writePipe(src, outFields)
    this
  }
}

class ColVector[RowT,ValT] (val rowS:Symbol, val valS:Symbol, inPipe : Pipe, val sizeH: SizeHint = FiniteHint(-1L, 1L))
  extends java.io.Serializable with WrappedPipe {

  def pipe = inPipe.project(rowS,valS)
  def fields = (rowS,valS)

  def *[That,Res](that : That)(implicit prod : MatrixProduct[ColVector[RowT,ValT],That,Res]) : Res
    = { prod(this, that) }

  def transpose : RowVector[RowT,ValT] = {
    new RowVector[RowT,ValT](rowS, valS, inPipe, sizeH.transpose)
  }

  def diag : DiagonalMatrix[RowT,ValT] = {
    val newHint = SizeHint.asDiagonal(sizeH.setRowsToCols)
    new DiagonalMatrix[RowT,ValT](rowS, valS, inPipe, newHint)
  }

  /** like zipWithIndex.map but ONLY CHANGES THE VALUE not the index.
   * Note you will only see non-zero elements on the vector. This does not enumerate the zeros
   */
  def mapWithIndex[ValNew](fn: (ValT,RowT) => ValNew)(implicit mon: Monoid[ValNew]):
    ColVector[RowT,ValNew] = transpose.mapWithIndex(fn).transpose

  def sum(implicit mon : Monoid[ValT]) : Scalar[ValT] = {
    val scalarPipe = pipe.groupAll{ _.reduce(valS -> valS) { (left : Tuple1[ValT], right : Tuple1[ValT]) =>
        Tuple1(mon.plus(left._1, right._1))
      }
    }
    new Scalar[ValT](valS, scalarPipe)
  }

  def topElems( k : Int )(implicit ord : Ordering[ValT]) : ColVector[RowT,ValT] = {
    if (k < 1000) { topWithTiny(k) }
    else {
      val newPipe = pipe.groupAll{ _
          .sortBy(valS)
          .reverse
          .take(k)
        }.project(rowS,valS)
      new ColVector[RowT,ValT](rowS, valS, newPipe, sizeH.setCols(1L).setRows(k))
    }
  }

  protected def topWithTiny( k : Int )(implicit ord : Ordering[ValT]) : ColVector[RowT,ValT] = {
    val topSym = Symbol(rowS.name + "_topK")
    val newPipe = pipe.groupAll{ _
        .sortWithTake( (rowS, valS) -> 'top_vals, k ) ( (t0 :(RowT,ValT), t1:(RowT,ValT)) => ord.gt(t0._2,t1._2) )
    }
    .flatMap('top_vals ->(topSym, valS)) { imp:List[(RowT,ValT)] => imp }
    new ColVector[RowT,ValT](topSym, valS, newPipe, sizeH.setCols(1L).setRows(k))
  }

  def toMatrix[ColT](colIdx : ColT) : Matrix[RowT,ColT,ValT] = {
    val colSym = newSymbol(Set(rowS, valS), 'col) //Matrix.newSymbol(Set(rowS, valS), 'col)
    val newPipe = inPipe.map(() -> colSym){ u:Unit => colIdx }
      .project(rowS, colSym, valS)
    new Matrix[RowT,ColT,ValT](rowS, colSym, valS, newPipe, sizeH.setCols(1L))
  }

  // Override the size hint
  def withRowsHint(rows : Long) : ColVector[RowT,ValT] = {
    new ColVector[RowT,ValT](rowS, valS, pipe, sizeH.setRows(rows).setCols(1L))
  }

  /** Write optionally renaming val fields to the given fields
   * then return this.
   */
  def write(src : Source, outFields : Fields = Fields.NONE)(implicit fd : FlowDef) = {
    writePipe(src, outFields)
    this
  }
}
