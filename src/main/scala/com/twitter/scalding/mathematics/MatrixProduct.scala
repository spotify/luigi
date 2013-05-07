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

/**
 * Handles the implementation of various versions of MatrixProducts
 */

import com.twitter.algebird.{Ring,Monoid,Group,Field}
import com.twitter.scalding.RichPipe
import com.twitter.scalding.Dsl._

import cascading.pipe.Pipe
import cascading.tuple.Fields

import scala.math.Ordering
import scala.annotation.tailrec

/** Abstracts the approach taken to join the two matrices
 */
abstract class MatrixJoiner extends java.io.Serializable {
  def apply(left : Pipe, joinFields : (Fields,Fields), right : Pipe) : Pipe
}

case object AnyToTiny extends MatrixJoiner {
  override def apply(left : Pipe, joinFields : (Fields,Fields), right : Pipe) : Pipe = {
    RichPipe(left).joinWithTiny(joinFields, right)
  }
}
case object BigToSmall extends MatrixJoiner {
  override def apply(left : Pipe, joinFields : (Fields,Fields), right : Pipe) : Pipe = {
    RichPipe(left).joinWithSmaller(joinFields, right)
  }
}

case object TinyToAny extends MatrixJoiner {
  override def apply(left : Pipe, joinFields : (Fields,Fields), right : Pipe) : Pipe = {
    val reversed = (joinFields._2, joinFields._1)
    RichPipe(right).joinWithTiny(reversed, left)
  }
}
case object SmallToBig extends MatrixJoiner {
  override def apply(left : Pipe, joinFields : (Fields,Fields), right : Pipe) : Pipe = {
    RichPipe(left).joinWithLarger(joinFields, right)
  }
}

trait MatrixProduct[Left,Right,Result] extends java.io.Serializable {
  def apply(left : Left, right : Right) : Result
}

/**
 * TODO: Muliplication is the expensive stuff.  We need to optimize the methods below:
 * This object holds the implicits to handle matrix products between various types
 */
object MatrixProduct extends java.io.Serializable {
  // These are VARS, so you can set them before you start:
  var maxTinyJoin = 100000L // Bigger than this, and we use joinWithSmaller
  var maxReducers = 200

  def getJoiner(leftSize : SizeHint, rightSize : SizeHint) : MatrixJoiner = {
    if (SizeHintOrdering.lteq(leftSize, rightSize)) {
      // If leftsize is definite:
      leftSize.total.map { t => if (t < maxTinyJoin) TinyToAny else SmallToBig }
        // Else just assume the right is smaller, but both are unknown:
        .getOrElse(BigToSmall)
    }
    else {
      // left > right
      rightSize.total.map { rs =>
        if (rs < maxTinyJoin) AnyToTiny else BigToSmall
      }.getOrElse(BigToSmall)
    }
  }

  implicit def literalScalarRightProduct[Row,Col,ValT](implicit ring : Ring[ValT]) :
    MatrixProduct[Matrix[Row,Col,ValT],LiteralScalar[ValT],Matrix[Row,Col,ValT]] =
    new MatrixProduct[Matrix[Row,Col,ValT],LiteralScalar[ValT],Matrix[Row,Col,ValT]] {
      def apply(left : Matrix[Row,Col,ValT], right : LiteralScalar[ValT]) = {
        val newPipe = left.pipe.map(left.valSym -> left.valSym) { (v : ValT) =>
          ring.times(v, right.value)
        }
        new Matrix[Row,Col,ValT](left.rowSym, left.colSym, left.valSym, newPipe, left.sizeHint)
      }
    }

  implicit def literalRightProduct[Row,Col,ValT](implicit ring : Ring[ValT]) :
    MatrixProduct[Matrix[Row,Col,ValT],ValT,Matrix[Row,Col,ValT]] =
    new MatrixProduct[Matrix[Row,Col,ValT],ValT,Matrix[Row,Col,ValT]] {
      def apply(left : Matrix[Row,Col,ValT], right : ValT) = {
        val newPipe = left.pipe.map(left.valSym -> left.valSym) { (v : ValT) =>
          ring.times(v, right)
        }
        new Matrix[Row,Col,ValT](left.rowSym, left.colSym, left.valSym, newPipe, left.sizeHint)
      }
    }


  implicit def literalScalarLeftProduct[Row,Col,ValT](implicit ring : Ring[ValT]) :
    MatrixProduct[LiteralScalar[ValT],Matrix[Row,Col,ValT],Matrix[Row,Col,ValT]] =
    new MatrixProduct[LiteralScalar[ValT],Matrix[Row,Col,ValT],Matrix[Row,Col,ValT]] {
      def apply( left : LiteralScalar[ValT], right : Matrix[Row,Col,ValT]) = {
        val newPipe = right.pipe.map(right.valSym -> right.valSym) { (v : ValT) =>
          ring.times(left.value, v)
        }
        new Matrix[Row,Col,ValT](right.rowSym, right.colSym, right.valSym, newPipe, right.sizeHint)
      }
    }

  implicit def scalarPipeRightProduct[Row,Col,ValT](implicit ring : Ring[ValT]) :
    MatrixProduct[Matrix[Row,Col,ValT],Scalar[ValT],Matrix[Row,Col,ValT]] =
    new MatrixProduct[Matrix[Row,Col,ValT],Scalar[ValT],Matrix[Row,Col,ValT]] {
      def apply(left : Matrix[Row,Col,ValT], right : Scalar[ValT]) = {
        left.nonZerosWith(right).mapValues({leftRight =>
          val (left, right) = leftRight
          ring.times(left, right)
        })(ring)
      }
    }

  implicit def scalarPipeLeftProduct[Row,Col,ValT](implicit ring : Ring[ValT]) :
    MatrixProduct[Scalar[ValT],Matrix[Row,Col,ValT],Matrix[Row,Col,ValT]] =
    new MatrixProduct[Scalar[ValT],Matrix[Row,Col,ValT],Matrix[Row,Col,ValT]] {
      def apply(left : Scalar[ValT], right : Matrix[Row,Col,ValT]) = {
        right.nonZerosWith(left).mapValues({matScal =>
          val (matVal, scalarVal) = matScal
          ring.times(scalarVal, matVal)
        })(ring)
      }
    }

  implicit def rowColProduct[IdxT,ValT](implicit ring : Ring[ValT]) :
    MatrixProduct[RowVector[IdxT,ValT],ColVector[IdxT,ValT],Scalar[ValT]] =
    new MatrixProduct[RowVector[IdxT,ValT],ColVector[IdxT,ValT],Scalar[ValT]] {
      def apply(left : RowVector[IdxT,ValT], right : ColVector[IdxT,ValT]) : Scalar[ValT] = {
        // Normal matrix multiplication works here, but we need to convert to a Scalar
        val prod = (left.toMatrix(0) * right.toMatrix(0)) : Matrix[Int,Int,ValT]
        new Scalar[ValT](prod.valSym, prod.pipe.project(prod.valSym))
      }
    }

  implicit def standardMatrixProduct[RowL,Common,ColR,ValT](implicit ring : Ring[ValT]) :
    MatrixProduct[Matrix[RowL,Common,ValT],Matrix[Common,ColR,ValT],Matrix[RowL,ColR,ValT]] =
    new MatrixProduct[Matrix[RowL,Common,ValT],Matrix[Common,ColR,ValT],Matrix[RowL,ColR,ValT]] {
      def apply(left : Matrix[RowL,Common,ValT], right : Matrix[Common,ColR,ValT]) = {
        val (newRightFields, newRightPipe) = ensureUniqueFields(
          (left.rowSym,left.colSym,left.valSym),
          (right.rowSym, right.colSym, right.valSym),
          right.pipe
        )
        val newHint = left.sizeHint * right.sizeHint
        // Hint of groupBy reducer size
        val grpReds = newHint.total.map { tot =>
          // + 1L is to make sure there is at least once reducer
          (tot / MatrixProduct.maxTinyJoin + 1L).toInt min MatrixProduct.maxReducers
        }.getOrElse(-1) //-1 means use the default number

        val productPipe = Matrix.filterOutZeros(left.valSym, ring) {
          getJoiner(left.sizeHint, right.sizeHint)
            // TODO: we should use the size hints to set the number of reducers:
            .apply(left.pipe, (left.colSym -> getField(newRightFields, 0)), newRightPipe)
            // Do the product:
            .map((left.valSym.append(getField(newRightFields, 2))) -> left.valSym) { pair : (ValT,ValT) =>
              ring.times(pair._1, pair._2)
            }
            .groupBy(left.rowSym.append(getField(newRightFields, 1))) {
              // We should use the size hints to set the number of reducers here
              _.reduce(left.valSym) { (x: Tuple1[ValT], y: Tuple1[ValT]) => Tuple1(ring.plus(x._1, y._1)) }
                // There is a low chance that many (row,col) keys are co-located, and the keyspace
                // is likely huge, just push to reducers
                .forceToReducers
                .reducers(grpReds)
            }
          }
          // Keep the names from the left:
          .rename(getField(newRightFields, 1) -> left.colSym)
        new Matrix[RowL,ColR,ValT](left.rowSym, left.colSym, left.valSym, productPipe, newHint)
      }
  }

  implicit def diagMatrixProduct[RowT,ColT,ValT](implicit ring : Ring[ValT]) :
    MatrixProduct[DiagonalMatrix[RowT,ValT],Matrix[RowT,ColT,ValT],Matrix[RowT,ColT,ValT]] =
    new MatrixProduct[DiagonalMatrix[RowT,ValT],Matrix[RowT,ColT,ValT],Matrix[RowT,ColT,ValT]] {
      def apply(left : DiagonalMatrix[RowT,ValT], right : Matrix[RowT,ColT,ValT]) = {
        val (newRightFields, newRightPipe) = ensureUniqueFields(
          (left.idxSym, left.valSym),
          (right.rowSym, right.colSym, right.valSym),
          right.pipe
        )
        val newHint = left.sizeHint * right.sizeHint
        val productPipe = Matrix.filterOutZeros(right.valSym, ring) {
          getJoiner(left.sizeHint, right.sizeHint)
            // TODO: we should use the size hints to set the number of reducers:
            .apply(left.pipe, (left.idxSym -> getField(newRightFields, 0)), newRightPipe)
            // Do the product:
            .map((left.valSym.append(getField(newRightFields, 2))) -> getField(newRightFields,2)) { pair : (ValT,ValT) =>
              ring.times(pair._1, pair._2)
            }
            // Keep the names from the right:
            .project(newRightFields)
            .rename(newRightFields -> (right.rowSym, right.colSym, right.valSym))
          }
        new Matrix[RowT,ColT,ValT](right.rowSym, right.colSym, right.valSym, productPipe, newHint)
      }
    }

  implicit def matrixDiagProduct[RowT,ColT,ValT](implicit ring : Ring[ValT]) :
    MatrixProduct[Matrix[RowT,ColT,ValT],DiagonalMatrix[ColT,ValT],Matrix[RowT,ColT,ValT]] =
    new MatrixProduct[Matrix[RowT,ColT,ValT],DiagonalMatrix[ColT,ValT],Matrix[RowT,ColT,ValT]] {
      def apply(left : Matrix[RowT,ColT,ValT], right : DiagonalMatrix[ColT,ValT]) = {
        // (A * B) = (B^T * A^T)^T
        // note diagonal^T = diagonal
        (right * (left.transpose)).transpose
      }
    }

  implicit def diagDiagProduct[IdxT,ValT](implicit ring : Ring[ValT]) :
    MatrixProduct[DiagonalMatrix[IdxT,ValT],DiagonalMatrix[IdxT,ValT],DiagonalMatrix[IdxT,ValT]] =
    new MatrixProduct[DiagonalMatrix[IdxT,ValT],DiagonalMatrix[IdxT,ValT],DiagonalMatrix[IdxT,ValT]] {
      def apply(left : DiagonalMatrix[IdxT,ValT], right : DiagonalMatrix[IdxT,ValT]) = {
        val (newRightFields, newRightPipe) = ensureUniqueFields(
          (left.idxSym, left.valSym),
          (right.idxSym, right.valSym),
          right.pipe
        )
        val newHint = left.sizeHint * right.sizeHint
        val productPipe = Matrix.filterOutZeros(left.valSym, ring) {
          getJoiner(left.sizeHint, right.sizeHint)
            // TODO: we should use the size hints to set the number of reducers:
            .apply(left.pipe, (left.idxSym -> getField(newRightFields, 0)), newRightPipe)
            // Do the product:
            .map((left.valSym.append(getField(newRightFields, 1))) -> left.valSym) { pair : (ValT,ValT) =>
              ring.times(pair._1, pair._2)
            }
          }
          // Keep the names from the left:
          .project(left.idxSym, left.valSym)
        new DiagonalMatrix[IdxT,ValT](left.idxSym, left.valSym, productPipe, newHint)
      }
    }

  implicit def diagColProduct[IdxT,ValT](implicit ring : Ring[ValT]) :
    MatrixProduct[DiagonalMatrix[IdxT,ValT],ColVector[IdxT,ValT],ColVector[IdxT,ValT]] =
    new MatrixProduct[DiagonalMatrix[IdxT,ValT],ColVector[IdxT,ValT],ColVector[IdxT,ValT]] {
      def apply(left : DiagonalMatrix[IdxT,ValT], right : ColVector[IdxT,ValT]) = {
        (left * (right.diag)).toCol
      }
    }
  implicit def rowDiagProduct[IdxT,ValT](implicit ring : Ring[ValT]) :
    MatrixProduct[RowVector[IdxT,ValT],DiagonalMatrix[IdxT,ValT],RowVector[IdxT,ValT]] =
    new MatrixProduct[RowVector[IdxT,ValT],DiagonalMatrix[IdxT,ValT],RowVector[IdxT,ValT]] {
      def apply(left : RowVector[IdxT,ValT], right : DiagonalMatrix[IdxT,ValT]) = {
        ((left.diag) * right).toRow
      }
    }
}
