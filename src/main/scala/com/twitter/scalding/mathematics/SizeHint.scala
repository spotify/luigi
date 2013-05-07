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

object SizeHint {
  implicit val ordering = SizeHintOrdering
  // Return a sparsity assuming all the diagonal is present, but nothing else
  def asDiagonal(h : SizeHint) : SizeHint = {
    def make(r : Long, c : Long) = {
      h.total.map { tot =>
        val maxElements = (r min c)
        val sparsity = 1.0 / maxElements
        SparseHint(sparsity, maxElements, maxElements)
      }.getOrElse(NoClue)
    }
    h match {
      case NoClue => NoClue
      case FiniteHint(r,c) => make(r,c)
      case SparseHint(sp,r,c) => make(r,c)
    }
  }
}

sealed abstract class SizeHint {
  def * (other : SizeHint) : SizeHint
  def + (other : SizeHint) : SizeHint
  def total : Option[Long]
  def setCols(cols : Long) : SizeHint
  def setRows(rows : Long) : SizeHint
  def setColsToRows : SizeHint
  def setRowsToCols : SizeHint
  def transpose : SizeHint
}

// If we have no idea, we still don't have any idea, this is like NaN
case object NoClue extends SizeHint {
  def * (other : SizeHint) = NoClue
  def + (other : SizeHint) = NoClue
  def total = None
  def setCols(cols : Long) = FiniteHint(-1L, cols)
  def setRows(rows : Long) = FiniteHint(rows, -1L)
  def setColsToRows = NoClue
  def setRowsToCols = NoClue
  def transpose = NoClue
}

case class FiniteHint(rows : Long = -1L, cols : Long = -1L) extends SizeHint {
  def *(other : SizeHint) = {
    other match {
      case NoClue => NoClue
      case FiniteHint(orows, ocols) => FiniteHint(rows, ocols)
      case sp@SparseHint(_,_,_) => (SparseHint(1.0, rows, cols) * sp)
    }
  }
  def +(other : SizeHint) = {
    other match {
      case NoClue => NoClue
      // In this case, a hint on one side, will overwrite lack of knowledge (-1L)
      case FiniteHint(orows, ocols) => FiniteHint(scala.math.max(rows,orows), scala.math.max(cols,ocols))
      case sp@SparseHint(_,_,_) => (sp + this)
    }
  }
  def total = if(rows >= 0 && cols >= 0) { Some(rows * cols) } else None
  def setCols(ncols : Long) = FiniteHint(rows, ncols)
  def setRows(nrows : Long) = FiniteHint(nrows, cols)
  def setColsToRows = FiniteHint(rows, rows)
  def setRowsToCols = FiniteHint(cols, cols)
  def transpose = FiniteHint(cols, rows)
}

// sparsity is the fraction of the rows and columns that are expected to be present
case class SparseHint(sparsity : Double, rows : Long, cols : Long)  extends SizeHint {
  def * (other : SizeHint) : SizeHint = {
    other match {
      case NoClue => NoClue
      case FiniteHint(r, c) => (this * SparseHint(1.0, r, c))
      case SparseHint(sp,r,c) => {
        // if I occupy a bin with probability p, and you q, then both: pq
        // There are cols samples of the, above, so the probability one is present:
        // 1-(1-pq)^cols ~ (cols * p * q) min 1.0
        val newSp = (cols * sp * sparsity)
        if(newSp >= 1.0) {
          FiniteHint(rows, c)
        }
        else {
          SparseHint(newSp, rows, c)
        }
      }
    }
  }
  def + (other : SizeHint) : SizeHint = {
    other match {
      case NoClue => NoClue
      case FiniteHint(r, c) => (this + SparseHint(1.0, r, c))
      case SparseHint(sp,r,c) => {
        // if I occupy a bin with probability p, and you q, then either: p + q - pq
        if ((sparsity == 1.0) || (sp == 1.0)) {
          FiniteHint(rows max r, cols max c)
        }
        else {
          val newSp = sparsity + sp - sp*sparsity
          SparseHint(newSp, rows max r, cols max c)
        }
      }
    }
  }
  def total : Option[Long] = {
    if((rows >= 0) && (cols >= 0)) {
      Some((rows * cols * sparsity).toLong)
    }
    else
      None
  }
  def setCols(c : Long) : SizeHint = copy(cols = c)
  def setRows(r : Long) : SizeHint = copy(rows = r)
  def setColsToRows : SizeHint = copy(cols = rows)
  def setRowsToCols : SizeHint = copy(rows = cols)
  def transpose : SizeHint = copy(cols = rows, rows = cols)
}

/** Allows us to sort matrices by approximate type
 */
object SizeHintOrdering extends Ordering[SizeHint] with java.io.Serializable {
  def compare(left : SizeHint, right : SizeHint) : Int = {
    left.total.getOrElse(-1L)
      .compareTo(java.lang.Long.valueOf(right.total.getOrElse(-1L)))
  }
}

