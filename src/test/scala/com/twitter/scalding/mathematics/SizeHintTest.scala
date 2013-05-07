package com.twitter.scalding.mathematics

import com.twitter.scalding._
import org.specs._

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll
import org.scalacheck.Gen._

object SizeHintProps extends Properties("SizeHint") {

  val noClueGen = value(NoClue)

  val finiteHintGen = for ( rows <- choose(-1L, 1000000L);
    cols <- choose(-1L, 1000000L))
    yield FiniteHint(rows, cols)

  val sparseHintGen = for ( rows <- choose(-1L, 1000000L);
    cols <- choose(-1L, 1000000L);
    sparsity <- choose(0.0, 1.0))
    yield SparseHint(sparsity, rows, cols)

  implicit val finiteArb : Arbitrary[FiniteHint] = Arbitrary { finiteHintGen }
  implicit val sparseArb : Arbitrary[SparseHint] = Arbitrary { sparseHintGen }
  implicit val genHint : Arbitrary[SizeHint] = Arbitrary { oneOf(noClueGen, finiteHintGen, sparseHintGen) }

  property("a+b is at least as big as a") = forAll { (a : SizeHint, b : SizeHint) =>
    val addT = for( ta <- a.total; tsum <- (a+b).total) yield (tsum >= ta)
    addT.getOrElse(true)
  }

  property("ordering makes sense") = forAll { (a : SizeHint, b : SizeHint) =>
    (List(a,b).max.total.getOrElse(-1L) >= a.total.getOrElse(-1L))
  }

  property("addition increases sparsity fraction") = forAll { (a : SparseHint, b : SparseHint) =>
    (a + b).asInstanceOf[SparseHint].sparsity >= a.sparsity
  }

  property("transpose preserves size") = forAll { (a : SizeHint) =>
    a.transpose.total == a.total
  }

  property("squaring a finite hint preserves size") = forAll { (a : FiniteHint) =>
    val sq = a.setRowsToCols
    val sq2 = a.setColsToRows
    (sq.total == (sq * sq).total) && (sq2.total == (sq2 * sq2).total)
  }

  property("adding a finite hint to itself preserves size") = forAll { (a : FiniteHint) =>
    (a + a).total == a.total
  }

  property("adding a sparse matrix to itself doesn't decrease size") = forAll { (a : SparseHint) =>
    (for ( doubleSize <- (a + a).total;
      asize <- a.total ) yield(doubleSize >= asize)).getOrElse(true)
  }

  property("diagonals are smaller") = forAll { (a : FiniteHint) =>
    SizeHint.asDiagonal(a).total.getOrElse(-2L) < a.total.getOrElse(-1L)
  }

  property("diagonals are about as big as the min(rows,cols)") = forAll { (a : FiniteHint) =>
    SizeHint.asDiagonal(a).total.getOrElse(-1L) <= (a.rows min a.cols)
    SizeHint.asDiagonal(a).total.getOrElse(-1L) >= ((a.rows min a.cols) - 1L)
  }

  property("transpose law is obeyed in total") = forAll { (a : SizeHint, b : SizeHint) =>
    // (A B)^T = B^T A^T
    (a * b).transpose.total == ((b.transpose) * (a.transpose)).total
  }
}
