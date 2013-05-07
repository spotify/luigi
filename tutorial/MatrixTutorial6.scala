package com.twitter.scalding.examples

import com.twitter.scalding._
import com.twitter.scalding.mathematics.Matrix

/*
* MatrixTutorial6.scala
*
* Loads a document to word matrix where a[i,j] = freq of the word j in the document i 
* computes the Tf-Idf score of each word w.r.t. to each document and keeps the top nrWords in each document
* (see http://en.wikipedia.org/wiki/Tf*idf for more info)
* 
* ../scripts/scald.rb --local MatrixTutorial6.scala --input data/docBOW.tsv --nrWords 300 --output data/featSelectedMatrix.tsv
*
*/

class TfIdfJob(args : Args) extends Job(args) {
  
  import Matrix._

  val docWordMatrix = Tsv( args("input"), ('doc, 'word, 'count) )
    .read
    .toMatrix[Long,String,Double]('doc, 'word, 'count)

  // compute the overall document frequency of each row
  val docFreq = docWordMatrix.sumRowVectors

  // compute the inverse document frequency vector
  val invDocFreqVct = docFreq.toMatrix(1).rowL1Normalize.mapValues( x => log2(1/x) )

  // zip the row vector along the entire document - word matrix
  val invDocFreqMat = docWordMatrix.zip(invDocFreqVct.getRow(1)).mapValues( pair => pair._2 )

  // multiply the term frequency with the inverse document frequency and keep the top nrWords
  docWordMatrix.hProd(invDocFreqMat).topRowElems( args("nrWords").toInt ).write(Tsv( args("output") ))

  def log2(x : Double) = scala.math.log(x)/scala.math.log(2.0)

}

