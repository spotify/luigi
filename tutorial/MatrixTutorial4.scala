package com.twitter.scalding.examples

import com.twitter.scalding._
import com.twitter.scalding.mathematics.Matrix


/*
* MatrixTutorial4.scala
*
* Loads a directed graph adjacency matrix where a[i,j] = 1 if there is an edge from a[i] to b[j]
* and computes the cosine of the angle between every two pairs of vectors
* 
* ../scripts/scald.rb --local MatrixTutorial4.scala --input data/graph.tsv --output data/cosineSim.tsv
*
*/

class ComputeCosineJob(args : Args) extends Job(args) {
  
  import Matrix._

  val adjacencyMatrix = Tsv( args("input"), ('user1, 'user2, 'rel) )
  	.read
  	.toMatrix[Long,Long,Double]('user1, 'user2, 'rel)

  // we compute the L2 normalized adjacency graph 
  val normMatrix = adjacencyMatrix.rowL2Normalize

  // we compute the innerproduct of the normalized matrix with itself
  // which is equivalent with computing cosine: AA^T / ||A|| * ||A||
  (normMatrix * normMatrix.transpose).write( Tsv( args("output") ) )

}

