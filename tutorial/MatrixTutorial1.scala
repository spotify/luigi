package com.twitter.scalding.examples

import com.twitter.scalding._
import com.twitter.scalding.mathematics.Matrix


/*
* MatrixTutorial1.scala
* 
* Loads a directed graph adjacency matrix where a[i,j] = 1 if there is an edge from a[i] to b[j]
* and compute the co-follows between any two nodes 
*
* ../scripts/scald.rb --local MatrixTutorial1.scala --input data/graph.tsv --output data/cofollows.tsv
*
*/


class CofollowsJob(args : Args) extends Job(args) {
  
  import Matrix._
  
  val adjacencyMatrix = Tsv( args("input"), ('user1, 'user2, 'rel) )
  	.read
  	.toMatrix[Long,Long,Double]('user1, 'user2, 'rel)

  // compute the innerproduct of the adjacency matrix with itself 
  (adjacencyMatrix * adjacencyMatrix.transpose).write( Tsv( args("output") ) )
}

