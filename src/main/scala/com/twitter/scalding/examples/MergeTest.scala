package com.twitter.scalding.examples

import scala.annotation.tailrec

import com.twitter.scalding._

/**
* This example job does not yet work.  It is a test for Kyro serialization
*/
class MergeTest(args : Args) extends Job(args) {
  TextLine(args("input")).flatMapTo('word) { _.split("""\s+""") }
    .groupBy('word) { _.size }
    //Now, let's get the top 10 words:
    .groupAll {
      _.mapReduceMap(('word,'size)->'list) /* map1 */ { tup : (String,Long) => List(tup) }
        /* reduce */ { (l1 : List[(String,Long)], l2 : List[(String,Long)]) =>
          mergeSort2(l1, l2, 10, cmpTup)
        } /* map2 */ {
          lout : List[(String,Long)] => lout
        }
    }
    //Now expand out the list.
    .flatMap('list -> ('word, 'cnt)) { list : List[(String,Long)] => list }
    .project('word, 'cnt)
    .write(Tsv(args("output")))

  //Reverse sort to get the top items
  def cmpTup( t1 : (String,Long), t2 : (String,Long) ) = t2._2.compareTo(t1._2)

  def mergeSort2[T](v1 : List[T], v2 : List[T], k : Int, cmp : Function2[T,T,Int]) = {
    @tailrec
    def mergeSortR(acc : List[T], list1 : List[T], list2 : List[T], k : Int) : List[T] = {
      (list1, list2, k) match {
        case (_,_,0) => acc
        case (x1 :: t1, x2 :: t2, _) => {
          if( cmp(x1,x2) < 0 ) {
            mergeSortR(x1 :: acc, t1, list2, k-1)
          }
          else {
            mergeSortR(x2 :: acc, list1, t2, k-1)
          }
        }
        case (x1 :: t1, Nil, _) => mergeSortR(x1 :: acc, t1, Nil, k-1)
        case (Nil, x2 :: t2, _) => mergeSortR(x2 :: acc, Nil, t2, k-1)
        case (Nil, Nil, _) => acc
      }
    }
    mergeSortR(Nil, v1, v2, k).reverse
  }
}
