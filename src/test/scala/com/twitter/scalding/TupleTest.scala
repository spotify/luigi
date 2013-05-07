package com.twitter.scalding

import cascading.tuple.{TupleEntry,Tuple=>CTuple}

import org.specs._

class TupleTest extends Specification with TupleConversions {
  noDetailedDiffs() //Fixes issue for scala 2.9

  def get[T](ctup : CTuple)(implicit tc : TupleConverter[T]) = tc(new TupleEntry(ctup))
  def set[T](t : T)(implicit ts : TupleSetter[T]) : CTuple = ts(t)

  def arityConvMatches[T](t : T, ar : Int)(implicit tc : TupleConverter[T]) : Boolean = {
    tc.arity == ar
  }
  def aritySetMatches[T](t : T, ar : Int)(implicit tc : TupleSetter[T]) : Boolean = {
    tc.arity == ar
  }

  def roundTrip[T](t : T)(implicit tc : TupleConverter[T], ts : TupleSetter[T]) : Boolean = {
    tc(new TupleEntry(ts(t))) == t
  }

  "TupleConverters" should {
    "get primitives out of cascading tuples" in {
      val ctup = new CTuple("hey",new java.lang.Long(2), new java.lang.Integer(3))
      get[(String,Long,Int)](ctup) must be_==(("hey",2L,3))

      roundTrip[Int](3) must beTrue
      arityConvMatches(3,1) must beTrue
      aritySetMatches(3,1) must beTrue
      roundTrip[Long](42L) must beTrue
      arityConvMatches(42L,1) must beTrue
      aritySetMatches(42L,1) must beTrue
      roundTrip[String]("hey") must beTrue
      arityConvMatches("hey",1) must beTrue
      aritySetMatches("hey",1) must beTrue
      roundTrip[(Int,Int)]((4,2)) must beTrue
      arityConvMatches((2,3),2) must beTrue
      aritySetMatches((2,3),2) must beTrue
    }
    "get non-primitives out of cascading tuples" in {
      val ctup = new CTuple(None,List(1,2,3), 1->2 )
      get[(Option[Int],List[Int],(Int,Int))](ctup) must be_==((None,List(1,2,3), 1->2 ))

      roundTrip[(Option[Int],List[Int])]((Some(1),List())) must beTrue
      arityConvMatches((None,Nil),2) must beTrue
      aritySetMatches((None,Nil),2) must beTrue

      arityConvMatches(None,1) must beTrue
      aritySetMatches(None,1) must beTrue
      arityConvMatches(List(1,2,3),1) must beTrue
      aritySetMatches(List(1,2,3),1) must beTrue
    }
    "deal with AnyRef" in {
      val ctup = new CTuple(None,List(1,2,3), 1->2 )
      get[(AnyRef,AnyRef,AnyRef)](ctup) must be_==((None,List(1,2,3), 1->2 ))
      get[AnyRef](new CTuple("you")) must be_==("you")

      roundTrip[AnyRef]("hey") must beTrue
      roundTrip[(AnyRef,AnyRef)]((Nil,Nil)) must beTrue
      arityConvMatches[(AnyRef,AnyRef)](("hey","you"),2) must beTrue
      aritySetMatches[(AnyRef,AnyRef)](("hey","you"),2) must beTrue
    }
  }
}
