package com.twitter.scalding
import org.specs._

class IntegralCompTest extends Specification {
  def box[T](t : T) = t.asInstanceOf[AnyRef]

  "IntegralComparator" should {
    val intComp = new IntegralComparator
    "recognize integral types" in {
      intComp.isIntegral(box(1)) must beTrue
      intComp.isIntegral(box(1L)) must beTrue
      intComp.isIntegral(box(1.asInstanceOf[Short])) must beTrue
      //Boxed
      intComp.isIntegral(new java.lang.Long(2)) must beTrue
      intComp.isIntegral(new java.lang.Integer(2)) must beTrue
      intComp.isIntegral(new java.lang.Short(2.asInstanceOf[Short])) must beTrue
      intComp.isIntegral(new java.lang.Long(2)) must beTrue
      intComp.isIntegral(new java.lang.Long(2)) must beTrue
      //These are not integrals
      intComp.isIntegral(box(0.0)) must beFalse
      intComp.isIntegral(box("hey")) must beFalse
      intComp.isIntegral(box(Nil)) must beFalse
      intComp.isIntegral(box(None)) must beFalse
    }
    "handle null inputs" in {
       intComp.hashCode(null) must be_==(0)
       List(box(1),box("hey"),box(2L),box(0.0)).foreach { x =>
         intComp.compare(null, x) must be_<(0)
         intComp.compare(x,null) must be_>(0)
         intComp.compare(x, x) must be_==(0)
       }
       intComp.compare(null,null) must be_==(0)
    }
    "have consistent hashcode" in {
      List( (box(1),box(1L)), (box(2),box(2L)), (box(3),box(3L)) )
        .foreach { pair =>
          intComp.compare(pair._1, pair._2) must be_==(0)
          intComp.hashCode(pair._1) must be_==(intComp.hashCode(pair._2))
        }
      List( (box(1),box(2L)), (box(2),box(3L)), (box(3),box(4L)) )
        .foreach { pair =>
          intComp.compare(pair._1, pair._2) must be_<(0)
          intComp.compare(pair._2, pair._1) must be_>(0)
        }
    }
    "Compare strings properly" in {
      intComp.compare("hey","you") must be_==("hey".compareTo("you"))
      intComp.compare("hey","hey") must be_==("hey".compareTo("hey"))
      intComp.compare("you","hey") must be_==("you".compareTo("hey"))
    }
  }
}
