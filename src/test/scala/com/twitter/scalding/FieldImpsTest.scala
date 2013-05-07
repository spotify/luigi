package com.twitter.scalding

import cascading.tuple.Fields

import org.specs._

class FieldImpsTest extends Specification with FieldConversions {
  noDetailedDiffs() //Fixes issue for scala 2.9
  def setAndCheck[T <: Comparable[_]](v : T)(implicit conv : (T) => Fields) {
    val vF = conv(v)
    vF.equals(new Fields(v)) must beTrue
  }
  def setAndCheckS[T <: Comparable[_]](v : Seq[T])(implicit conv : (Seq[T]) => Fields) {
    val vF = conv(v)
    vF.equals(new Fields(v : _*)) must beTrue
  }
  def setAndCheckSym(v : Symbol) {
    val vF : Fields = v
    vF.equals(new Fields(v.toString.tail)) must beTrue
  }
  def setAndCheckSymS(v : Seq[Symbol]) {
    val vF : Fields = v
    vF.equals(new Fields(v.map(_.toString.tail) : _*)) must beTrue
  }
  def setAndCheckField(v : Field[_]) {
    val vF : Fields = v
    val fields = new Fields(v.id)
    fields.setComparators(v.ord)
    checkFieldsWithComparators(vF, fields)
  }
  def setAndCheckFieldS(v : Seq[Field[_]]) {
    val vF : Fields = v
    val fields = new Fields(v.map(_.id) : _*)
    fields.setComparators(v.map(_.ord) : _*)
    checkFieldsWithComparators(vF, fields)
  }
  def checkFieldsWithComparators(actual: Fields, expected: Fields) {
    // sometimes one or the other is actually a RichFields, so rather than test for
    // actual.equals(expected), we just check that all the field names and comparators line up
    actual.size must_== expected.size
    (0 until actual.size).foreach { i => actual.get(i).equals(expected.get(i)) must beTrue }
    actual.getComparators.toSeq.equals(expected.getComparators.toSeq) must beTrue
  }
  "Field" should {
    "contain manifest" in {
      val field = Field[Long]("foo")
      field.mf mustEqual Some(implicitly[Manifest[Long]])
    }
  }
  "RichFields" should {
    "convert to Fields" in {
      val f1 = Field[Long]('foo)
      val f2 = Field[String]('bar)
      val rf = RichFields(f1, f2)
      val fields: Fields = rf
      fields.size mustEqual 2
      f1.id mustEqual fields.get(0)
      f2.id mustEqual fields.get(1)
      f1.ord mustEqual fields.getComparators()(0)
      f2.ord mustEqual fields.getComparators()(1)
    }
    "convert from Fields" in {
      val fields = new Fields("foo", "bar")
      val comparator = implicitly[Ordering[String]]
      fields.setComparators(comparator, comparator)
      val fieldList: List[Field[_]] = fields.toFieldList
      fieldList mustEqual List(new StringField[String]("foo")(comparator, None), new StringField[String]("bar")(comparator, None))
    }
    "throw an exception on when converting a virtual Fields instance" in {

      import Fields._
      List(ALL, ARGS, FIRST, GROUP, LAST, NONE, REPLACE, RESULTS, SWAP, UNKNOWN, VALUES).foreach { fields =>
        fields.toFieldList must throwA[Exception]
      }
    }
  }
  "Fields conversions" should {
    "convert from ints" in {
      setAndCheck(int2Integer(0))
      setAndCheck(int2Integer(5))
      setAndCheckS(List(1,23,3,4).map(int2Integer))
      setAndCheckS((0 until 10).map(int2Integer))
    }
    "convert from strings" in {
      setAndCheck("hey")
      setAndCheck("world")
      setAndCheckS(List("one","two","three"))
      //Synonym for list
      setAndCheckS(Seq("one","two","three"))
    }
    "convert from symbols" in {
      setAndCheckSym('hey)
      //Shortest length to make sure the tail stuff is working:
      setAndCheckSym('h)
      setAndCheckSymS(List('hey,'world,'symbols))
    }
    "convert from com.twitter.scalding.Field instances" in {
      // BigInteger is just a convenient non-primitive ordered type
      setAndCheckField(Field[java.math.BigInteger]("foo"))
      setAndCheckField(Field[java.math.BigInteger]('bar))
      setAndCheckField(Field[java.math.BigInteger](0))
      // Try a custom ordering
      val ord = implicitly[Ordering[java.math.BigInteger]].reverse
      setAndCheckField(Field[java.math.BigInteger]("bell")(ord, implicitly[Manifest[java.math.BigInteger]]))
      setAndCheckFieldS(List(Field[java.math.BigInteger](0), Field[java.math.BigDecimal]("bar")))
    }
    "convert from general int tuples" in {
      var vf : Fields = Tuple1(1)
      vf must be_==(new Fields(int2Integer(1)))
      vf = (1,2)
      vf must be_==(new Fields(int2Integer(1),int2Integer(2)))
      vf = (1,2,3)
      vf must be_==(new Fields(int2Integer(1),int2Integer(2),int2Integer(3)))
      vf = (1,2,3,4)
      vf must be_==(new Fields(int2Integer(1),int2Integer(2),int2Integer(3),int2Integer(4)))
    }
    "convert from general string tuples" in {
      var vf : Fields = Tuple1("hey")
      vf must be_==(new Fields("hey"))
      vf = ("hey","world")
      vf must be_==(new Fields("hey","world"))
      vf = ("foo","bar","baz")
      vf must be_==(new Fields("foo","bar","baz"))
    }
    "convert from general symbol tuples" in {
      var vf : Fields = Tuple1('hey)
      vf must be_==(new Fields("hey"))
      vf = ('hey,'world)
      vf must be_==(new Fields("hey","world"))
      vf = ('foo,'bar,'baz)
      vf must be_==(new Fields("foo","bar","baz"))
    }
    "convert from general com.twitter.scalding.Field tuples" in {
      val foo = Field[java.math.BigInteger]("foo")
      val bar = Field[java.math.BigDecimal]("bar")

      var vf  : Fields = Tuple1(foo)
      var fields = new Fields("foo")
      fields.setComparators(foo.ord)
      checkFieldsWithComparators(vf, fields)

      vf = Tuple2(foo, bar)
      fields = new Fields("foo", "bar")
      fields.setComparators(foo.ord, bar.ord)
      checkFieldsWithComparators(vf, fields)

      vf = Tuple3(foo, bar, 'bell)
      fields = new Fields("foo", "bar", "bell")
      fields.setComparator("foo", foo.ord)
      fields.setComparator("bar", bar.ord)
      checkFieldsWithComparators(vf, fields)
    }
    "convert to a pair of Fields from a pair of values" in {
      var f2 : (Fields,Fields) = "hey"->"you"
      f2 must be_==((new Fields("hey"),new Fields("you")))

      f2 = 'hey -> 'you
      f2 must be_==((new Fields("hey"),new Fields("you")))

      f2 = (0 until 10) -> 'you
      f2 must be_==((new Fields((0 until 10).map(int2Integer) : _*),new Fields("you")))

      f2 = (('hey, 'world) -> 'other)
      f2 must be_==((new Fields("hey","world"),new Fields("other")))

      f2  = 0 -> 2
      f2 must be_==((new Fields(int2Integer(0)),new Fields(int2Integer(2))))

      f2 = (0, (1,"you"))
      f2 must be_==((new Fields(int2Integer(0)),new Fields(int2Integer(1),"you")))

      val foo = Field[java.math.BigInteger]("foo")
      val bar = Field[java.math.BigDecimal]("bar")
      f2 = ((foo,bar) -> 'bell)
      var fields = new Fields("foo", "bar")
      fields.setComparators(foo.ord, bar.ord)
      f2 must be_==((fields, new Fields("bell")))

      f2 = (foo -> ('bar,'bell))
      fields = RichFields(foo)
      fields.setComparators(foo.ord)
      f2 must be_==((fields, new Fields("bar", "bell")))

      f2 = Seq("one","two","three") -> Seq("1","2","3")
      f2 must be_==((new Fields("one","two","three"),new Fields("1","2","3")))
      f2 = List("one","two","three") -> List("1","2","3")
      f2 must be_==((new Fields("one","two","three"),new Fields("1","2","3")))
      f2 = List('one,'two,'three) -> List('n1,'n2,'n3)
      f2 must be_==((new Fields("one","two","three"),new Fields("n1","n2","n3")))
      f2 = List(4,5,6) -> List(1,2,3)
      f2 must be_==((new Fields(int2Integer(4),int2Integer(5),int2Integer(6)),
        new Fields(int2Integer(1),int2Integer(2),int2Integer(3))))
    }
    "correctly see if there are ints" in {
      hasInts(0) must beTrue
      hasInts((0,1)) must beTrue
      hasInts('hey) must beFalse
      hasInts((0,'hey)) must beTrue
      hasInts(('hey,9)) must beTrue
      hasInts(('a,'b)) must beFalse
      def i(xi : Int) = new java.lang.Integer(xi)
      asSet(0) must be_==(Set(i(0)))
      asSet((0,1,2)) must be_==(Set(i(0),i(1),i(2)))
      asSet((0,1,'hey)) must be_==(Set(i(0),i(1),"hey"))
    }
    "correctly determine default modes" in {
      //Default case:
      defaultMode(0,'hey) must be_==(Fields.ALL)
      defaultMode((0,'t),'x) must be_==(Fields.ALL)
      defaultMode(('hey,'x),'y) must be_==(Fields.ALL)
      //Equal:
      defaultMode('hey,'hey) must be_==(Fields.REPLACE)
      defaultMode(('hey,'x),('hey,'x)) must be_==(Fields.REPLACE)
      defaultMode(0,0) must be_==(Fields.REPLACE)
      //Subset/superset:
      defaultMode(('hey,'x),'x) must be_==(Fields.SWAP)
      defaultMode('x, ('hey,'x)) must be_==(Fields.SWAP)
      defaultMode(0, ('hey,0)) must be_==(Fields.SWAP)
      defaultMode(('hey,0),0) must be_==(Fields.SWAP)
    }

  }
}
