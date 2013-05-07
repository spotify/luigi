package com.twitter.scalding.mathematics

class Histogram(map : Map[Double,Int], binWidth : Double) {
  lazy val size = map.values.sum
  lazy val sum = map.foldLeft(0.0){case (acc, (bin, count)) => acc + bin * count} 
  lazy val keys = map.keys.toList.sorted

  lazy val min = keys.head
  lazy val max = keys.last

  lazy val stdDev = {
    val squaredDiff = map.foldLeft(0.0){case (acc, (bin, count)) => acc + count * math.pow(bin - mean, 2.0) }
    math.sqrt(squaredDiff / size)
  }

  lazy val cdf = {
    var cumulative = 0
    var result = Map[Double,Double]()
    keys.foreach {bin =>
      cumulative += map(bin)
      result += (bin -> (cumulative.toDouble / size))
    }
    result
  }

  lazy val lorenz = {
    var cumulativeUnique = 0.0
    var cumulativeTotal = 0.0
    var result = Map[Double,Double]()
    keys.foreach {bin =>
      cumulativeUnique += map(bin)
      cumulativeTotal += bin * map(bin)
      result += (cumulativeUnique / size -> cumulativeTotal / sum)
    }
    result
  }

  def percentile(p : Int) = keys.find{bin => cdf(bin) * 100 >= p}.getOrElse(-1d)

  lazy val median = percentile(50)
  lazy val q1 = percentile(25)
  lazy val q3 = percentile(75)

  def mean = sum / size
  def innerQuartileRange = q3 - q1
  def coefficientOfDispersion = innerQuartileRange / (q3 + q1)
}
