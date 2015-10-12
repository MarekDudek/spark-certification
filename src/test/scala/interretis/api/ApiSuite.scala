package interretis.api

import interretis.utils.SeparateSparkContext
import org.scalatest.Matchers
import math.pow
import org.apache.spark.rdd.RDD

class ApiSuite extends SeparateSparkContext with Matchers {

  "map" should "transform whole input with function" in { f =>

    // given
    val numbers = f.sc parallelize (1 to 5)

    // when
    val squares = numbers map (pow(_, 2))

    // then
    squares.collect shouldBe Array(1, 4, 9, 16, 25)
  }

  "filter" should "leave only elements that fulfill condition" in { f =>

    // given
    val numbers = f.sc parallelize (1 to 5)

    // when
    val even = numbers filter (_ % 2 == 0)

    // then
    even.collect shouldBe Array(2, 4)
  }

  "flatMap" should "transform whole input with function and flatten results" in { f =>

    // given
    val numbers = f.sc parallelize (1 to 5)

    // when
    val sequences = numbers flatMap (1 to _)

    // then
    sequences.collect shouldBe Array(1, 1, 2, 1, 2, 3, 1, 2, 3, 4, 1, 2, 3, 4, 5)
  }
}
