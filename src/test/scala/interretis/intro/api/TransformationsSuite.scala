package interretis.intro.api

import interretis.utils.SeparateSparkContext
import org.scalatest.Matchers
import org.apache.spark.rdd.RDD
import scala.math.pow
import language.postfixOps

class TransformationsSuite extends SeparateSparkContext with Letters with Matchers {

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

  "sample" should "should return fraction of data" in { f =>

    // given
    val numbers = f.sc parallelize (1 to 5)

    // when
    val single = numbers sample (false, 0.2, 0)

    // then
    single.collect should contain oneOf (1, 2, 3, 4, 5)
  }

  "union" should "sum the data from two sets" in { f =>

    // given
    val numbers1 = f.sc parallelize (1 to 3)
    val numbers2 = f.sc parallelize (4 to 5)

    // when
    val numbers = numbers1 union numbers2

    // then
    numbers.collect shouldBe Array(1, 2, 3, 4, 5)
  }

  "distinct" should "return distinct elemements from dataset" in { f =>

    // given
    val numbers = f.sc parallelize Array(1, 1, 2, 3, 3, 4, 5, 5)

    // when
    val distinctNumbers = numbers distinct

    // then
    val collected = distinctNumbers.collect.toList
    collected should contain allOf (1, 2, 3, 4, 5)
  }

  "groupByKey" should "return values grouped by first elements of pairs" in { f =>

    // given
    val pairs = f.sc parallelize Array((1, A), (2, A), (1, B), (3, B))

    // when
    val grouped = pairs groupByKey

    // then
    grouped.collect should contain allOf ((1, List(A, B)), (2, List(A)), (3, List(B)))
  }

  "reduceByKey" should "return dataset with values aggregated by function" in { f =>

    // given
    val pairs = f.sc parallelize Array((1, A), (2, A), (1, B), (3, B))

    // when
    val aggregated = pairs reduceByKey (_ + _)

    // then
    aggregated.collect should contain allOf ((1, "AB"), (2, A), (3, B))
  }

  "sortByKey" should "return datased sorted by key" in { f =>

    // given
    val pairs = f.sc parallelize Array((4, A), (2, A), (1, B), (3, B))

    // when
    val sorted = pairs sortByKey ()

    // then
    sorted.collect shouldBe Array((1, B), (2, A), (3, B), (4, A))
  }

  "join" should "join datasets by key" in { f =>

    // given
    val setA = f.sc parallelize Array((1, A), (2, B), (3, C), (1, D))
    val setB = f.sc parallelize Array((1, List('d')), (3, Nil), (4, List('z')), (1, List('t', 'z')))

    // when
    val joined = setA join setB collect

    // then

    joined should have size 5

    joined should contain((1, (A, List('d'))))
    joined should contain((1, (A, List('t', 'z'))))
    joined should contain((1, (D, List('d'))))
    joined should contain((1, (D, List('t', 'z'))))
    joined should contain((3, (C, Nil)))
  }

  "cogroup" should "group datasets" in { f =>

    // given
    val setA = f.sc parallelize Array((1, A), (2, B), (3, C), (1, D))
    val setB = f.sc parallelize Array((1, List('d')), (3, Nil), (4, List('z')), (1, List('t', 'z')))

    // when
    val cogrouped = setA cogroup setB collect

    // then

    cogrouped should have size 4

    cogrouped should contain(1, (List(A, D), List(List('d'), List('t', 'z'))))
    cogrouped should contain(2, (List(B), List()))
    cogrouped should contain(3, (List(C), List(Nil)))
    cogrouped should contain(4, (List(), List(List('z'))))
  }

  "cartesian" should "give product of two datasets" in { f =>

    // given
    val setA = f.sc parallelize Array(1, 2, 3)
    val setB = f.sc parallelize Array(A, B)

    // when
    val product = setA cartesian setB

    // then
    product.collect should contain allOf ((1, A), (1, B), (2, A), (2, B), (3, A), (3, B))
  }
}
