package interretis.intro.api

import interretis.utils.SeparateSparkContext
import interretis.utils.FileSystemUtils
import org.scalatest.Matchers
import language.postfixOps

class ActionsSuite extends SeparateSparkContext with Matchers with Letters {

  "reduce" should "aggregate elements using function" in { f =>

    // given
    val numbers = f.sc parallelize (1 to 5)

    // when
    val sum = numbers reduce (_ + _)

    // then
    sum shouldBe 15
  }

  "count" should "return number of elements in dataset" in { f =>

    // given
    val numbers = f.sc parallelize (1 to 5)

    // when
    val count = numbers count

    // then
    count shouldBe 5
  }

  "first" should "return first element" in { f =>

    // given
    val numbers = f.sc parallelize (1 to 5)

    // when
    val first = numbers first

    // then
    first shouldBe 1
  }

  "take" should "return first elements from dataset" in { f =>

    // given
    val numbers = f.sc parallelize (1 to 5)

    // when
    val taken = numbers take 3

    // then
    taken shouldBe Array(1, 2, 3)
  }

  "takeSample" should "return fraction of elements" in { f =>

    // given
    val numbers = f.sc parallelize (1 to 5)

    // when
    val sample = numbers takeSample (false, 1, 0)

    // then
    sample should contain oneOf (1, 2, 3, 4, 5)
  }

  "saveAsTextFile" should "save data into filesystem" in { f =>

    val tempDir = FileSystemUtils createTempDirectory ("target", "action-suite-")
    val outputDir = FileSystemUtils buildPath (tempDir, "numbers")

    val numbers = f.sc parallelize (1 to 5)

    // when
    numbers saveAsTextFile outputDir

    // then
    val lines = f.sc textFile outputDir
    val numbersAgain = lines map (_.toInt)

    numbers.collect shouldBe numbersAgain.collect
  }

  "countByKey" should "count elements that have matching key" in { f =>

    // given
    val pairs = f.sc parallelize Array((1, A), (2, A), (1, B), (3, B))

    // when
    val counted = pairs countByKey

    // then
    counted should contain allOf ((1 -> 2), (2 -> 1), (3 -> 1))
  }

  "foreach" should "run function on each element of the dataset" in { f =>

    // given
    val numbers = f.sc parallelize (1 to 5)
    val acc = f.sc accumulator (0)

    // when
    numbers foreach (acc += _)

    // then
    acc.value shouldBe 15
  }
}
