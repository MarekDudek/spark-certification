package interretis.api

import interretis.utils.SeparateSparkContext
import interretis.utils.FileSystemUtils
import org.scalatest.Matchers
import language.postfixOps

class ActionsSuite extends SeparateSparkContext with Matchers {

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
}
