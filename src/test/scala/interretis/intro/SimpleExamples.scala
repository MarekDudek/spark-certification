package interretis.intro

import interretis.utils.SeparateSparkContext
import org.scalatest.Matchers

import language.postfixOps

class SimpleExamples extends SeparateSparkContext with Matchers {

  "Local data" should "be parallelized" in { f =>

    // given
    val data = 1 to 1000

    // when
    val distData = f.sc parallelize data
    val belowTen = distData filter (_ < 10) collect

    // then

    belowTen shouldBe Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
  }
}
