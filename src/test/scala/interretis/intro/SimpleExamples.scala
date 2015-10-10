package interretis.intro

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import language.postfixOps
import interretis.utils.SparkTestContext.createTestContext
import org.scalatest.BeforeAndAfterAll
import interretis.utils.SeparateSparkContext

class SimpleExamples extends FlatSpec with Matchers with SeparateSparkContext {

  "Local data" should "be parallelized" in {

    // given
    val data = 1 to 1000

    // when
    val distData = sc parallelize data
    val belowTen = distData filter (_ < 10) collect

    // then

    belowTen shouldBe Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
  }
}
