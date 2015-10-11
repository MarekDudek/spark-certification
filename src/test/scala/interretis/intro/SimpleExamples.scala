package interretis.intro

import interretis.utils.SeparateSparkContext
import org.scalatest.Matchers
import language.postfixOps
import interretis.utils.Resources
import interretis.WordCount

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

  "Log mining" should "be deconstructed" in { f =>

    // given
    val lines = f.sc textFile (Resources.mainResources + "/log.txt")

    // when
    val errors = lines filter (_ startsWith "ERROR")
    val messages = errors map (_ split "\t") map (_(1))
    messages cache ()

    val mysql = messages filter (_ contains "mysql")
    val php = messages filter (_ contains "php")

    // then
    mysql count () shouldBe 2
    php count () shouldBe 1
  }

  "Word count" should "work" in { f =>

    // given
    val lines = f.sc textFile (Resources.mainResources + "/alice-in-wonderland.txt")

    // when
    val app = new WordCount
    val counts = app wordCount lines
    val results = counts collect ()

    // then
    results should contain("Alice" -> 221)
    results should contain("Queen" -> 34)
    results should contain("Hatter" -> 24)
  }
}
