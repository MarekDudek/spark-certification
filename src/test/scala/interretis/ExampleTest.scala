package interretis

import interretis.utils.SeparateSparkContext
import org.scalatest.fixture.FlatSpec
import org.scalatest.Matchers

import language.postfixOps

class ExampleTest extends SeparateSparkContext with Matchers {

  // given
  val book = "src/main/resources/alice-in-wonderland.txt"

  "This test" should "check Spark processing" in { f =>

    // when
    val verses = f.sc textFile book cache
    val alices = verses filter (_ contains "Alice")
    val characterCount = alices count

    // then
    characterCount shouldBe 396
  }

  "This test" should "check character count application" in { f =>

    // given
    val verses = f.sc textFile book

    // when
    val app = new CharacterCount
    val count = app countCharacter (verses, "Alice")

    // then
    count shouldBe 396
  }
}
