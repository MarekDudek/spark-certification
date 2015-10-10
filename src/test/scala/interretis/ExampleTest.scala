package interretis

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import interretis.SparkTestContext.createTestContext

import language.postfixOps

class ExampleTest extends FlatSpec with Matchers {

  // given
  val context = createTestContext()
  val book = "src/main/resources/alice-in-wonderland.txt"

  "This test" should "check Spark processing" in {

    // when
    val verses = context textFile book cache
    val alices = verses filter (_ contains "Alice")
    val characterCount = alices count

    // then
    characterCount shouldBe 396
  }

  "This test" should "check character count application" in {

    // given
    val verses = context textFile book

    // when
    val app = new CharacterCount
    val count = app countCharacter (verses, "Alice")

    // then
    count shouldBe 396
  }
}
