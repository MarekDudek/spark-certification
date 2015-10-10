package interretis

import org.scalatest.FlatSpec
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import language.postfixOps
import org.scalatest.Matchers

class ExampleTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  // given

  var context: SparkContext = null

  override def beforeAll: Unit = {

    val config = new SparkConf()
    config setAppName "Example application"
    config setMaster "local"

    context = new SparkContext(config)
  }

  val book = "src/main/resources/alice-in-wonderland.txt"

  "This test" should "check Spark processing" in {

    // when
    val verses = context textFile book cache
    val alices = verses filter (_.contains("Alice"))
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
