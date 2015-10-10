package interretis

import org.scalatest.FlatSpec
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import language.postfixOps
import org.scalatest.Matchers

class ExampleTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  var context: SparkContext = null

  override def beforeAll: Unit = {

    val config = new SparkConf()
    config setAppName "Example application"
    config setMaster "local"

    context = new SparkContext(config)
  }

  "This test" should "check Spark application" in {

    // given
    val book = "src/main/resources/alice-in-wonderland.txt"

    // when
    val verses = context textFile book cache
    val alices = verses filter (_.contains("Alice"))
    val characterCount = alices count

    // then
    characterCount shouldBe 396
  }
}
