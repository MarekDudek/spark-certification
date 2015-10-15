package interretis.intro.sql

import interretis.utils.Resources
import interretis.utils.SeparateSparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.Matchers
import language.postfixOps

case class Person(name: String, age: Int)

class PeopleWithAgesSuite extends SeparateSparkContext with Matchers {

  "Spark" should "operate on data with SQL" in { f =>

    // given
    val sqlContext = new SQLContext(f.sc)
    import sqlContext.implicits._

    // when
    val peopleLines = f.sc textFile (Resources.mainResources + "/people.txt")
    val people = peopleLines map (_ split ",") map (p => Person(p(0), p(1).trim.toInt)) toDF

    people.registerTempTable("people")
    val teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

    // then
    teenagers.count shouldBe 1
  }
}
